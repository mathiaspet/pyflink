/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.examples.java.spatial;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.TileWrapper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Sort the tiles according to the coordinate order (in a two dimensional array)
 * Stitch them up and cut away the additional pixels...
 * 
 * @author mathiasp
 *
 */
public class TileStitchReduce implements GroupReduceFunction<Tuple3<String, byte[], byte[]>, Tuple3<String, byte[], byte[]>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Coordinate leftUpper, rightLower;
	private int xSize, ySize;

	@Override
	public void reduce(Iterable<Tuple3<String, byte[], byte[]>> values, Collector<Tuple3<String, byte[], byte[]>> out)
			throws Exception {
		/*
		//TODO: why?!
		for (Tuple3<String, byte[], byte[]> t: values) {
			out.collect(t);
		}
		*/

		Map<Integer, Set<TileWrapper>> bandToTiles = new HashMap<Integer, Set<TileWrapper>>();
		for (Tuple3<String, byte[], byte[]> t : values) {
			TileWrapper tw = new TileWrapper(t);
			Set<TileWrapper> tiles = bandToTiles.get(tw.getBand());
			if (tiles == null) {
				tiles = new HashSet<TileWrapper>();
				bandToTiles.put(new Integer(tw.getBand()), tiles);
			}
			tiles.add(tw);
		}
		long origNotNullCounter = 0;
		List<Integer> bands = new ArrayList<Integer>(bandToTiles.keySet());
		Collections.sort(bands);
		long insideCounter = 0;
		long knownCounter = 0;
		
		for (Integer band : bands) {
			boolean updated = false;
			TileWrapper result = new TileWrapper();
			short[] content = new short[xSize * ySize];
			result.setS16Tile(content);
			// initialize with no data values
			for (int i = 0; i < content.length; i++) {
				content[i] = -9999; // TODO: get this from tile info
			}
			

			Set<TileWrapper> inputTiles = bandToTiles.get(band);
			for (TileWrapper t : inputTiles) {
				if (!updated) {
					result.update(t.getTileInfo(), leftUpper, rightLower,
							xSize, ySize, band, t.getPathRow(),
							t.getAcquisitionDate(), t.getTileInfo().getPixelWidth(),
							t.getTileInfo().getPixelHeight());
					updated = true;
				}

				// TODO: make this more efficient by operating on blocks
				//TODO: use array copy
				
				for (int i = 0; i < t.getS16Tile().length; i++) {
					Coordinate pixelCoord = t.getCoordinate(i);
					if(t.getS16Tile()[i] != -9999) {
						origNotNullCounter++;
					}
					
					if(
							(this.leftUpper.lat >= pixelCoord.lat)
							&& (pixelCoord.lat >= this.rightLower.lat)
							
							&& (this.leftUpper.lon <= pixelCoord.lon)
							&& (pixelCoord.lon <= this.rightLower.lon)
						) {
						int index = result
								.getContentIndexFromCoordinate(pixelCoord);
						if (index >= 0 && index < content.length) {
							insideCounter++;
							short pixelValue = t.getS16Tile()[i];
							if(pixelValue != -9999) {
								knownCounter++;
							}
							
							content[index] = pixelValue;
						}
					}

				}
			}

//			result.setS16Tile(content);
			out.collect(result.toTuple());

		}
		System.out.println("Counted " + insideCounter + " and " + knownCounter + " originally not null: " + origNotNullCounter);
	}

	public TileStitchReduce configure(Coordinate leftUpper, Coordinate rightLower, int xSize, int ySize) {
		this.leftUpper = leftUpper;
		this.rightLower = rightLower;
		this.xSize = xSize;
		this.ySize = ySize;
		return this;
	}

}
