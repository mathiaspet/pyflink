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
package org.apache.flink.api.java.spatial;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Spatial tile This class represents a spatial tile. TODO: describe me & make
 * the internal representation more efficient now the fields are accessed
 * deserialized, this is very inefficient
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 * TODO: refactor the ctor calls to reflect on the 
 */
public class Tile extends SpatialObject{
	private static final long serialVersionUID = 3999969290376342375L;

	int band = -1;

	// TODO: decide whether to keep this public or not
	public Tile() {
	}

	public Tile(Coordinate leftUpper, Coordinate rightLower, short[] content,
			int width, int height) {
		this.luCord = leftUpper;
		this.rlCord = rightLower;
		this.s16Tile = content;
		this.tileWidth = width;
		this.tileHeight = height;
	}

	public Tile(Tile tile) {
		super(tile);
		
		this.band = tile.getBand();
		
	}

	public int getBand() {
		return this.band;
	}
	
	public void setBand(int band) {
		this.band = band;
	}

	/**
	 * Return the coordinate of the north-west boundary point of this tile.
	 */
	public Coordinate getNWCoord() {
		return this.luCord;
	}

	/**
	 * Return the coordinate of the south-east boundary point of this tile.
	 */
	public Coordinate getSECoord() {
		return this.rlCord;
	}

	/**
	 * Update the tile information to the given object.
	 * 
	 * @param aqcDate
	 */
	public void update(TileInfo tileInfo, Coordinate leftUpper,
			Coordinate rightLower, int width, int height, int band,
			String pathRow, String aqcDate, double xPixelWidth, 
			double yPixelWidth) {
		this.tileInfo = tileInfo;
		this.luCord = leftUpper;
		this.rlCord = rightLower;
		this.tileWidth = width;
		this.tileHeight = height;
		this.band = band;
		this.pathRow = pathRow;
		this.aqcuisitionDate = aqcDate;
		this.xPixelWith = xPixelWidth;
		this.yPixelWidth = yPixelWidth;
	}

	public Long getAqcuisitionDateAsLong() {
		if (this.tileInfo == null) {
			return new Long(-1);
		} else {
			return this.tileInfo.getAcqDate();
		}
	}

	/**
	 * Given the number of samples, the pixel dimensions and the upper left
	 * reference point we calculate the geographical coordinate;
	 * 
	 * @param contentIndex
	 * @return
	 */
	public Coordinate getCoordinate(int contentIndex) {
		int x = contentIndex % tileWidth;
		int y = (int) (contentIndex / tileWidth);
		double newLon = this.luCord.lon + this.xPixelWith * x;
		double newLat = this.luCord.lat - this.yPixelWidth * y;

		return new Coordinate(newLon, newLat);
	}

	/**
	 * Given the left upper reference point, the tile and pixel dimensions we
	 * calculate the actual position in the s16Tile array.
	 * 
	 * @param coord
	 * @return
	 */
	public int getContentIndexFromCoordinate(Coordinate coord) {
		int latDiff = (int) (this.luCord.lat - coord.lat);
		int lonDiff = (int) (coord.lon - this.luCord.lon);

		// check if coord is fully contained in this tile
		if (latDiff < 0 || lonDiff < 0) {
			return -1;
		}

		int x = (int) (lonDiff / this.xPixelWith);
		int y = (int) (latDiff / this.yPixelWidth);

		return y * this.tileWidth + x;
	}

	public Tile createCopy() {
		return new Tile(this);
	}

	public void copyTo(Tile target) {
		super.copyTo(target);
		target.band = this.band;
	}

	public void serialize(DataOutputView target) throws IOException {
		super.serialize(target);
		
		target.writeInt(this.band);
	}

	public void deserialize(DataInputView source) throws IOException {
		super.deserialize(source);
		
		
		this.band = source.readInt();

			}
}
