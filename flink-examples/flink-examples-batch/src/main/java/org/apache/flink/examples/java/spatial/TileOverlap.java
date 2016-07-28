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


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.TileInfoWrapper;
import org.apache.flink.api.java.spatial.TileWrapper;
import org.apache.flink.api.java.spatial.envi.ImageOutputFormat;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.spatial.envi.OverlappingTileInputFormat;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;


public class TileOverlap {


    private static int dop;
    private static String filePath;
    private static Coordinate aoiLeftUpper, aoiRightLower;
    private static int blockSize, aoi_edge, overlapSize; // in pixel, squared blocks
    private static String outputFilePath;


	private static FileSystem.WriteMode writeMode = OVERWRITE;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(dop);

        DataSet<Tuple3<String, byte[], byte[]>> allTiles = getTiles(env);


        DataSet<Tuple3<String, byte[], byte[]>> involvedTiles = allTiles.filter(new InvolvedTileSelector())
																		.groupBy(new SceneIDExtractor())
																		//.groupBy(0)
																		.reduceGroup(new TileIntersection());

		DataSink<Tuple3<String, byte[], byte[]>> writeAsEnvi = involvedTiles.write(new ImageOutputFormat(), outputFilePath, writeMode).setParallelism(1);

		/*
		allTiles.flatMap(new FlatMapFunction<Tuple3<String,byte[],byte[]>, Tuple3<String,byte[],byte[]>>() {
			@Override
			public void flatMap(Tuple3<String, byte[], byte[]> value, Collector<Tuple3<String, byte[], byte[]>> out) throws Exception {
				out.collect(value);
			}
		}).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple3<String,byte[],byte[]>, Tuple3<String, byte[], byte[]>>() {
			@Override
			public void reduce(Iterable<Tuple3<String, byte[], byte[]>> values, Collector<Tuple3<String, byte[], byte[]>> out) throws Exception {
				for(Tuple3<String, byte[], byte[]> i : values){
					System.out.println("In reduceGroup: " + i.f0);
					out.collect(i);
				}
			}
		}).write(new ImageOutputFormat<Tuple3<String, byte[], byte[]>>(), outputFilePath, OVERWRITE);
        */

        env.execute("Tile Overlap");
    }

    private static boolean parseParameters(String[] params) {

        if ((params.length==9)||(params.length==8)) {
            // input A, 2 coordinates: dop, input dir, left_upper_lat, left_upper_long, right_lower_lat, right_lower_long, blockSize, overlapSize, output dir = 9
            // input B, 1 coordinate and 1 edge: dop, input dir, left_upper_lat, left_upper_long, aoi_edge, blockSize, overlapSize, output dir = 8

            // ** A -- 2 Coordinates **
            if (params.length == 9) {
				System.out.	println("Params_len: " + params.length);
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				String leftLat = params[2];
				String leftLong = params[3];
				String rightLat = params[4];
				String rightLong = params[5];
				aoiLeftUpper = new Coordinate(Double.parseDouble(leftLong), Double.parseDouble(leftLat));
				aoiRightLower = new Coordinate(Double.parseDouble(rightLong), Double.parseDouble(rightLat));
				blockSize = Integer.parseInt(params[6]);
				overlapSize = Integer.parseInt(params[7]);
				outputFilePath = params[8];
            }

            // ** B -- 1 Coordinate and 1 Edge **
            if (params.length == 8) {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				String leftLat = params[2];
				String leftLong = params[3];
				aoiLeftUpper = new Coordinate(Double.parseDouble(leftLong), Double.parseDouble(leftLat));
				aoi_edge = Integer.parseInt(params[4]);
				double rightLong = Double.parseDouble(leftLong) + aoi_edge;
				double rightLat = Double.parseDouble(leftLat) - aoi_edge;
				aoiRightLower = new Coordinate(rightLong, rightLat);
				blockSize = Integer.parseInt(params[5]);
				overlapSize = Integer.parseInt(params[6]);
				outputFilePath = params[7];
            }
        } else {
            System.out.println(" Input parameters necessary!");
            return false;
        }
        return true;
    }

    private static DataSet<Tuple3<String, byte[], byte[]>> getTiles(ExecutionEnvironment env) {
        OverlappingTileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new OverlappingTileInputFormat<>(new Path(filePath), overlapSize);
        enviFormat.setTileSize(blockSize, blockSize);
        TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		return new DataSource<>(env, enviFormat, typeInfo, "enviSource");
    }


	/** InvolvedTileSelector:
	 * Returns only those tiles that are intersecting with the given area of interest.
	 */

    public static class InvolvedTileSelector implements FilterFunction<Tuple3<String, byte[], byte[]>> {

        @Override
        public boolean filter(Tuple3<String, byte[], byte[]> tile) throws Exception {
			OverlappingTileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new OverlappingTileInputFormat<>(new Path(filePath), overlapSize);
            TileWrapper tileInfo = new TileWrapper(tile);
            Coordinate tileLeftUpper = tileInfo.getNWCoord();
            Coordinate tileRightLower = tileInfo.getSECoord();
            enviFormat.setLimitRectangle(aoiLeftUpper, aoiRightLower);
			boolean value = enviFormat.rectIntersectsLimits(tileLeftUpper, tileRightLower);
			return value;
        }
    }

	/** TileIntersection:
	 * Calculate tile overlap size.
	 */
	public static class TileIntersection implements GroupReduceFunction<Tuple3<String, byte[], byte[]>, Tuple3<String, byte[], byte[]>> {

		@Override
		public void reduce(Iterable<Tuple3<String, byte[], byte[]>> input,
						   Collector<Tuple3<String, byte[], byte[]>> output) throws Exception {
			System.out.println("enter reduce");
			OverlappingTileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new OverlappingTileInputFormat<>(new Path(filePath), overlapSize);
			ArrayList<Tuple3<String, byte[], byte[]>> inputTiles = new ArrayList<>();
			String overlapProp = "";

			for(Tuple3<String, byte[], byte[]> tile : input) {
				System.out.println("Tile ID: " + tile.f0);
				inputTiles.add(new Tuple3<>(tile.f0, tile.f1, tile.f2));
				TileWrapper tileWrapper = new TileWrapper(tile);
				System.out.println("Tile Coord. " + tileWrapper.getNWCoord() + tileWrapper.getSECoord());
			}

			for(Tuple3<String, byte[], byte[]> tile : inputTiles) {
				TileWrapper tileInfo = new TileWrapper(tile);
				Coordinate tileLeftUpper = tileInfo.getNWCoord();
				Coordinate tileRightLower = tileInfo.getSECoord();
				enviFormat.setLimitRectangle(tileLeftUpper, tileRightLower);

				for(Tuple3<String, byte[], byte[]> otherTile : inputTiles) {
					TileWrapper otherTileInfo = new TileWrapper(otherTile);
					Coordinate otherTileLeftUpper = otherTileInfo.getNWCoord();
					Coordinate otherTileRightLower = otherTileInfo.getSECoord();

					if(!(tileLeftUpper.equals(otherTileLeftUpper) && tileRightLower.equals(otherTileRightLower))){

						if(enviFormat.rectIntersectsLimits(otherTileLeftUpper, otherTileRightLower)){
							Double lonDiff = -1.0;
							Double latDiff = -1.0;
							Coordinate lu_rl_Diff = tileLeftUpper.diff(otherTileRightLower);
							Coordinate rl_lu_Diff = tileRightLower.diff(otherTileLeftUpper);
							
							// 8 different cases of how the tiles might be located:
							// upper-left corner
							if((tileLeftUpper.lon > otherTileLeftUpper.lon) && (tileLeftUpper.lat < otherTileLeftUpper.lat)
								&& (tileRightLower.lon > otherTileRightLower.lon) && (tileRightLower.lat < otherTileRightLower.lat)){
								lonDiff = lu_rl_Diff.lon;
								latDiff = lu_rl_Diff.lat;
							}
							// upper-right corner
							if((tileLeftUpper.lon < otherTileLeftUpper.lon) && (tileLeftUpper.lat < otherTileLeftUpper.lat)
								&& (tileRightLower.lon < otherTileRightLower.lon) && (tileRightLower.lat < otherTileRightLower.lat)){
								lonDiff = rl_lu_Diff.lon;
								latDiff = lu_rl_Diff.lat;
							}
							// upper-middle
							if((tileLeftUpper.lon == otherTileLeftUpper.lon) && (tileLeftUpper.lat < otherTileLeftUpper.lat)
								&& (tileRightLower.lon == otherTileRightLower.lon) && (tileRightLower.lat < otherTileRightLower.lat)){
								lonDiff = 0.0;
								latDiff = lu_rl_Diff.lat;
							}
							// lower-middle
							if((tileLeftUpper.lon == otherTileLeftUpper.lon) && (tileLeftUpper.lat > otherTileLeftUpper.lat)
								&& (tileRightLower.lon == otherTileRightLower.lon) && (tileRightLower.lat > otherTileRightLower.lat)){
								lonDiff = 0.0;
								latDiff = rl_lu_Diff.lat;
							}
							// lower-left corner
							if((tileLeftUpper.lon > otherTileLeftUpper.lon) && (tileLeftUpper.lat > otherTileLeftUpper.lat)
								&& (tileRightLower.lon > otherTileRightLower.lon) && (tileRightLower.lat > otherTileRightLower.lat)){
								lonDiff = rl_lu_Diff.lon;
								latDiff = lu_rl_Diff.lat;
							}
							// lower-right corner
							if((tileLeftUpper.lon < otherTileLeftUpper.lon) && (tileLeftUpper.lat > otherTileLeftUpper.lat)
								&& (tileRightLower.lon < otherTileRightLower.lon) && (tileRightLower.lat > otherTileRightLower.lat)){
								lonDiff = rl_lu_Diff.lon;
								latDiff = rl_lu_Diff.lat;
							}
							// right-middle
							if ((tileLeftUpper.lon < otherTileLeftUpper.lon) && (tileLeftUpper.lat == otherTileLeftUpper.lat)
								&& (tileRightLower.lon < otherTileRightLower.lon) && (tileRightLower.lat == otherTileRightLower.lat)){
								lonDiff = rl_lu_Diff.lon;
								latDiff = 0.0;
							}
							// left-middle
							if((tileLeftUpper.lon > otherTileLeftUpper.lon) && (tileLeftUpper.lat == otherTileLeftUpper.lat)
								&& (tileRightLower.lon > otherTileRightLower.lon) && (tileRightLower.lat == otherTileRightLower.lat)){
								lonDiff = lu_rl_Diff.lat;
								latDiff = 0.0;
							}

							if(lonDiff<0.0){lonDiff = lonDiff*(-1.0);}
							if(latDiff<0.0){latDiff = latDiff*(-1.0);}

							System.out.println("For tile A (Coord. LU: "+tileLeftUpper+", RL: "+tileRightLower+") and " +
								"tile B (Coord. LU: "+otherTileLeftUpper+", RL: "+otherTileRightLower+") overlap size of:" +
								" lon: "+lonDiff+", lat: "+latDiff+"");

							overlapProp += ("(tile [LU: "+otherTileLeftUpper+", RL: "+otherTileRightLower+"]; " +
											"overlap [lon: "+lonDiff+", lat: "+latDiff+"]), ");
							tileInfo.setOverlap4tile(tile, overlapProp);
							output.collect(new Tuple3<>(tile.f0, tile.f1, tile.f2));

						}else{
							System.out.println("These tiles do not intersect.");
						}
					}
				}
			}
		}
	}


	/** Scene ID Extraction
	 * Group incoming tiles by scene ID
	 */
	public static class SceneIDExtractor implements KeySelector<Tuple3<String, byte[], byte[]>, String> {

		@Override
		public String getKey(Tuple3<String, byte[], byte[]> tile) throws Exception {
			TileWrapper tileInfo = new TileWrapper(tile);
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ ");
			String sceneID = tileInfo.getSceneID();
			return sceneID;
		}
	}


}



