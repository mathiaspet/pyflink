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


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.envi.ImageOutputFormat;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;

public class TileOverlap {


    private static int dop;
    private static String filePath;
    private static Coordinate aoiLeftUpper, aoiRightLower;
    private static int blockSize, aoi_edge; // in pixel, squared blocks
    private static String outputFilePath;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(dop);

        DataSet<Tuple3<String, byte[], byte[]>> allTiles = readTiles(env);
        DataSet<Tuple3<String, byte[], byte[]>> overlappingTiles = allTiles.flatMap(new InvolvedTileSelector( //TODO ));

        DataSink<Tuple3<String, byte[], byte[]>> writeAsEnvi = overlappingTiles.write(new ImageOutputFormat(), outputFilePath);
        writeAsEnvi.setParallelism(1);
        env.execute("Tile Overlapping");

        /* TODO:
            - AOI und Tiles an Mapper verteilen. oder filter
            - Je Mapper: AOI und Tiles vergleichen, ob ueberlappung
            - WIE Sortieren? nach welchem Kiterium nachbarschaft festlegen??
            - groupBy nach nachbarschaftskey, sccenenweise gruppieren. bei 2 scenen: reducer für die scenen und reducer für die schnittestelle der scenen, dann uberlappung berechnen
         */

    }

    private static boolean parseParameters(String[] params) {

        if (params.length > 0) {
            // input A, 2 coordinates: dop, input dir, left_upper_long, left_upper_lat, right_lower_long, right_lower_lat, pixel_size, output dir = 7
            // input B, 1 coordinate and 1 edge: dop, input dir, left_upper_long, left_upper_lat, aoi_edge, output dir = 6

            // ** A -- 2 Coordinates **
            if (params.length != 7) {
                System.out.println("Usage: <dop> <input directory> <left-upper-longitude> " +
                        "<left-upper-latitude> <right-lower-longitude> <right-lower-latitude> <output path>");
                return false;
            } else {
                dop = Integer.parseInt(params[0]);
                filePath = params[1];
                String leftLong = params[2];
                String leftLat = params[3];
                String rightLong = params[4];
                String rightLat = params[5];
                aoiLeftUpper = new Coordinate(Double.parseDouble(leftLong), Double.parseDouble(leftLat));
                aoiRightLower = new Coordinate(Double.parseDouble(rightLong), Double.parseDouble(rightLat));
                outputFilePath = params[6];
            }

            // ** B -- 1 Coordinate and 1 Edge **
            if (params.length != 6) {
                System.out.println("Usage: <dop> <input directory> <left-upper-longitude> " +
                        "<left-upper-latitude> <aoi_edge> <output path>");
                return false;
            } else {
                dop = Integer.parseInt(params[0]);
                filePath = params[1];
                String leftLong = params[2];
                String leftLat = params[3];
                aoiLeftUpper = new Coordinate(Double.parseDouble(leftLong), Double.parseDouble(leftLat));
                aoi_edge = Integer.parseInt(params[4]);
                double rightLong = Double.parseDouble(leftLong) + aoi_edge;
                double rightLat = Double.parseDouble(leftLat) - aoi_edge;
                aoiRightLower = new Coordinate(rightLong, rightLat);
                outputFilePath = params[5];
            }
        } else {
            System.out.println(" Input parameters necessary!");
            return false;
        }
        return true;
    }

    private static DataSet<Tuple3<String, byte[], byte[]>> readTiles(ExecutionEnvironment env) {
        //TODO: auf ueberlappende Tiles anpassen
        TileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new TileInputFormat<Tuple3<String, byte[], byte[]>>(new Path(filePath));
        enviFormat.setLimitRectangle(aoiLeftUpper, aoiRightLower);
        enviFormat.setTileSize(blockSize, blockSize);
        TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<Tuple3<String, byte[], byte[]>>(BasicTypeInfo.STRING_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        return new DataSource<Tuple3<String, byte[], byte[]>>(env, enviFormat, typeInfo, "enviSource");
    }


    public static class InvolvedTileSelector implements FlatMapFunction<Tuple2<Coordinate, Coordinate>, Tuple3<String, byte[], byte[]>> {

        public void flatMap(Tuple2<Coordinate, Coordinate> aoi, Tuple3<String, byte[], byte[]> tile){

            Coordinate aoiLU = aoi.getField(0);
            Coordinate aoiRL = aoi.getField(1);
            Coordinate aoiRU = new Coordinate(aoiRL.lon, aoiLU.lat);
            Coordinate aoiLL = new Coordinate(aoiLU.lon, aoiRL.lat);


            //TODO: Tile Koordinaten extrahieren: in tile oject sollten die koord. drinn sein
            Coordinate tileLU; //TODO
            Coordinate tileRL; //TODO
            Coordinate tileRU = new Coordinate(tileRL.lon, tileLU.lat);
            Coordinate tileLL = new Coordinate(tileLU.lon, tileRL.lat);

            // compare coordinates
            Coordinate lu_rl_Diff = aoiLU.diff(tileRL);
            Coordinate rl_lu_Diff = aoiRL.diff(tileLU);
            Coordinate lu_lu_Diff = aoiLU.diff(tileLU);
            Coordinate rl_rl_Diff = aoiRL.diff(tileRL);

            // if tile coordinate within aoi
            if((lu_rl_Diff.lon<0 && lu_rl_Diff.lat>0) && (lu_lu_Diff.lon>0 && lu_lu_Diff.lat<0)){

            }

            if((rl_rl_Diff.lat<0 && rl_rl_Diff.lon>0) && (rl_lu_Diff.lon>0 && rl_lu_Diff.lat<0)){
                   // return Tile cuz involved
            }


        }
    }

}



