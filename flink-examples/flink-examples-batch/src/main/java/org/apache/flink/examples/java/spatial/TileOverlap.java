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
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.flink.api.java.spatial.envi.OverlappingTileInputFormat;

public class TileOverlap {


    private static int dop;
    private static String filePath;
    private static Coordinate aoiLeftUpper, aoiRightLower;
    private static int blockSize, aoi_edge, overlapSize; // in pixel, squared blocks
    private static String outputFilePath;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        // TODO Scenen überlappend in Tiles schneiden


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(dop);

        DataSet<Tuple3<String, byte[], byte[]>> allTiles = getTiles(env);
        DataSet<Tuple3<String, byte[], byte[]>> overlappingTiles = allTiles.filter(new InvolvedTileSelector()); // weitere Schritte noch

        DataSink<Tuple3<String, byte[], byte[]>> writeAsEnvi = overlappingTiles.write(new ImageOutputFormat(), outputFilePath);
        writeAsEnvi.setParallelism(1);
        env.execute("Tile Overlap");

        /* TODO:
            - AOI und Tiles an Mapper verteilen. --> filter
            - Je Mapper: AOI und Tiles vergleichen, ob Ueberlappung
            - Reducer für Scenenweise Berechnung der Tile Überlappung
            - Reducer für Berechnung der Tile Überlappung im Schnittbereich der Scenen
            - Vll einen Coordinaten-"extracter" schreiben
         */

    }

    private static boolean parseParameters(String[] params) {

        if (params.length > 0) {
            // input A, 2 coordinates: dop, input dir, left_upper_long, left_upper_lat, right_lower_long, right_lower_lat, blockSize, overlapSize, output dir = 9
            // input B, 1 coordinate and 1 edge: dop, input dir, left_upper_long, left_upper_lat, aoi_edge, blockSize, overlapSize, output dir = 8

            // ** A -- 2 Coordinates **
            if (params.length != 9) {
                System.out.println("Usage: <dop> <input directory> <left-upper-longitude> " +
                        "<left-upper-latitude> <right-lower-longitude> <right-lower-latitude> <blockSize> <overlapSize> <output path>");
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
                blockSize = Integer.parseInt(params[6]);
                overlapSize = Integer.parseInt(params[7]);
                outputFilePath = params[8];
            }

            // ** B -- 1 Coordinate and 1 Edge **
            if (params.length != 8) {
                System.out.println("Usage: <dop> <input directory> <left-upper-longitude> " +
                        "<left-upper-latitude> <aoi_edge> <blockSize> <overlapSize> <output path>");
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
        //TODO: auf ueberlappende Tiles anpassen
        OverlappingTileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new OverlappingTileInputFormat<>(new Path(filePath), overlapSize);
        enviFormat.setLimitRectangle(aoiLeftUpper, aoiRightLower);
        enviFormat.setTileSize(blockSize, blockSize);
        TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        return new DataSource<>(env, enviFormat, typeInfo, "enviSource");
    }




    public static class InvolvedTileSelector implements FilterFunction<Tuple3<String, byte[], byte[]>> {

        @Override
        public boolean filter(Tuple3<String, byte[], byte[]> tile) throws Exception {
            // TODO aus dem Tuple3 die Tile Koordinaten extrahieren
            OverlappingTileInputFormat<Tuple3<String, byte[], byte[]>> enviFormat = new OverlappingTileInputFormat<>(new Path(filePath), overlapSize);
            Coordinate tileLeftUpper, tileRightLower;
            return enviFormat.rectIntersectsLimits(tileLeftUpper, tileRightLower);
        }
    }

}



