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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.api.java.spatial.TileTypeInformation;
import org.apache.flink.api.java.spatial.envi.SceneInputFormat;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

//import org.apache.flink.api.java.io.EnviReader;

/**
 * Example to select a tile from a time series of scenes and to create a cubic
 * representation of it.
 * 1 file:///opt2/gms_sample/ 227064 file:///opt2/gms_sample/out
 * 4 hdfs://localhost:50041/geo 227064 hdfs://localhost:50041/out
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class ReadCompleteScenes {

	private static int dop;
	private static String filePath;
	private static String pathRow;
	private static int blockSize; // squared blocks for the beginning
	private static String outputFilePath;
	private static int pixelSize;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(dop);
		
		DataSet<Tile> tiles = readTiles(env);
		DataSet<Tile> filtered = tiles.filter(new FilterFunction<Tile>() {
			int count = 0;
			@Override
			public boolean filter(Tile value) throws Exception {
				count++;
				System.out.print("counted: " + count);
				return true;
			}
		});
		DataSink<Tile> writeAsEnvi = filtered.writeAsEnvi(outputFilePath, WriteMode.OVERWRITE);
		
		writeAsEnvi.setParallelism(1);
			
		env.execute("Read Complete Scenes");
	}

	private static boolean parseParameters(String[] params) {

		if (params.length > 0) {
			if (params.length != 4) {
				System.out
						.println("Usage: <dop> <input directory> <pathrow> <output path>");
				return false;
			} else {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				pathRow = params[2];
				outputFilePath = params[3];
			}
		} else {
			System.out
					.println("Usage: <dop> <input directory> <pathrow> <output path>");
			return false;
		}

		return true;
	}

	private static DataSet<Tile> readTiles(ExecutionEnvironment env) {
		SceneInputFormat<Tile> enviFormat = new SceneInputFormat<Tile>(new Path(filePath));
		enviFormat.setCompleteScene(true);
		enviFormat.setPathRow(pathRow);

		return new DataSource<Tile>(env, enviFormat, new TileTypeInformation(), "enviSource");
	}

}
