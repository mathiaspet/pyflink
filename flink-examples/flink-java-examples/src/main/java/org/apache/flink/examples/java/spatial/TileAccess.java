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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.EnviReader;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.Tile;

/**
 * 
 * Simple Map-Reduce job to select a tile out of one foot print. 
 * 
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class TileAccess {

	private static Coordinate upperLeft, lowerRight;
	private static String path;
	private static int dop;
	
	
	public static void main(String[] args) {
		
		if(!parseParameters(args)) {
			return;
		}	
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tile> tileSet = readTileDataSet(env); 
			
		tileSet.map(mapper)	
		
		
	}

	private static DataSet<Tile> readTileDataSet(ExecutionEnvironment env) {
		
		EnviReader readEnviFile = env.readEnviFile(path);
//		readEnviFile.
		
		return null;
	}

	/**
	 * TODO: parse coordinates, etc.
	 * 
	 * @param args
	 * @return
	 */
	private static boolean parseParameters(String[] arguments) {
		if(arguments.length > 0) {
			if(arguments.length != 5) {
				return false;
			}
			dop = Integer.parseInt(arguments[0]);
			
			path = Integer.parseInt(arguments[1]);
			row = Integer.parseInt(arguments[2]);
			
			double upperLat = Double.parseDouble(arguments[3]);
			double upperLong = Double.parseDouble(arguments[4]);
			
			double lowerLat = Double.parseDouble(arguments[5]);
			double lowerLong = Double.parseDouble(arguments[6]);
			
			upperLeft = new Coordinate(upperLat, upperLong);
			lowerRight = new Coordinate(lowerLat, lowerLong);
			
		}else{
			System.err.println("Usage: dop path row upperleft lat long lower right lat long ");
			return false;
		}
		
		
		return true;
	}
	
}
