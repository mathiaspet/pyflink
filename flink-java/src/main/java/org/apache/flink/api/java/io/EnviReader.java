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

package org.apache.flink.api.java.io;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.api.java.spatial.TileTypeInformation;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.core.fs.Path;

import com.google.common.base.Preconditions;

/**
 * A builder class to instantiate a data source that parses ENVI files.
 * TODO: Describe parameters
 */
@Deprecated
public class EnviReader {

	private final Path path;

	private final ExecutionEnvironment executionContext;
	
	private Coordinate leftUpperLimit, rightLowerLimit;
	
	private final int xpixels, ypixels; 
	
	// --------------------------------------------------------------------------------------------
	
	public EnviReader(Path filePath, int xpixels, int ypixels, ExecutionEnvironment executionContext) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		Preconditions.checkNotNull(executionContext, "The execution context may not be null.");
		Preconditions.checkArgument(xpixels > 0,  "Positive x pixel count required.");
		Preconditions.checkArgument(xpixels <= Integer.MAX_VALUE, "Positive x pixel count required.");
		Preconditions.checkArgument(ypixels > 0,  "Positive x pixel count required.");
		Preconditions.checkArgument(ypixels <= Integer.MAX_VALUE, "Positive x pixel count required.");
		
		this.path = filePath;
		this.executionContext = executionContext;
		this.xpixels = xpixels;
		this.ypixels = ypixels;
	}
	
	public EnviReader(String filePath, int xpixels, int ypixels, ExecutionEnvironment executionContext) {
		this(new Path(Preconditions.checkNotNull(filePath, "The file path may not be null.")), xpixels, ypixels, executionContext);
	}
	
	public Path getFilePath() {
		return this.path;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Instructs the input reader to only return tiles that share an area with the rectangle given
	 * by the supplied coordinates.
	 * 
	 * @return The reader instance itself, to support function chaining.
	 */
	public EnviReader restrictTo(Coordinate leftUpper, Coordinate rightLower) {
		if(leftUpper == null || rightLower == null) {
			throw new IllegalArgumentException("Both coordinates may not be null.");
		}
		
		this.leftUpperLimit = leftUpper;
		this.rightLowerLimit = rightLower;
		return this;
	}
	
	private void configureInputFormat(TileInputFormat<?> format) {
		format.setLimitRectangle(leftUpperLimit, rightLowerLimit);
		format.setTileSize(xpixels, ypixels);
	}

	/**
	 * Build the data source from this reader.
	 * This creates a data source with the configuration of this reader.
	 *
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed ENVI tiles.
	 */
	public DataSource<Tile> build() {
		TileInputFormat<Tile> inputFormat = new TileInputFormat<Tile>(path);
		configureInputFormat(inputFormat);
		return new DataSource<Tile>(executionContext, inputFormat, new TileTypeInformation(), Utils.getCallLocationName());
	}
}
