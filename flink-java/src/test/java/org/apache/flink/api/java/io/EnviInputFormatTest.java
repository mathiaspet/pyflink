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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.Assert;
import org.apache.flink.api.java.spatial.envi.TileInputFormat;
import org.apache.flink.api.java.spatial.envi.TileInputFormat.EnviInputSplit;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.core.fs.Path;
import org.junit.After;
import org.junit.Test;

public class EnviInputFormatTest {
	private static final byte[] dataBlock1 = {
		00, 0,  1, 0,  2, 0,  3, 0,  4, 0,  5, 0,  6, 0,
		07, 0,  8, 0,  9, 0, 10, 0, 11, 0, 12, 0, 13, 0,
		14, 0, 15, 0, 16, 0, 17, 0, 18, 0, 19, 0, 20, 0,
		32, 0, 33, 0, 34, 0, 35, 0, 36, 0, 37, 0, 38, 0,
		40, 0, 41, 0, 42, 0, 43, 0, 44, 0, 45, 0, 46, 0,
		50, 0, 51, 0, 52, 0, 53, 0, 54, 0, 55, 0, 56, 0

	};

	private static final short[] expectedDataBlocks1[] = {
		{
			0, 1, 2, 3, 4, 5, 6,
			7, 8, 9, 10, 11, 12, 13,
			14, 15, 16, 17, 18, 19, 20
		},
		{
			32, 33, 34, 35, 36, 37, 38,
			40, 41, 42, 43, 44, 45, 46,
			50, 51, 52, 53, 54, 55, 56
		}
	};
	
	private static final short[][] expectedSubBlocks1 = {
		{
			0, 1, 2, 3,
			7, 8, 9, 10
		},
		{
			04,  5,  6, -1,
			11, 12, 13, -1 
		},
		{
			14, 15, 16, 17,
			-1, -1, -1, -1
		},
		{
			18, 19, 20, -1,
			-1, -1, -1, -1
		},
		{
			32, 33, 34, 35,
			40, 41, 42, 43,
		},
		{
			36, 37, 38, -1,
			44, 45, 46, -1
		},
		{
			50, 51, 52, 53,
			-1, -1, -1, -1
		},
		{
			54, 55, 56, -1,
			-1, -1, -1, -1
		}
	};

	private static final short[][] expectedIntersectBlocks = {
		{
			04,  5,  6, -1,
			11, 12, 13, -1 
		},
		{
			18, 19, 20, -1,
			-1, -1, -1, -1
		},
		{
			36, 37, 38, -1,
			44, 45, 46, -1
		},
		{
			54, 55, 56, -1,
			-1, -1, -1, -1
		}
	};

	
	String header1 = "ENVI\n" +
			"samples = 7\n" +
			"byte order = 0\n" +
			"lines = 3\n" +
			"data type = 2\n" +
			"interleave=bsq\n" +
			"map info = {South_America_Albers_Equal_Area_Conic, 1.0000, 1.0000, 430404.0572, 3120036.4653, 3.000000e+001, 3.000000e+001, South American 1969 mean, units=Meters}\n" +
			"projection info = {9, 6378160.0, 6356774.7, -32.000000, -60.000000, 0.0, 0.0, -5.000000, -42.000000, South American 1969 mean, South_America_Albers_Equal_Area_Conic, units=Meters}\n" +  
			"coordinate system string = {PROJCS[\"South_America_Albers_Equal_Area_Conic\",GEOGCS[\"GCS_South_American_1969\",DATUM[\"D_South_American_1969\",SPHEROID[\"GRS_1967_Truncated\",6378160.0,298.25]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Albers\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"central_meridian\",-60.0],PARAMETER[\"Standard_Parallel_1\",-5.0],PARAMETER[\"Standard_Parallel_2\",-42.0],PARAMETER[\"latitude_of_origin\",-32.0],UNIT[\"Meter\",1.0]]}\n" + 
			"bands = 2\n" +
			"data ignore value = -1\n" +
			"upperleftcornerlatlong = {\n" +
			"-4.835949, -56.076531}\n" +
			"lowerrightcornerlatlong = {\n" +
			"-6.721377, -53.949345}\n"
			;
	
	java.nio.file.Path tempDir = null;
	
	@After
	public void cleanUp() throws IOException {
		if(tempDir == null) { return;}
		Files.walkFileTree(tempDir, new FileVisitor<java.nio.file.Path>() {
			@Override
			public FileVisitResult preVisitDirectory(java.nio.file.Path dir,
					BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(java.nio.file.Path file,
					BasicFileAttributes attrs) throws IOException {
				Files.deleteIfExists(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(java.nio.file.Path file, IOException exc)
					throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc)
					throws IOException {
				Files.deleteIfExists(dir);
				return FileVisitResult.CONTINUE;
			}			 
			
		});
	}
	
	Path prepareInput(byte[] data, String header) throws IOException {
		String name = "227064_00020212_blob";

		// create temporary file with 3 blocks
		if(tempDir == null) {
			tempDir = Files.createTempDirectory("enviInputTest");
		}

		File tempFile = new File(tempDir.toFile(), name + ".bsq");
		FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
		fileOutputStream.write(data, 0, data.length);
		fileOutputStream.close();

		File headerFile = new File(tempDir.toFile(), name + ".hdr");
		PrintStream ps = new PrintStream(new FileOutputStream(headerFile));
		ps.print(header);
		ps.close();
		
		return new Path(headerFile.toURI());
	}
	
	@Test
	public void testSingleBlock() throws IOException {
		Path path = prepareInput(dataBlock1, header1);
		
		TileInputFormat<Tile> eif = new TileInputFormat<Tile>(path);
		eif.setTileSize(7, 3);
		Coordinate[][] expectedCoords = new Coordinate[2][2];
		Coordinate[] firstRow = new Coordinate[2];
		Coordinate leftUpper1 = new Coordinate(430404.0572, 3120036.4653);
		Coordinate rightLower1 = new Coordinate(430404.0572 + 6 * 30.0, 3120036.4653 - 2 * 30.0);
		firstRow[0] = leftUpper1;
		firstRow[1] = rightLower1;
		//same coordinates since these are different bands
		expectedCoords[0] = firstRow;
		expectedCoords[1] = firstRow;
		
		checkResult(eif, expectedDataBlocks1, expectedCoords);
	}
	
	@Test
	public void testSubBlocks() throws IOException {
		Path path = prepareInput(dataBlock1, header1);
		
		TileInputFormat<Tile> eif = new TileInputFormat<Tile>(path);
		eif.setTileSize(4, 2);
		
		Coordinate[][] expectedCoords = new Coordinate[8][2];
		Coordinate[] firstRow = new Coordinate[2];
		Coordinate leftUpper1 = new Coordinate(430404.0572, 3120036.4653);
		Coordinate rightLower1 = new Coordinate(430404.0572 + 3 * 30.0, 3120036.4653 - 1 * 30.0);
		firstRow[0] = leftUpper1;
		firstRow[1] = rightLower1;
		expectedCoords[0] = firstRow;
		
		Coordinate[] secondRow = new Coordinate[2];
		Coordinate leftUpper2 = new Coordinate(430404.0572 + 4 * 30.0, 3120036.4653);
		Coordinate rightLower2 = new Coordinate(430404.0572 + 7 * 30.0, 3120036.4653 - 1 * 30.0);
		secondRow[0] = leftUpper2;
		secondRow[1] = rightLower2;
		expectedCoords[1] = secondRow;
		
		Coordinate[] thirdRow = new Coordinate[2];
		Coordinate leftUpper3 = new Coordinate(430404.0572, 3120036.4653 - 2 * 30.0);
		Coordinate rightLower3 = new Coordinate(430404.0572 + 3 * 30.0, 3120036.4653 - 3 * 30.0);
		thirdRow[0] = leftUpper3;
		thirdRow[1] = rightLower3;
		expectedCoords[2] = thirdRow;
		
		Coordinate[] fourthRow = new Coordinate[2];
		Coordinate leftUpper4 = new Coordinate(430404.0572  + 4 * 30.0, 3120036.4653 - 2 * 30.0);
		Coordinate rightLower4 = new Coordinate(430404.0572 + 7 * 30.0, 3120036.4653 - 3 * 30.0);
		fourthRow[0] = leftUpper4;
		fourthRow[1] = rightLower4;
		expectedCoords[3] = fourthRow;
		
		expectedCoords[4] = firstRow;
		expectedCoords[5] = secondRow;
		expectedCoords[6] = thirdRow;
		expectedCoords[7] = fourthRow;
		
		checkResult(eif, expectedSubBlocks1, expectedCoords);
	}

	@Test
	public void testIntersect() throws IOException {
		Path path = prepareInput(dataBlock1, header1);
		
		TileInputFormat<Tile> eif = new TileInputFormat<Tile>(path);
		eif.setTileSize(4, 2);
		eif.setLimitRectangle(new Coordinate(430404.0572 + 5 * 30.0, 3120036.4653 - 1 * 30.0), 
				new Coordinate(430404.0572 + 6 * 30.0, 3120036.4653 - 2 * 30.0));
		
		Coordinate[][] expectedCoords = new Coordinate[4][2];
		
		Coordinate[] secondRow = new Coordinate[2];
		Coordinate leftUpper2 = new Coordinate(430404.0572 + 4 * 30.0, 3120036.4653);
		Coordinate rightLower2 = new Coordinate(430404.0572 + 7 * 30.0, 3120036.4653 - 1 * 30.0);
		secondRow[0] = leftUpper2;
		secondRow[1] = rightLower2;
		expectedCoords[0] = secondRow;
		
		Coordinate[] fourthRow = new Coordinate[2];
		Coordinate leftUpper4 = new Coordinate(430404.0572  + 4 * 30.0, 3120036.4653 - 2 * 30.0);
		Coordinate rightLower4 = new Coordinate(430404.0572 + 7 * 30.0, 3120036.4653 - 3 * 30.0);
		fourthRow[0] = leftUpper4;
		fourthRow[1] = rightLower4;
		expectedCoords[1] = fourthRow;
		
		expectedCoords[2] = secondRow;
		expectedCoords[3] = fourthRow;

		
		checkResult(eif, expectedIntersectBlocks, expectedCoords);
	}

	private void checkResult(TileInputFormat<Tile> eif, short[][] result, Coordinate[][] expectedCoords) throws IOException {
		TileInputFormat.EnviInputSplit[] splits = (EnviInputSplit[]) eif.createInputSplits(-1);
		Assert.assertEquals("Sub splits generated", result.length, splits.length);
		int i = 0;
		for(TileInputFormat.EnviInputSplit split: splits) {
			eif.open(split);
			Tile tile = new Tile();
			eif.nextRecord(tile);
			eif.close();
			
			Assert.assertArrayEquals(result[split.getSplitNumber()], tile.getS16Tile());
			Coordinate leftUpper = expectedCoords[i][0];
			assertThat(tile.getNWCoord(), is(equalTo(leftUpper)));
			Coordinate rightLower = expectedCoords[i][1];
			assertThat(tile.getSECoord(), is(equalTo(rightLower)));
			Assert.assertEquals("Band is correct for split " + i, i >= result.length / 2 ? 1 : 0, tile.getBand());
			i++;
		}
	}
}
