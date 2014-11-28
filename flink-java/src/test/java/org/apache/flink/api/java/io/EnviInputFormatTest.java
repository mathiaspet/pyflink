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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.Assert;
import org.apache.flink.api.java.io.EnviInputFormat.EnviInputSplit;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.configuration.Configuration;
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
		String name = "blob";

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
		
		EnviInputFormat<Tile> eif = new EnviInputFormat<Tile>(path);
		eif.setTileSize(7, 3);
		checkResult(eif, expectedDataBlocks1);
	}
	
	@Test
	public void testSubBlocks() throws IOException {
		Path path = prepareInput(dataBlock1, header1);
		
		EnviInputFormat<Tile> eif = new EnviInputFormat<Tile>(path);
		eif.setTileSize(4, 2);
		checkResult(eif, expectedSubBlocks1);
	}

	@Test
	public void testIntersect() throws IOException {
		Path path = prepareInput(dataBlock1, header1);
		
		EnviInputFormat<Tile> eif = new EnviInputFormat<Tile>(path);
		eif.setTileSize(4, 2);
		eif.setLimitRectangle(new Coordinate(20, -54), new Coordinate(-8, -30));
		checkResult(eif, expectedIntersectBlocks);
	}

	private void checkResult(EnviInputFormat<Tile> eif, short[][] result) throws IOException {
		EnviInputFormat.EnviInputSplit[] splits = (EnviInputSplit[]) eif.createInputSplits(-1);
		Assert.assertEquals("Sub splits generated", result.length, splits.length);
		int i = 0;
		for(EnviInputFormat.EnviInputSplit split: splits) {
			eif.open(split);
			Tile tile = new Tile();
			eif.nextRecord(tile);
			eif.close();
			Assert.assertArrayEquals(result[split.getSplitNumber()], tile.getS16Tile());
			Assert.assertEquals("Band is correct for split " + i, i >= result.length / 2 ? 1 : 0, tile.getBand());
			i++;
		}
	}
}
