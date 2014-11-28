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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.spatial.Coordinate;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.api.java.spatial.TileInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

/**
 * An input format to parse ENVI files into Tile objects.
 * Every input file is split into adjacent tiles of the given size.
 * Missing pixels are filled with the missing value specified in the ENVI file.
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 */
public class EnviInputFormat<T extends Tile> extends FileInputFormat<T> {
	private static final long serialVersionUID = -6483882465613479436L;
	private static final Logger LOG = LoggerFactory.getLogger(EnviInputFormat.class);

	/**
	 * Configuration parameters: Size of each tile in pixels in both dimensions.
	 */
	public static final String PARAM_XSIZE = "input.xsize";
	public static final String PARAM_YSIZE = "input.ysize";

	private int xsize = -1, ysize = -1;
	private Coordinate leftUpperLimit = null, rightLowerLimit = null;
	
	private TileInfo info;
	private EnviTilePosition pos;
	
	private int readRecords = 0;
	
	public EnviInputFormat(Path path) {
		super(path);
	} 
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		this.xsize = parameters.getInteger(PARAM_XSIZE, -1);
		if(this.xsize <= 0) {
			throw new IllegalArgumentException("Please set the xsize parameter to a positive value.");
		}
		this.ysize = parameters.getInteger(PARAM_YSIZE, -1);
		if(this.ysize <= 0) {
			throw new IllegalArgumentException("Please set the ysize parameter to a positive value.");
		}
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<FileStatus> files = this.getFiles();
		final FileSystem fs = this.filePath.getFileSystem();

		if(minNumSplits < 1) { minNumSplits = 1; }
		
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
		for (FileStatus file : files) {
			// Read header file:
			FSDataInputStream fdis = fs.open(file.getPath());
			TileInfo info = new TileInfo(fdis);
			fdis.close();

			// Determine data file associated with this header:
			String interleaveType = info.getInterleaveType();
			if(!interleaveType.equals("bsq")) {
				throw new RuntimeException("Interleave type " + interleaveType + " unsupported, use bsq.");
			}
			Path dataFile = new Path(file.getPath().toUri().getPath().replaceAll("\\.hdr$", "." + interleaveType));
			FileStatus dataFileStatus;
			try {
				dataFileStatus = fs.getFileStatus(dataFile);
			} catch(FileNotFoundException e) {
				throw new RuntimeException("Data file " + dataFile + " for header " + file + " not found.", e);
			}
			
			if(info.getDataType() != TileInfo.DataTypes.INT) {
				throw new RuntimeException("Data type " + info.getDataType().name() + " is unsupported, use INT."
						+ " File: " + file.getPath());
			}
			int data_size = info.getPixelSize();
			int numBands = info.getNumBands();
			int numRows = info.getPixelRows();
			int numColumns = info.getPixelColumns();
			/*
			 *  Calculate pixel tile size: The rightmost column and lowest row of tiles may contain empty
			 *  pixels.
			 */
			int xsplits = (numColumns + xsize - 1) / xsize;
			int ysplits = (numRows + ysize - 1) / ysize;
			
			// Real coordinates of this image + coordinate differences:
			Coordinate upperLeftCorner = info.getUpperLeftCoordinate();
			Coordinate realLowerRightCorner = info.getLowerRightCoordinate();
			Coordinate realDiff = realLowerRightCorner.diff(upperLeftCorner);
			// Distance between upper left corner and virtual lower right corner of the last tile, including empty pixels:
			Coordinate diff = realDiff.scale(1.0 * xsize / info.getPixelColumns(),
					1.0 * ysize / info.getPixelRows());

			
			LOG.info("Splitting " + numColumns + "x" + numRows + " image into " +
					xsplits + "x" + ysplits + " tiles of size " + xsize + "x" + ysize + " for " + numBands + " bands: " + file.getPath());
	
			for(int band = 0; band < numBands; band++) {
				long bandOffset = band * numRows * numColumns;
				for(int y = 0; y < ysplits; y++) {
					// Calculate pixel coordinate of tile:
					int pystart = y *  ysize; // inclusive
					int pynext = (y + 1) *  ysize; // EXCLUSIVE
	
					for(int x = 0; x < xsplits; x++) {
						// Calculate pixel coordinate of tile:
						int pxstart = x *  xsize; // inclusive
						int pxnext = ((x + 1) % xsplits) *  xsize; // EXCLUSIVE
						int pxnextNoWrapped = (x + 1) *  xsize; // EXCLUSIVE
						
						// Calculate coordinate of leftmost and rightmost pixels
						Coordinate tileUpperLeft = upperLeftCorner.addScaled(diff, x, y);
						Coordinate tileLowerRight = upperLeftCorner.addScaled(diff, x + 1, y + 1);
						
						// Determine start and end position of the pixel block, considering empty pixels at the right and lower boundary:
						long startPos = bandOffset + 1L * pystart * numColumns + pxstart;
						/*
						 *  Pixel position after last pixel in this tile.
						 *  If the next block is in the same row (pxnext >= pxstart), the next start position is not below this block,
						 *  but in the last row of the current block. Thus, decrement pynext in this case.
						 */
						long nextStartPos = bandOffset + 1L * (pxnext < pxstart ? pynext : pynext - 1) * numColumns + pxnext;
						long numPixels = nextStartPos - startPos;
						
						if(LOG.isDebugEnabled()) { LOG.debug("Tile " + x + "x" + y + " startPos: " + startPos +" next: " + nextStartPos); }
						
						long offset = startPos * data_size;
						long length = numPixels * data_size;
						
						if(offset + length > dataFileStatus.getLen()) { // Don't read over the end of file
							length = dataFileStatus.getLen() - offset;
						}
						
						if(LOG.isDebugEnabled()) { LOG.debug("Tile " + x + "x" + y + " at offset " + offset +" +" + length + " bytes"); }
						// Determine list of FS blocks that contain the given block
						final BlockLocation[] blocks = fs.getFileBlockLocations(dataFileStatus, offset, length);
						Arrays.sort(blocks);
						
						inputSplits.add(new EnviInputSplit(inputSplits.size(), dataFile, offset, length,
								blocks[0].getHosts(), info, 
								new EnviTilePosition(pxstart, pxnextNoWrapped, pystart, pynext, tileUpperLeft, tileLowerRight)));
					}
				}
			}
		}

		if (inputSplits.size() < minNumSplits) {
			LOG.warn("WARNING: Too few splits generated (" + inputSplits.size() +"), adding dummy splits.");
			for (int index = files.size(); index < minNumSplits; index++) {
				inputSplits.add(new EnviInputSplit(index, null, 0, 0, null, null, null));
			}
		}

		return inputSplits.toArray(new EnviInputSplit[0]);
	}

	protected List<FileStatus> getFiles() throws IOException {
		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = this.filePath.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(this.filePath);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] partials = fs.listStatus(this.filePath);
			for (FileStatus partial : partials) {
				if (!partial.isDir()) {
					files.add(partial);
				}
			}
		} else {
			files.add(pathFile);
		}
		
		List<FileStatus> headerFiles = new ArrayList<FileStatus>();
		for(FileStatus fstat: files) {
			// Only accept header files
			if(fstat.getPath().toUri().toString().endsWith(".hdr")) {
				headerFiles.add(fstat);
			}
		}
		
		return files;
	}

	@Override
	public SequentialStatistics getStatistics(BaseStatistics cachedStats) {
		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
			(FileBaseStatistics) cachedStats : null;

		try {
			final Path filePath = this.filePath;

			// get the filesystem
			final FileSystem fs = FileSystem.get(filePath.toUri());
			final ArrayList<FileStatus> allFiles = new ArrayList<FileStatus>(1);

			// let the file input format deal with the up-to-date check and the basic size
			final FileBaseStatistics stats = getFileStats(cachedFileStats, filePath, fs, allFiles);
			if (stats == null) {
				return null;
			}

			// check whether the file stats are still sequential stats (in that case they are still valid)
			if (stats instanceof SequentialStatistics) {
				return (SequentialStatistics) stats;
			}
			return createStatistics(allFiles, stats);
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("Could not determine complete statistics for file '%s' due to an I/O error: %s",
					this.filePath, StringUtils.stringifyException(ioex)));
			}
		} catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error(String.format("Unexpected problem while getting the file statistics for file '%s' due to %s",
					this.filePath, StringUtils.stringifyException(t)));
			}
		}
		// no stats available
		return null;
	}

	protected FileInputSplit[] getInputSplits() throws IOException {
		return this.createInputSplits(0);
	}

	/**
	 * Fill in the statistics. The last modification time and the total input size are prefilled.
	 * 
	 * @param files
	 *        The files that are associated with this block input format.
	 * @param stats
	 *        The pre-filled statistics.
	 */
	protected SequentialStatistics createStatistics(List<FileStatus> files, FileBaseStatistics stats)
			throws IOException {
		if (files.isEmpty()) {
			return null;
		}
		final FileSystem fs = this.filePath.getFileSystem();
		long totalCount = 0;
		long totalWidth = 0;
		for (FileStatus file : files) {
			// Read header file:
			FSDataInputStream fdis = fs.open(file.getPath());
			TileInfo info = new TileInfo(fdis);
			fdis.close();
	
			// Calculate the number of splits, as done above:
			int xsplits = (info.getPixelColumns() + xsize - 1) / xsize;
			int ysplits = (info.getPixelRows() + ysize - 1) / ysize;
			
			// Count the tiles and calculate the size of each tile in bytes (virtual size):
			int tileCount = xsplits * ysplits * info.getNumBands();
			totalCount += tileCount;
			totalWidth += 1L * info.getPixelSize() * tileCount * xsize * ysize;
		}

		final float avgWidth = totalCount == 0 ? 0 : 1.0f * totalWidth / totalCount;
		return new SequentialStatistics(stats.getLastModificationTime(), stats.getTotalInputSize(), avgWidth,
			totalCount);
	}

	private static class SequentialStatistics extends FileBaseStatistics {

		private final long numberOfRecords;

		public SequentialStatistics(long fileModTime, long fileSize, float avgBytesPerRecord, long numberOfRecords) {
			super(fileModTime, fileSize, avgBytesPerRecord);
			this.numberOfRecords = numberOfRecords;
		}

		@Override
		public long getNumberOfRecords() {
			return this.numberOfRecords;
		}
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		this.info = ((EnviInputSplit) split).info;
		this.pos = ((EnviInputSplit) split).pos;

		if(LOG.isDebugEnabled()) { LOG.debug("Opened ENVI file " + split.getPath() + " with positions: " + pos); }
		
		this.readRecords = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.readRecords > 0;
	}

	@Override
	public T nextRecord(T record) throws IOException {
		if (this.reachedEnd()) {
			return null;
		}
		
		record = readEnviTile(record);
		this.readRecords++;
		return record;
	}

	private T readEnviTile(T record) throws IOException {
		/*
		 * Determine how may pixels to read from the file.
		 * All remaining pixels are filled with missing values
		 */
		int lineWidth = this.info.getPixelColumns();
		int xread = lineWidth - this.pos.xstart;
		if(xread > xsize) { xread = xsize; }
		int yread = this.info.getPixelRows() - this.pos.ystart;
		if(yread > ysize) { yread = ysize; }
		
		record.update(this.info, this.pos.leftUpperCorner, this.pos.rightLowerCorner, xsize, ysize);
		short[] values = record.getS16Tile();
		if(values == null) {
			values = new short[xsize * ysize];
			record.setS16Tile(values);
		}
		short missingData = (short) info.getMissingValue();
		int data_size = info.getPixelSize();
		
		//LOG.info("Reading " + xread + "x" + yread + " pixels from file into " + xsize + "x" + ysize + " tile");
		
		// Fill pixel array pixel by pixel (please optimise this!):
		int pos = 0;
		for(int y = 0; y < yread; y++) {
			// Seek to the beginning of the current line of real data:
			stream.seek(y * lineWidth * data_size + this.splitStart);
			// Fill in pixels (little endian):
			for(int x = 0; x < xread; x++) {
				int b0 = stream.read();
				int b1 = stream.read();
				short val = (short)(b0 | (b1 << 8));
				values[pos++] = val;
			}
			// Fill with empty columns:
			for(int x = xread; x < xsize; x++) {
				values[pos++] = missingData;
			}
		}
		// Fill missing rows with empty data:
		while(pos < values.length) {
			values[pos++] = missingData;
		}

		return record;
	}
	
	
	public static final class EnviTilePosition {
		public final int xstart, xnext;
		public final int ystart, ynext;
		public final Coordinate leftUpperCorner, rightLowerCorner;
		
		public EnviTilePosition(int xstart, int xnext, int ystart, int ynext, Coordinate leftUpperCorner, Coordinate rightLowerCorner) {
			this.xstart = xstart;
			this.xnext = xnext;
			this.ystart = ystart;
			this.ynext = ynext;
			this.leftUpperCorner = leftUpperCorner;
			this.rightLowerCorner = rightLowerCorner;
		}
		
		@Override
		public String toString() {
			return "x:" + xstart + "--" + xnext + ", y:" + ystart + "--" + ynext + ", left Upper: " + leftUpperCorner + ", rightLower: " + rightLowerCorner;
		}
	}
	
	public static final class EnviInputSplit extends FileInputSplit {
		private static final long serialVersionUID = -9205048860784884871L;
		public final TileInfo info;
		public final EnviTilePosition pos;

		public EnviInputSplit() {
			super();
			this.info = null;
			this.pos = null;
		}
		
		public EnviInputSplit(int num, Path file, long start, long length, String[] hosts, TileInfo info, EnviTilePosition pos) {
			super(num, file, start, length, hosts);
			this.info = info;
			this.pos = pos;
		}
	}

	public void setLimitRectangle(Coordinate leftUpperLimit,
			Coordinate rightLowerLimit) {
		this.leftUpperLimit = leftUpperLimit;
		this.rightLowerLimit = rightLowerLimit;
	}
}
