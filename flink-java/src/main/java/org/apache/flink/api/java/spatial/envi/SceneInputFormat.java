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

package org.apache.flink.api.java.spatial.envi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.spatial.Scene;
import org.apache.flink.api.java.spatial.TileInfo;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input format to parse ENVI files into scene objects.
 * Missing pixels are filled with the missing value specified in the ENVI file.
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
public class SceneInputFormat<T extends Scene> extends FileInputFormat<T> {
	private static final long serialVersionUID = -6483882465613479436L;
	private static final Logger LOG = LoggerFactory.getLogger(SceneInputFormat.class);

	private int xsize = -1, ysize = -1;
	
	private TileInfo info;
	private String pathRow;
	
	private int readRecords = 0;


	public SceneInputFormat(Path path) {
		super(path);
	} 
	
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<FileStatus> files = this.getFiles();
		final FileSystem fs = this.filePath.getFileSystem();
		
		if(minNumSplits < 1) { minNumSplits = 1; }
		
		List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
		for (FileStatus file : files) {
			// Read header file:
			FSDataInputStream fdis = fs.open(file.getPath());
			try{
				TileInfo info = new TileInfo(fdis);
				fdis.close();

				// Determine data file associated with this header:
				int interleaveType = info.getInterleave();
				if(interleaveType != 0) {
					throw new RuntimeException("Interleave type " + interleaveType + " unsupported, use bsq.");
				}
				
				Path dataFile = new Path(this.filePath.getFileSystem().getUri() + file.getPath().toUri().getPath().replaceAll("\\.hdr$", "." + TileInfo.InterleaveTypes.values()[interleaveType]));
				FileStatus dataFileStatus;
				try {
					dataFileStatus = fs.getFileStatus(dataFile);
				} catch(FileNotFoundException e) {
					throw new RuntimeException("Data file " + dataFile + " for header " + file + " not found.", e);
				}

				if(this.pathRow != null && !dataFile.getName().contains(this.pathRow)) {
					continue;
				}
				
				if(info.getDataType() != TileInfo.DataTypes.INT) {
					throw new RuntimeException("Data type " + info.getDataType().name() + " is unsupported, use INT."
							+ " File: " + file.getPath());
				}
				
				int data_size = info.getPixelSize();
				int numRows = info.getLines();
				int numColumns = info.getSamples();
				
				String filePath = file.getPath().toString();
				int lastIndexOf = filePath.lastIndexOf("/");
				filePath = filePath.substring(lastIndexOf + 1);
				String[] split = filePath.split("_");

				// Determine list of FS blocks that contain the given block
				final BlockLocation[] blocks = fs.getFileBlockLocations(dataFileStatus, 0, numRows * numColumns);
				Arrays.sort(blocks);

				SceneInputSplit sceneSplit = new SceneInputSplit(1, dataFile, 0, data_size,
						blocks[0].getHosts(), info);
				inputSplits.add(sceneSplit);
				
			}catch (RuntimeException ex) {
				LOG.warn(ex.getMessage(), ex);
				continue;
			}

		}

		if (inputSplits.size() < minNumSplits) {
			LOG.warn("WARNING: Too few splits generated (" + inputSplits.size() +"), adding dummy splits.");
			for (int index = files.size(); index < minNumSplits; index++) {
				inputSplits.add(new SceneInputSplit(index, null, 0, 0, null, null));
			}
		}

		return inputSplits.toArray(new SceneInputSplit[0]);
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
				if (!partial.isDir() && partial.getPath().toUri().toString().endsWith(".hdr")) {
					files.add(partial);
				}
			}
		} else {
			files.add(pathFile);
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
			if (file.getPath().toString().endsWith("bsq")) {
				continue;
			}
			// Read header file:
			FSDataInputStream fdis = fs.open(file.getPath());
			TileInfo info = new TileInfo(fdis);
			fdis.close();

			// Calculate the number of splits, as done above:
			int xsplits = (info.getSamples() + xsize - 1) / xsize;
			int ysplits = (info.getLines() + ysize - 1) / ysize;
			
			// Count the tiles and calculate the size of each tile in bytes (virtual size):
			int tileCount = xsplits * ysplits * info.getBands();
			totalCount += tileCount;
			totalWidth += 1L * info.getPixelSize() * tileCount * xsize * ysize;
		}

		final float avgWidth = totalCount == 0 ? 0 : 1.0f * totalWidth / totalCount;
		return new SequentialStatistics(stats.getLastModificationTime(), stats.getTotalInputSize(), avgWidth,
			totalCount);
	}

	public void setPathRow(String pathRow) {
		this.pathRow = pathRow;
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
		
		this.info = ((SceneInputSplit) split).info;
		
		if(LOG.isDebugEnabled()) { LOG.debug("Opened ENVI file " + split.getPath()); }
		
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

	//TODO: finish!!!
	private T readEnviTile(T record) throws IOException {
		/*
		 * Determine how many pixels to read from the file.
		 * All remaining pixels are filled with missing values
		 */
		int lineWidth = this.info.getSamples();
		int xread = lineWidth;
		int yread = this.info.getLines() * this.info.getBands();
		
		record.update(this.info);
		short[] values = record.getS16Tile();
		if(values == null) {
			values = new short[xread * yread];
			record.setS16Tile(values);
		}
		short dataIgnoreValue = (short) info.getDataIgnoreValue();
		int data_size = info.getPixelSize();
		
		// Fill pixel array pixel by pixel (please optimize this!):
		int pos = 0;
		
		byte[] buffer = new byte[xread * 2];
		short[] shortBuffer = new short[xread];
		
		for(int y = 0; y < yread; y++) {
			// Seek to the beginning of the current line of real data:
			stream.seek(y * lineWidth * data_size + this.splitStart);
			// Fill in pixels (little endian):
			int read = stream.read(buffer);
			if(read < xread) {
				LOG.warn("Should've read " + xread + " pixel, but read only " + read);
			}
			ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(shortBuffer);
			System.arraycopy(shortBuffer, 0, values, pos, xread);
			pos = pos + xread;
			
		}
		// Fill missing rows with empty data:
		while(pos < values.length) {
			values[pos++] = dataIgnoreValue;
		}
		return record;
	}
	
}
