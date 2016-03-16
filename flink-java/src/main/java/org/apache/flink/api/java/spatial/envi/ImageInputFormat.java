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

// import java.io.Serializable;
// import java.nio.ByteBuffer;
// import java.nio.ByteOrder;
// import java.util.Arrays;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// import org.apache.flink.api.common.io.statistics.BaseStatistics;
// import org.apache.flink.api.java.spatial.Coordinate;
// import org.apache.flink.api.java.spatial.TileInfoWrapper;
import org.apache.flink.core.fs.BlockLocation;
// import org.apache.flink.util.StringUtils;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.spatial.ImageWrapper;
import org.apache.flink.api.java.spatial.ImageInfoWrapper;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImageInputFormat extends FileInputFormat<ImageWrapper> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ImageInputFormat.class);

	private String currentKey;
	private ImageInfoWrapper currentInfo;

	public ImageInputFormat(Path path) {
		super(path);
	} 

	@Override
	public ImageWrapper nextRecord(ImageWrapper reuse) throws IOException {
		if (this.reachedEnd()) {
			return null;
		}

		// Read image data from stream
		int dataSize = this.currentInfo.getLines() * this.currentInfo.getSamples() * this.currentInfo.getPixelSize();
		byte[] data = new byte[dataSize];
		if (stream.read(data) != dataSize) {
			throw new RuntimeException("Unexpected file size (" + dataSize + ") while reading data file.");
		}

		reuse.setFields(this.currentKey, this.currentInfo.toBytes(), data); 

		this.currentKey = null;
		this.currentInfo = null;
		return reuse;
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		ImageInputSplit imageSplit = (ImageInputSplit) split;
		
		this.currentKey = imageSplit.key;
		this.currentInfo = imageSplit.info;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.currentInfo == null;
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}

		List<FileStatus> headerFiles = getHeaderFiles();
		List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(headerFiles.size());

		final FileSystem fs = this.filePath.getFileSystem();
		for (FileStatus header: headerFiles) {
			FSDataInputStream headerIn = fs.open(header.getPath());
			try {
				// Read header
				ImageInfoWrapper info = new ImageInfoWrapper(headerIn);
				headerIn.close();
				
				// Determine data file for header
				String interleaveType = info.getInterleave();
				if (!interleaveType.equals("bsq")) {
					throw new RuntimeException("Interleave type " + interleaveType + " unsupported, use bsq.");
				}
				Path data = new Path(this.filePath.getFileSystem().getUri() + header.getPath().toUri().getPath().replaceAll("\\.hdr$", "." + interleaveType));
				FileStatus dataStatus;
				try {
					dataStatus = fs.getFileStatus(data);
				} catch(FileNotFoundException e) {
					throw new RuntimeException("Data file " + data + " for header " + header + " not found.", e);
				}

				// Determine blocks
				final BlockLocation[] blocks = fs.getFileBlockLocations(dataStatus, 0, dataStatus.getLen());

				// Create split
				inputSplits.add(new ImageInputSplit(inputSplits.size(), data, 0, dataStatus.getLen(), blocks[0].getHosts(), info.getAcquisitionDate(), info));
			} catch (RuntimeException e){
				LOG.warn(e.getMessage(), e);
				continue;
			}
		}

		return inputSplits.toArray(new FileInputSplit[0]);
	}

	protected List<FileStatus> getHeaderFiles() throws IOException {
		List<FileStatus> headerFiles = new ArrayList<FileStatus>();

		final FileSystem fs = this.filePath.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(this.filePath);

		if (pathFile.isDir()) {
			// Add all header files in directory
			final FileStatus[] partials = fs.listStatus(this.filePath);
			for (FileStatus partial : partials) {
				if (!partial.isDir() && partial.getPath().toUri().toString().endsWith(".hdr")) {
					headerFiles.add(partial);
				}
			}
		} else {
			headerFiles.add(pathFile);
		}
		
		return headerFiles;
	}

	/**
	 * File input split with meta data from header file.
	 */
	public static final class ImageInputSplit extends FileInputSplit {
		private static final long serialVersionUID = 1L;
		public String key;
		public ImageInfoWrapper info;

		public ImageInputSplit(int num, Path file, long start, long length, String[] hosts, String key, ImageInfoWrapper info) {
			super(num, file, start, length, hosts);
			this.key = key;
			this.info = info;
		}
	}
}
