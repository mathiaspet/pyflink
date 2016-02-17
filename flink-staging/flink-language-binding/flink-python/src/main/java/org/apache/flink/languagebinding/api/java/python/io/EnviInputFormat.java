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

package org.apache.flink.languagebinding.api.java.python.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input format to parse ENVI files into Tile objects.
 * Every input file is split into adjacent tiles of the given size.
 * Missing pixels are filled with the missing value specified in the ENVI file.
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
//public class EnviInputFormat<T extends Tile> extends FileInputFormat<T> {
public class EnviInputFormat<T extends Tile> extends SpatialInputFormat<T> {	
	static final Logger LOG = LoggerFactory.getLogger(EnviInputFormat.class);

	public EnviInputFormat(Path path) {
		super(path);
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
		 * Determine how many pixels to read from the file.
		 * All remaining pixels are filled with missing values
		 */
		int lineWidth = this.info.getSamples();
		double pixelWidth = this.info.getPixelWidth();
		double pixelHeight = this.info.getPixelHeight();

		//TODO: find out why xsize and ysize are -1 here
		if(this.completeScene) {
			this.xsize = this.info.getSamples();
			this.ysize = this.info.getLines();
		}

		System.out.println("number of bands: " + this.info.getBands());

		int xread = lineWidth - this.pos.xstart;
		if(!this.completeScene && xread > xsize) { xread = xsize; }

		int yread = this.info.getLines() - this.pos.ystart;
		if(!this.completeScene && yread > ysize) { yread = ysize; }

		//in case complete scene should be read, use the number of lines of all bands
		if(this.completeScene) {yread = yread * this.info.getBands();}

		record.update(this.info, this.pos.leftUpperCorner, this.pos.rightLowerCorner, xsize, ysize, this.pos.band, this.pos.pathRow, this.pos.aqcDate, pixelWidth, pixelHeight);
		short[] values = record.getS16Tile();
		if(values == null) {
			values = new short[xsize * ysize];
			record.setS16Tile(values);
		}
		short dataIgnoreValue = (short) info.getDataIgnoreValue();
		int data_size = info.getPixelSize();
		
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
			
			// Fill with empty columns:
			for(int x = xread; x < xsize; x++) {
				values[pos++] = dataIgnoreValue;
			}
		}
		// Fill missing rows with empty data:
		while(pos < values.length) {
			values[pos++] = dataIgnoreValue;
		}
		return record;
	}

}
