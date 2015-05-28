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

package org.apache.flink.api.java.spatial;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.flink.util.StringUtils;

public class TileInfo extends HashMap<String, String> {
	private static final long serialVersionUID = 5579375867489556640L;
	
	public static enum DataTypes {
		UNDEFINED,
		BYTE,  // uint8
		INT,   //  int16
		UINT,  // uint16
		LONG,  //  int32
		ULONG, // uint32
		LONG64,
		ULONG64,
		FLOAT,
		DOUBLE,
		COMPLEX,
		DCOMPLEX,
		STRING,
		STRUCT,
		POINTER,
		OBJREF,
		LIST,
		HASH,
		DICTIONARY,
		ORDEREDHASH,
		MISSING // special value, always the last one
	}
	
	public static enum InterleaveTypes {
		bsq,
		bil,
		bip
	}

	public TileInfo() {
		super();
	}

	public TileInfo(TileInfo other) {
		super(other);
	}

	public TileInfo(InputStream is) throws IOException {
		byte[] buf = new byte[4096];
		int offset = 0;
		int readBytes;
		while((readBytes = is.read(buf, offset, buf.length - offset)) > 0) {
			offset += readBytes;
			if(offset == buf.length) {
				byte[] buf2 = new byte[2 * buf.length];
				System.arraycopy(buf, 0, buf2, 0, buf.length);
				buf = buf2;
			}
		}
		parseInfo(new String(buf, 0, offset, Charset.forName("UTF-8")));
	}

	public TileInfo(String tileHeader) {
		parseInfo(tileHeader);
	}
	
	private final void parseInfo(String tileHeader) {
		String name = null;
		String value = null;
		int braceImbalance = 0;
		int lineNo = 0;

		for(String line: tileHeader.split("\r?\n")) {
			lineNo++;
			// Parse header:
			if(lineNo == 1) {
				if(line.equals("ENVI")) {
					continue;
				} else {
					throw new RuntimeException("Expected ENVI header, found: "
							+ line);
				}
			}

			// Skip empty lines:
			if(line.length() == 0 || (line.length() < 2 && (line.equals("\n") || line.equals("\r\n")))) {
				continue;
			}
			
			// Continue the previous line:
			if(braceImbalance > 0) {
				braceImbalance -= countOccurences(line, '}');
				braceImbalance += countOccurences(line, '{');
				value = value + " " + line;
			} else if(line.indexOf('=') == -1) {
				// Ignore comments:
				if(line.trim().startsWith(";")) {
					continue;
				} else {
					throw new RuntimeException("Line " + lineNo
							+ " contains no '=': " + StringUtils.showControlCharacters(line));
				}
			} else {
				// Or parse a new line:
				String[] entry = line.split("=", 2);
				name = entry[0].trim();
				value = entry[1].trim();

				braceImbalance += countOccurences(value, '{');
				braceImbalance -= countOccurences(value, '}');
			}
			if(braceImbalance == 0) {
				this.put(name, value);
			} else if(braceImbalance < 0) {
				throw new RuntimeException(
						"Parse error (negative curly brance balance) at line "
								+ lineNo + ": " + line);
			}
		}
	}


	/**
	 * Return how many times c appears in str.
	 */
	private int countOccurences(String str, char c) {
		int occurences = 0;
		int offset = 0;
		while((offset = str.indexOf(c, offset) + 1) > 0) {
			occurences++;
		}
		return occurences;
	}

	
	public DataTypes getDataType() {
		if (this.containsKey("data type")) {
			int parseInt = Integer.parseInt(this.get("data type"));

			if(parseInt == -1) {
				return DataTypes.MISSING;
			}
			return DataTypes.values()[parseInt];
		}

		return null;
	}
	
	
	/**
	 * Return the size of each pixel in bytes.
	 */
	public int getPixelSize() {
		switch(getDataType()) {
			case BYTE:
				return 1;
			case INT:
			case UINT:
				return 2;
			case LONG:
			case ULONG:
				return 4;
			case LONG64:
			case ULONG64:
				return 8;
			default:
				throw new RuntimeException("Unsupported data format: " + getDataType());
		}
	}


	public TileInfo copy() {
		return new TileInfo(this);
	}

	public void serialize(DataOutputView target) throws IOException {
		target.writeInt(this.size());
		for (Map.Entry<String, String> entry: this.entrySet()) {
			target.writeUTF(entry.getKey());
			target.writeUTF(entry.getValue());
		}
	}

	public void deserialize(DataInputView source) throws IOException {
		String key;
		String value;
		for (int i = source.readInt(); i > 0; i--) {
			key = source.readUTF();
			value = source.readUTF();
			this.put(key, value);
		}

	}

	public int getBands() {
		return Integer.parseInt(this.get("bands"));
	}

	public int getLines() {
		return Integer.parseInt(this.get("lines"));
	}

	public int getSamples() {
		return Integer.parseInt(this.get("samples"));
	}

	public int getHeaderOffset() {
		return Integer.parseInt(this.get("header offset"));
	}

	public int getInterleave() {
		if (this.containsKey("interleave")) {
			if (this.get("Interleave") == "bsq") {
				return 0;
			}
			else if (this.get("Interleave") == "bil") {
				return 1;
			}
			else if (this.get("Interleave") == "bip") {
				return 2;
			}
		}
		return -1;
	}


	public long getAcqDate() {
		if (this.containsKey("acquisitiondate")) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS"); 
			try {
				Date date = df.parse(this.get("acquisitiondate"));
				return date.getTime();  
			} catch (ParseException e) {
			}
		}
		return -1;
	}
	
	public String getAcqDateAsString() {
		if (this.containsKey("acquisitiondate")) {
			return this.get("acquisitiondate");
		}
		return null;
	}
	
	public int getDataIgnoreValue() {
		if (this.containsKey("data ignore value")) {
			return Integer.parseInt(this.get("data ignore value"));
		}
		return -1;
	}
	
	/**
	 * Parse the header date given by this.info and compute the 
	 * upper left coordinate given by map info.
	 * @return a {@link Coordinate} yielding the geographical position of the upper left corner
	 */
	public Coordinate getUpperLeftCoordinate() {
		if (this.containsKey("map info")) {
			String list = this.get("map info").substring(1, this.get("map info").length() - 1).trim(); 
			String[] splitted = list.split(", *");

			String easting = splitted[3];
			String northing = splitted[4];
			return new Coordinate(Double.parseDouble(easting), Double.parseDouble(northing));
		}
		return null;
	}

	/**
	 * Since map info does not have a lower right coordinate we calculate it 
	 * by adding the number of pixels times the width/height of a pixel to 
	 * get to easting/northing of the lower right corner.
	 * TODO: cache this value
	 *  
	 * @return a {@link Coordinate} yielding the geographical position of the lower right corner
	 */
	public Coordinate getLowerRightCoordinate() {
		Coordinate upperLeft = getUpperLeftCoordinate();
		int lines = getLines();
		int samples = getSamples();
		double pixelWidth = getPixelWidth();
		double pixelHeight = getPixelHeight();

		double lowerRightEasting = upperLeft.lon + (samples-1) * pixelWidth;
		double lowerRightNorthing = upperLeft.lat - (lines-1) * pixelHeight;

		return new Coordinate(lowerRightEasting, lowerRightNorthing);
	}


	public double getPixelWidth() {
		if (this.containsKey("map info")) {
			String list = this.get("map info").substring(1, this.get("map info").length() - 1).trim(); 
			String[] splitted = list.split(", *");

			return Double.parseDouble(splitted[5]);
		}
		return -1.0;
	}

	public double getPixelHeight() {
		if (this.containsKey("map info")) {
			String list = this.get("map info").substring(1, this.get("map info").length() - 1).trim(); 
			String[] splitted = list.split(", *");

			return Double.parseDouble(splitted[5]);
		}
		return -1.0;
	}
}
