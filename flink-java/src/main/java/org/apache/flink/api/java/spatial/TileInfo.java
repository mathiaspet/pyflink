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
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.flink.util.StringUtils;

public class TileInfo implements Serializable {
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

	private HashMap<String, String> entries = new HashMap<String, String>();

	public TileInfo() {}

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
				entries.put(name, value);
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

	public String getString(String name) {
		return this.entries.get(name);
	}

	public int getInteger(String name) {
		String entry = this.entries.get(name);
		if(entry == null) {
			return -1;
		}
		return Integer.parseInt(entry);
	}

	public long getLong(String name) {
		String entry = this.entries.get(name);
		if(entry == null) {
			return -1;
		}
		return Long.parseLong(entry);
	}

	public double getDouble(String name) {
		String entry = this.entries.get(name);
		if(entry == null) {
			return Double.NaN;
		}
		return Double.parseDouble(entry);
	}

	public Coordinate getCoordinate(String latName, String lonName) {
		double lat = getDouble(latName);
		double lon = getDouble(lonName);
		if(lat == Double.NaN || lon == Double.NaN) {
			return null;
		}
		return new Coordinate(lat, lon);
	}

	private Coordinate parseCoordinate(String latStr, String lonStr) {
		double lat = Double.parseDouble(latStr);
		double lon = Double.parseDouble(lonStr);
		if(lat == Double.NaN || lon == Double.NaN) {
			return null;
		}
		return new Coordinate(lat, lon);
	}

	/**
	 * Parse the header date given by this.info and compute the 
	 * upper left coordinate given by map info.
	 * @return a {@link Coordinate} yielding the geographical position of the upper left corner
	 */
	public Coordinate getUpperLeftCoordinate() {
		String[] args = getStringArray("map info");
		//TODO: find out what the minimum number of map info elements is
		if(args == null || args.length != 9) {
			return null;
		}
		return parseCoordinate(args[3], args[4]);
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
		int lines = getInteger("lines");
		int samples = getInteger("samples");
		double pixelWidth = getPixelWidth();
		double pixelHeight = getPixelHeight();
		
		double lowerRightEasting = upperLeft.lon + (samples-1) * pixelWidth;
		double lowerRightNorthing = upperLeft.lat + (lines-1) * pixelHeight;
		
		return new Coordinate(lowerRightEasting, lowerRightNorthing);
	}

	public String[] getStringArray(String name) {
		String string = getString(name);
		if(string == null || !string.startsWith("{") || !string.endsWith("}")) {
			return null;
		}
		String list = string.substring(1, string.length() - 1).trim(); // Strip { }
		return list.split(", *");
	}
	
	public int getPixelColumns() {
		return getInteger("samples");
	}
	
	public int getPixelRows() {
		return getInteger("lines");
	}
	
	public String getInterleaveType() {
		return getString("interleave");
	}
	
	public DataTypes getDataType() {
		int typeVal = getInteger("data type");
		if(typeVal == -1) {return DataTypes.MISSING;}
		return DataTypes.values()[typeVal];
	}
	
	public int getMissingValue() {
		return getInteger("data ignore value");
	}

	public Long getAqcuisitionDate() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS"); 
		
		String acqDateString = getString("acquisitiondate");
		try {
			Date date = df.parse(acqDateString);
			return date.getTime();
		} catch (ParseException e) {
		}
		
		return new Long(-1);
	}
	
	public int getNumBands() {
		return getInteger("bands");
	}
	
	/**
	 * TODO: fix for different coordinate systems.
	 * @return
	 */
	public Coordinate getMapInfoUpperLeft() {
		String[] mapInfo = getStringArray("map info");
		
		String northing = mapInfo[3];
		String easting = mapInfo[4];
		
		
		return new Coordinate(Double.parseDouble(northing), Double.parseDouble(easting));
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

	public double getPixelWidth() {
		String witdh = getStringArray("map info")[5];
		
		return Double.parseDouble(witdh);
	}

	public double getPixelHeight() {
		String height = getStringArray("map info")[6];
		return Double.parseDouble(height);
	}	
}
