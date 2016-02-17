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
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
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
	
	public static enum InterleaveTypes {
		bsq,
		bil,
		bip
	}
	
	private DataTypes dataType;
	private int bands, lines, samples, headerOffset, byteOrder, dataIgnoreValue;
	private double pixelWidth, pixelHeight;
	private int interleave; 
	private Coordinate leftUpper, rightLower;
	
	private long acqDate;
//	private String mapInfoString; //TODO: maybe constructed
	private int dataTypeindex;
	

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
	

	public TileInfo(TileInfo tileInfo) {
		this.acqDate = tileInfo.acqDate;
		this.bands = tileInfo.bands;
		this.byteOrder = tileInfo.byteOrder;
		this.dataIgnoreValue = tileInfo.dataIgnoreValue;
		this.dataType = tileInfo.dataType;
		this.headerOffset = tileInfo.headerOffset;
		this.interleave = tileInfo.interleave;
		this.leftUpper = new Coordinate(tileInfo.getLeftUpper());
		this.lines = tileInfo.lines;
//		this.mapInfoString = tileInfo.mapInfoString;
		this.pixelHeight = tileInfo.pixelHeight;
		this.pixelWidth = tileInfo.pixelWidth;
		this.rightLower = new Coordinate(tileInfo.getLowerRightCoordinate());
		this.samples = tileInfo.samples;
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
				
				
				if(name.equals("bands")) {
					this.bands = Integer.parseInt(value);
					continue;
				}
				
				if(name.equals("samples")) {
					this.samples = Integer.parseInt(value);
					continue;
				}
				
				if(name.equals("lines")) {
					this.lines = Integer.parseInt(value);
					continue;
				}
				
				if(name.equals("data type")) {
					int parseInt = Integer.parseInt(value);
					
					if(parseInt == -1) {
						this.dataType = DataTypes.MISSING;
						continue;
					}
					this.dataType = DataTypes.values()[parseInt];
					this.dataTypeindex = parseInt;
					continue;
				}
				if(name.equals("header offset")) {
					this.headerOffset = Integer.parseInt(value);
					continue;
				}
				
				if(name.equals("interleave")) {
					//TODO: switch
					if(value.equals("bsq")) {
						this.interleave = 0;
					}
					
					if(value.equals("bil")) {
						this.interleave = 1;
					}
					
					if(value.equals("bip")) {
						this.interleave = 2;
					}
					
					continue;
				}
				
				if(name.equals("data ignore value")) {
					this.dataIgnoreValue = Integer.parseInt(value);
					continue;
				}
				
				if(name.trim().equals("map info")) {
					
					String list = value.substring(1, value.length() - 1).trim(); 
					String[] splitted = list.split(", *");
					
					String easting = splitted[3];
					String northing = splitted[4];
					this.leftUpper = new Coordinate(Double.parseDouble(easting), Double.parseDouble(northing));
					
					
					
					this.pixelWidth = Double.parseDouble(splitted[5]);
					this.pixelHeight = Double.parseDouble(splitted[6]);
					
					
					
					continue;
				}
				
				if(name.equals("acquisitiondate")) {
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS"); 
					
					try {
						Date date = df.parse(value);
						//TODO: find out how many seconds between two scenes on the same row
						//and normalize the time to minutes
						this.acqDate = date.getTime();  
					} catch (ParseException e) {
					}
				}
				
				//entries.put(name, value);
			} else if(braceImbalance < 0) {
				throw new RuntimeException(
						"Parse error (negative curly brance balance) at line "
								+ lineNo + ": " + line);
			}
		}
		
		getLowerRightCoordinate();
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


	/**
	 * Since map info does not have a lower right coordinate we calculate it 
	 * by adding the number of pixels times the width/height of a pixel to 
	 * get to easting/northing of the lower right corner.
	 * TODO: cache this value
	 *  
	 * @return a {@link Coordinate} yielding the geographical position of the lower right corner
	 */
	public Coordinate getLowerRightCoordinate() {
		
		if(this.rightLower == null) {
		Coordinate upperLeft = getUpperLeftCoordinate();
		int lines = getLines();
		int samples = getSamples();
		double pixelWidth = getPixelWidth();
		double pixelHeight = getPixelHeight();
		
		double lowerRightEasting = upperLeft.lon + (samples-1) * pixelWidth;
		double lowerRightNorthing = upperLeft.lat - (lines-1) * pixelHeight;
		
		this.rightLower = new Coordinate(lowerRightEasting, lowerRightNorthing);
		}
		
		return this.rightLower;
	}

	
	public DataTypes getDataType() {
		return this.dataType;
	}
	
	/**
	 * TODO: fix for different coordinate systems.
	 * @return
	 */
	public Coordinate getMapInfoUpperLeft() {
		return this.leftUpper;
//		String[] mapInfo = getStringArray("map info");
//		
//		String easting = mapInfo[3];
//		String northing = mapInfo[4];
//		
//		return new Coordinate(Double.parseDouble(easting), Double.parseDouble(northing));
	}
	
	/**
	 * Return the size of each pixel in bytes.
	 */
	public int getPixelSize() {
//		switch(getDataType()) {
		switch(this.dataType) {
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
		
		target.writeLong(this.acqDate);
		
		target.writeInt(this.bands);
		
		target.writeInt(this.byteOrder);
//		target.writeUTF(this.coordinateString);
//		writeString(target, coordinateString);
		
		target.writeInt(this.dataIgnoreValue);
		
		target.writeInt(this.dataTypeindex);
		
		target.writeInt(this.headerOffset);
		
		target.writeInt(this.interleave);
		
		this.leftUpper.serialize(target);
		
		target.writeInt(this.lines);
		
//		target.writeUTF(this.mapInfoString);
		target.writeDouble(this.pixelHeight);
		target.writeDouble(this.pixelWidth);
		
//		target.writeUTF(this.projectionInfoString);
		this.rightLower.serialize(target);
		target.writeInt(this.samples);
	}

	public void deserialize(DataInputView source) throws IOException {
		this.acqDate = source.readLong();
		
		this.bands = source.readInt();
		
		this.byteOrder = source.readInt();
		
		this.dataIgnoreValue = source.readInt();
		
		this.dataTypeindex = source.readInt();
		
		if(this.dataTypeindex >= 0) {
			this.dataType = DataTypes.values()[this.dataTypeindex];
		}else {
			System.out.println("DT index wrong: " + this.dataTypeindex);
		}
		this.headerOffset = source.readInt();
		
		this.interleave = source.readInt();
		
		this.leftUpper = new Coordinate();
		this.leftUpper.deserialize(source);
		
		this.lines = source.readInt();
		
		this.pixelHeight = source.readDouble();
		this.pixelWidth = source.readDouble();
		
		this.rightLower = new Coordinate();
		this.rightLower.deserialize(source);
		
		this.samples = source.readInt();
	}

	public int getBands() {
		return bands;
	}

	public int getLines() {
		return lines;
	}

	public int getSamples() {
		return samples;
	}

	public int getHeaderOffset() {
		return headerOffset;
	}

	public int getByteOrder() {
		return byteOrder;
	}

	public int getInterleave() {
		return interleave;
	}

	public Coordinate getLeftUpper() {
		return leftUpper;
	}

	public long getAcqDate() {
		return acqDate;
	}

	public void setPixelWidth(double pixelWidth) {
		this.pixelWidth = pixelWidth;
	}

//	public String getCoordinateString() {
//		return coordinateString;
//	}

//	public String getProjectionInfoString() {
//		return projectionInfoString;
//	}

//	public String getMapInfoString() {
//		return mapInfoString;
//	}

	public int getDataIgnoreValue() {
		return dataIgnoreValue;
	}
	
	public double getPixelWidth() {
		return this.pixelWidth;
	}

	public double getPixelHeight() {
		return this.pixelHeight;
	}

	/**
	 * Parse the header date given by this.info and compute the 
	 * upper left coordinate given by map info.
	 * @return a {@link Coordinate} yielding the geographical position of the upper left corner
	 */
	public Coordinate getUpperLeftCoordinate() {
		return this.leftUpper;
	}

	public int getDataTypeindex() {
		return dataTypeindex;
	}

}
