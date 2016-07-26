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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class TileInfoWrapper implements Serializable{
	private Map<String, String> metaData;

	public TileInfoWrapper() {
		this.metaData = new HashMap<String, String>();
	}

	public TileInfoWrapper(InputStream is) throws IOException {
		this.metaData = parseHeader(readHeader(is));
	}

	public TileInfoWrapper(byte[] buf) {
		this.metaData = fromBytes(buf);
	}

	public TileInfoWrapper(TileInfoWrapper other) {
		this.metaData = other.metaData;
	}

	public TileInfoWrapper copy() {
		return new TileInfoWrapper(this);
	}

	private String readHeader(InputStream is) throws IOException {
		// Read header
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
		return new String(buf, 0, offset, Charset.forName("UTF-8"));
	}

	private Map<String, String> parseHeader(String header) {
		// Parse header:
		HashMap<String, String> metaData = new HashMap<String, String>();
		String name = null;
		String value = null;
		int braceImbalance = 0;
		int lineNo = 0;

		for(String line: header.split("\r?\n")) {
			lineNo++;
			if(lineNo == 1) {
				if(line.equals("ENVI")) {
					continue;
				} else {
					throw new RuntimeException("Expected ENVI header, found: " + StringUtils.showControlCharacters(line));
				}
			}

			// Skip empty lines:
			if(line.length() == 0 || (line.length() < 2 && (line.equals("\n") || line.equals("\r\n")))) {
				continue;
			}

			// Continue the previous line:
			if(braceImbalance > 0) {
				braceImbalance -= count('}', line);
				braceImbalance += count('{', line);
				value = value + " " + line;
			} else if(line.indexOf('=') == -1) {
				// Ignore comments:
				if(line.trim().startsWith(";")) {
					continue;
				} else {
					throw new RuntimeException("Error parsing header file: expected '=' at  " + lineNo + ": " + StringUtils.showControlCharacters(line));
				}
			} else {
				// Or parse a new line:
				String[] entry = line.split("=", 2);
				name = entry[0].trim();
				value = entry[1].trim();

				braceImbalance -= count('}', value);
				braceImbalance += count('{', value);
			}
			if(braceImbalance == 0) {
				metaData.put(name, value);
			} else if(braceImbalance < 0) {
				throw new RuntimeException("Error parsing header file: unbalanced braces at " + lineNo + ": " + StringUtils.showControlCharacters(line));
			}
		}

		return metaData;
	}

	private static int count(char c, String s) {
		int n = 0;
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == c) {
				n++;
			}
		}
		return n;
	}

	public short getDataIgnoreValue() {
		return Short.parseShort(this.metaData.get("data ignore value"));
	}

	public int getDataType() {
		return Integer.parseInt(this.metaData.get("data type"));
	}

	public String getInterleave() {
		return this.metaData.get("interleave");
	}

	public int getPixelSize() {
		int dataType = getDataType();
		switch(dataType) {
			case 1: // BYTE
				return 1;
			case 2: // INT
			case 3: // UINT
				return 2;
			case 4: // LONG
			case 5: // ULONG
				return 4;
			case 6: // LONG64
			case 7: // ULONG64
				return 8;
			default:
				throw new RuntimeException("Unsupported data format: " + dataType);
		}
	}

	public String getSceneID(){
		String[] description = getList("description");
		String[] sceneInfo = description[0].trim().split("\\s*:\\s*");
		String sceneID = sceneInfo[1];
		return sceneID;}

	public String getOverlap() {return this.metaData.get("overlap");}

	public void setOverlap(String overlappingTile){
		this.metaData.put("overlap", overlappingTile);
	}

	public int getBands() {
		return Integer.parseInt(this.metaData.get("bands"));
	}

	public void setBands(int bands) {
		this.metaData.put("bands", Integer.toString(bands));
	}

	public String [] getBandNames() {
		return getList("band names");
	}

	public void setBandNames(String [] bandNames) {
		putList("band names", bandNames);
	}

	public int getLines() {
		return Integer.parseInt(this.metaData.get("lines"));
	}

	public void setLines(int lines) {
		this.metaData.put("lines", Integer.toString(lines));
	}

	public int getSamples() {
		return Integer.parseInt(this.metaData.get("samples"));
	}

	public void setSamples(int samples) {
		this.metaData.put("samples", Integer.toString(samples));
	}

	public void setBand(int band) {this.metaData.put("band", Integer.toString(band));}

	public int getBand() {return Integer.parseInt(this.metaData.get("band"));}

	private String[] getMapInfo() {
		/* map info fields:
		 * Projection name
		 * Reference (tie point) pixel x location (in file coordinates)
		 * Reference (tie point) pixel y location (in file coordinates)
		 * Pixel easting
		 * Pixel northing
		 * x pixel size
		 * y pixel size
		 * Projection zone (UTM only)
		 * North or South (UTM only)
		 * Datum
		 * Units
		 */
		return getList("map info");
	}

	public String[] getList(String key) {
		try {
			String values = this.metaData.get(key);
			return values.substring(values.indexOf('{') + 1, values.indexOf('}')).trim().split(" *, *");
		} catch(IndexOutOfBoundsException e){
			throw new RuntimeException("Badly formatted list at " + key + ".");
		}
	}

	private void putList(String key, String[] entries) {
		StringBuilder list = new StringBuilder("{");
		list.append(entries[0]);
		for (int i = 1; i < entries.length; i++) {
			list.append(", ");
			list.append(entries[i]);
		}
		list.append("}");
		this.metaData.put(key, list.toString());
	}

	public double getPixelWidth() {
		String[] mapInfo = getMapInfo();
		return Double.parseDouble(mapInfo[5]);
	}

	public void setPixelWidth(double pixelWidth) {
		String[] mapInfo = getMapInfo();
		mapInfo[5] = Double.toString(pixelWidth);
		putList("map info", mapInfo);
	}

	public double getPixelHeight() {
		String[] mapInfo = getMapInfo();
		return Double.parseDouble(mapInfo[6]);
	}

	public void setPixelHeight(double pixelHeight) {
		String[] mapInfo = getMapInfo();
		mapInfo[6] = Double.toString(pixelHeight);
		putList("map info", mapInfo);
	}

	public Coordinate getLeftUpper() {
		String[] mapInfo = getMapInfo();
		return new Coordinate(Double.parseDouble(mapInfo[3]), Double.parseDouble(mapInfo[4]));
	}
	
	public void setLeftUpper(Coordinate leftUpper) {
		String[] mapInfo = getMapInfo();
		mapInfo[3] = Double.toString(leftUpper.lon);
		mapInfo[4] = Double.toString(leftUpper.lat);
		putList("map info", mapInfo);
	}

	public Coordinate getLowerRightCoordinate() {
		Coordinate upperLeft = getLeftUpper();
		int lines = getLines();
		int samples = getSamples();
		double pixelWidth = getPixelWidth();
		double pixelHeight = getPixelHeight();

		double lowerRightEasting = upperLeft.lon + (samples - 1) * pixelWidth;
		double lowerRightNorthing = upperLeft.lat - (lines - 1) * pixelHeight;

		return new Coordinate(lowerRightEasting, lowerRightNorthing);
	}

	public byte[] toBytes() {
		StringBuilder strBuf = new StringBuilder();
		for (Entry<String, String> e: this.metaData.entrySet()) {
			strBuf.append(e.getKey() + '\0' + e.getValue() + '\0');
		}
		strBuf.append('\0');
		return strBuf.toString().getBytes(Charset.forName("UTF-8"));
	}

	private Map<String, String> fromBytes(byte[] raw) {
		HashMap<String, String> metaData = new HashMap<String, String>();
		String asString = new String(raw, Charset.forName("UTF-8"));

		int offset = 0;
		int nextBreak = -1;
		String key;
		String value;
		//ArrayList<String> allKeys = new ArrayList<>();
		while ((nextBreak = asString.indexOf('\0', offset)) != -1) {
			key = asString.substring(offset, nextBreak);
			//allKeys.add(key);
			offset = nextBreak + 1;
			nextBreak = asString.indexOf('\0', offset);
			if (nextBreak == -1) {
				break;
				//throw new RuntimeException("Missing value for key: " + key);
			}
			value = asString.substring(offset, nextBreak);
			metaData.put(key, value);
			offset = nextBreak + 1;
		}
		return metaData;
	}

	public void serialize(DataOutputView target) throws IOException {
		target.writeInt(this.metaData.size());
		for (Entry<String, String> e: this.metaData.entrySet()) {
			target.writeUTF(e.getKey());
			target.writeUTF(e.getValue());
		}
	}

	public void deserialize(DataInputView source) throws IOException {
		int entries = source.readInt();
		while (entries > 0) {
			this.metaData.put(source.readUTF(), source.readUTF());
			entries--;
		}
	}

	private static String leftPad(String s, int l, char p) {
		StringBuilder filled = new StringBuilder(l);
		for (int i = 0; i < l - s.length(); i++) {
			filled.append(p);
		}
		filled.append(s);
		return filled.toString();
	}

	public String getPathRow() {
		String path = this.metaData.get("wrs_path");
		String row = this.metaData.get("wrs_row");
		return leftPad(path, 3, '0') + leftPad(row, 3, '0');
	}

	public void setPathRow(String pathRow) {
		this.metaData.put("wrs_path", pathRow.substring(0, 3));
		this.metaData.put("wrs_path", pathRow.substring(3, 6));
	}

	public String getAcquisitionDate() {
		return this.metaData.get("acquisitiondate");
	}

	public void setAcquisitionDate(String acqDate) {
		this.metaData.put("acquisitiondate", acqDate);
	}

	public long getAcquisitionDateAsLong() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS"); 
		try {
			Date date = df.parse(this.metaData.get("acquisitiondate"));
			return date.getTime();  
		}
		catch (ParseException e) {
			return -1;
		}
	}

	public int getHeaderOffset() {
		return Integer.parseInt(this.metaData.get("header offset"));
	}


}
