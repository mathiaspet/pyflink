package org.apache.flink.api.java.spatial;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
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

	public Coordinate getUpperLeftCoordinate() {
		String[] args = getStringArray("upperleftcornerlatlong");
		if(args == null || args.length != 2) {
			return null;
		}
		return parseCoordinate(args[0], args[1]);
	}

	public Coordinate getLowerRightCoordinate() {
		String[] args = getStringArray("lowerrightcornerlatlong");
		if(args == null || args.length != 2) {
			return null;
		}
		return parseCoordinate(args[0], args[1]);
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

	public int getNumBands() {
		return getInteger("bands");
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
}
