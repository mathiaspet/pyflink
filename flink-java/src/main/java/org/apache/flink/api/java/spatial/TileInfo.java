package org.apache.flink.api.java.spatial;

import java.io.Serializable;
import java.util.HashMap;

public class TileInfo implements Serializable {
	private static final long serialVersionUID = 5579375867489556640L;

	private HashMap<String, String> entries = new HashMap<String, String>();
	
	public TileInfo() {	}
	
	public TileInfo(String tileHeader) {
		String name = null;
		String value = null;
		int braceImbalance = 0;
		int lineNo = 0;
		for(String line: tileHeader.split("\r?\n")) {
			lineNo++;
			// Parse header:
			if(lineNo == 1) {
				if(line.equals("ENVI"))
					continue;
				else
					throw new RuntimeException("Expected ENVI header, found: " + line);
			}
			
			// Continue the previous line:
			if(braceImbalance > 0) {
				braceImbalance -= countOccurences(line, '}');
				braceImbalance += countOccurences(line, '{');
				value = value + " " + line;
			} else if(line.indexOf('=') == -1) {
				// Ignore comments:
				if(line.trim().startsWith(";"))
					continue;
				else
					throw new RuntimeException("Line " + lineNo + " contains no '=': " + line);
			} else {
				// Or parse a new line:
				String entry[] = line.split("=", 2);
				name = entry[0].trim();
				value = entry[1].trim();
				
				braceImbalance += countOccurences(value, '{');
				braceImbalance -= countOccurences(value, '}');
			}
			if (braceImbalance == 0)
				entries.put(name, value);
			else if (braceImbalance < 0)
				throw new RuntimeException("Parse error (negative curly brance balance) at line " + lineNo + ": " + line);
		}
	}
	
	/**
	 * Return how many times c appears in str.
	 */
	private int countOccurences(String str, char c) {
		int occurences = 0;
		int offset = 0;
		while((offset = str.indexOf(c, offset) + 1) > 0)
			occurences++;
		return occurences;
	}
	
	public String getString(String name) {
		return this.entries.get(name);
	}
	
	public int getInteger(String name) {
		String entry = this.entries.get(name);
		if(entry == null) return -1;
		return Integer.parseInt(entry);
	}
	
	public long getLong(String name) {
		String entry = this.entries.get(name);
		if(entry == null) return -1;
		return Long.parseLong(entry);
	}
	
	public double getDouble(String name) {
		String entry = this.entries.get(name);
		if(entry == null) return Double.NaN;
		return Double.parseDouble(entry);
	}
	
	public Coordinate getCoordinate(String latName, String lonName) {
		double lat = getDouble(latName);
		double lon = getDouble(lonName);
		if(lat == Double.NaN || lon == Double.NaN) return null;
		return new Coordinate(lat, lon);
	}
}
