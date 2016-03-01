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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Spatial tile This class represents a spatial tile. TODO: describe me & make
 * the internal representation more efficient now the fields are accessed
 * deserialized, this is very inefficient
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 *
 */
public class Tile implements Serializable {
	private static final long serialVersionUID = 3999969290376342375L;

	private int band = -1;

	/**
	 * Tile data in 16-bit signed integers. It is organized in rows of pixels.
	 */
	private short[] s16Tile = null;

	// x- and y-width of a pixel
	private double xPixelWidth = -1.0, yPixelWidth = -1.0;

	private TileInfoWrapper tileInfo = null;

	// TODO: decide whether to keep this public or not
	public Tile() {
	}

	public Tile(Coordinate leftUpper, Coordinate rightLower, short[] content,
			int width, int height) {
		this.s16Tile = content;
	}

	public Tile(Tile tile) {
		this.band = tile.getBand();

		short[] content = tile.getS16Tile();
		short[] newContent = new short[content.length];
		System.arraycopy(content, 0, newContent, 0, content.length);
		this.s16Tile = newContent;

		this.tileInfo = tile.getTileInfo().copy();

		this.xPixelWidth = tile.xPixelWidth;
		this.yPixelWidth = tile.yPixelWidth;
	}

	public int getBand() {
		return this.band;
	}

	/**
	 * Returns the tile contents as 1-dimensional array of pixels organized in
	 * rows. The pixels are addressed by converting a coordinate (x, y) starting
	 * at (0, 0) to the array offset x + (y * this.getTileWidth()).
	 * 
	 * @return
	 */
	public short[] getS16Tile() {
		return this.s16Tile;
	}

	/**
	 * Update the stored tile array to the given one.
	 */
	public void setS16Tile(short[] data) {
		this.s16Tile = data;
	}

	/**
	 * Convenience function to retrieve a single pixel by pixel coordinates.
	 * Coordinates start at (0, 0).
	 */
	public short getPixel(int width, int height) {
		return this.s16Tile[width + (height * getTileWidth())];
	}

	/**
	 * Returns the width of the tile in pixels, i.e. the number of pixels
	 * representing one row in the tile array.
	 */
	public int getTileWidth() {
		return this.tileInfo.getSamples();
	}

	/**
	 * Returns the height of the tile in pixels, i.e. the number of rows in the
	 * tile array.
	 */
	public int getTileHeight() {
		return this.tileInfo.getLines();
	}

	/**
	 * Return the coordinate of the north-west boundary point of this tile.
	 */
	public Coordinate getNWCoord() {
		return this.tileInfo.getLeftUpper();
	}

	/**
	 * Return the coordinate of the south-east boundary point of this tile.
	 */
	public Coordinate getSECoord() {
		return this.tileInfo.getLowerRightCoordinate();
	}

	/**
	 * Return the header associated with this stream, if present. Otherwise,
	 * null is returned.
	 */
	public TileInfoWrapper getTileInfo() {
		return this.tileInfo;
	}

	/**
	 * Update the tile information to the given object.
	 */
	public void update(TileInfoWrapper tileInfo, Coordinate leftUpper,
			Coordinate rightLower, int width, int height, int band,
			String pathRow, String acqDate, double xPixelWidth, 
			double yPixelWidth) {
		this.tileInfo = tileInfo;
		this.tileInfo.setLeftUpper(leftUpper);

		this.tileInfo.setSamples(width);
		this.tileInfo.setLines(height);
		this.tileInfo.setPathRow(pathRow);
		this.tileInfo.setAcquisitionDate(acqDate);

		this.band = band;
		this.xPixelWidth = xPixelWidth;
		this.yPixelWidth = yPixelWidth;
	}

	public Long getAcquisitionDateAsLong() {
		return this.tileInfo.getAcquisitionDateAsLong();
	}

	public String getPathRow() {
		return this.tileInfo.getPathRow();
	}

	public void setPathRow(String pathRow) {
		this.tileInfo.setPathRow(pathRow);
	}

	public void setAcquisitionDate(String acquisitionDate) {
		this.tileInfo.setAcquisitionDate(acquisitionDate);
	}

	public String getAcquisitionDate() {
		return this.tileInfo.getAcquisitionDate();
	}

	/**
	 * Given the number of samples, the pixel dimensions and the upper left
	 * reference point we calculate the geographical coordinate;
	 * 
	 * @param contentIndex
	 * @return
	 */
	public Coordinate getCoordinate(int contentIndex) {
		int x = contentIndex % getTileWidth();
		int y = contentIndex / getTileWidth();
		double newLon = getLuCord().lon + this.xPixelWidth * x;
		double newLat = getLuCord().lat - this.yPixelWidth * y;

		return new Coordinate(newLon, newLat);
	}

	/**
	 * Given the left upper reference point, the tile and pixel dimensions we
	 * calculate the actual position in the s16Tile array.
	 * 
	 * @param coord
	 * @return
	 */
	public int getContentIndexFromCoordinate(Coordinate coord) {
		int latDiff = (int) (getLuCord().lat - coord.lat);
		int lonDiff = (int) (coord.lon - getLuCord().lon);

		// check if coord is fully contained in this tile
		if (latDiff < 0 || lonDiff < 0) {
			return -1;
		}

		int x = (int) (lonDiff / this.xPixelWidth);
		int y = (int) (latDiff / this.yPixelWidth);

		return y * getTileWidth() + x;
	}

	public Tile createCopy() {
		return new Tile(this);
	}

	public void copyTo(Tile target) {
		target.band = this.band;

		short[] content = this.s16Tile;
		short[] newContent = new short[content.length];
		System.arraycopy(content, 0, newContent, 0, content.length);
		target.s16Tile = newContent;

		target.tileInfo = this.getTileInfo().copy();
		target.xPixelWidth = this.xPixelWidth;
		target.yPixelWidth = this.yPixelWidth;

	}

	public void serialize(DataOutputView target) throws IOException {
		target.writeInt(this.band);
		

		target.writeDouble(this.xPixelWidth);
		target.writeDouble(this.yPixelWidth);
		
		this.tileInfo.serialize(target);
		
		if(this.s16Tile != null && this.s16Tile.length > 0) {
			target.writeBoolean(true);
			
			byte[] byteContent = new byte[s16Tile.length * 2];
	
			ByteBuffer.wrap(byteContent).order(ByteOrder.LITTLE_ENDIAN)
					.asShortBuffer().put(s16Tile);
			target.writeInt(byteContent.length);
			target.write(byteContent);
		} else{
			target.writeBoolean(false);
		}
	
	}

	public void deserialize(DataInputView source) throws IOException {
		this.band = source.readInt();
		
		
		this.xPixelWidth = source.readDouble();
		this.yPixelWidth = source.readDouble();
		
		this.tileInfo = new TileInfoWrapper();
		this.tileInfo.deserialize(source);
		
		if(source.readBoolean()) {
			int contentLength = source.readInt();
			byte[] content = new byte[contentLength];
			source.read(content);
			this.s16Tile = new short[content.length / 2];
			ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
					.get(s16Tile);
		} else {
			this.s16Tile = new short[0];
		}
	}

	
	public void setTileInfo(TileInfoWrapper tileInfo) {
		this.tileInfo = tileInfo;
	}

	public void setLuCord(Coordinate luCord) {
		this.tileInfo.setLeftUpper(luCord);
	}

	public Coordinate getLuCord() {
		return this.tileInfo.getLeftUpper();
	}

	public Coordinate getRlCord() {
		return this.tileInfo.getLowerRightCoordinate();
	}

	public void setBand(int band) {
		this.band = band;
	}

	public void setTileHeight(int tileHeight) {
		this.tileInfo.setLines(tileHeight);
	}

	public void setTileWidth(int tileWidth) {
		this.tileInfo.setSamples(tileWidth);
	}

	public void setxPixelWidth(Double xPixelWidth) {
		this.xPixelWidth = xPixelWidth;
	}

	public void setyPixelWidth(Double yPixelWidth) {
		this.yPixelWidth = yPixelWidth;
	}

	public double getxPixelWidth() {
		return this.xPixelWidth;
	}

	public double getyPixelWidth() {
		return this.yPixelWidth;
	}
}
