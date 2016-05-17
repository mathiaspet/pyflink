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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Spatial tile This class represents a spatial tile. TODO: describe me & make
 * the internal representation more efficient now the fields are accessed
 * deserialized, this is very inefficient
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 *
 */
public class TileWrapper implements Serializable {
	private static final long serialVersionUID = 3999969290376342375L;

	private String pathRow;
	private String acquisitionDate;

	private int band = -1;

	/**
	 * Tile data in 16-bit signed integers. It is organized in rows of pixels.
	 */
	private short[] s16Tile = null;

	// Coordinates of left upper and right lower edge (according to the map
	// info)
	private Coordinate luCord = null, rlCord = null;

	// x- and y-width of a pixel
	private double xPixelWidth = -1.0, yPixelWidth = -1.0;

	private TileInfoWrapper tileInfo = null;

	// Tile width and height in pixels
	private int tileWidth = -1, tileHeight = -1;

	// TODO: decide whether to keep this public or not
	public TileWrapper() {
	}

	public TileWrapper(Coordinate leftUpper, Coordinate rightLower, short[] content, int width, int height) {
		this.luCord = leftUpper;
		this.rlCord = rightLower;
		this.s16Tile = content;
		this.tileWidth = width;
		this.tileHeight = height;
	}

	public TileWrapper(Tuple3<String, byte[], byte[]> content) {

		this.s16Tile = new short[content.f2.length];
		ByteBuffer.wrap(content.f2).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(this.s16Tile);

		TileInfoWrapper tiWrapper = new TileInfoWrapper(content.f1);
		this.tileInfo = tiWrapper;
		this.acquisitionDate = tiWrapper.getAcquisitionDate();
		//this.band = tiWrapper.get
		this.luCord = tiWrapper.getLeftUpper();
		this.rlCord = tiWrapper.getLowerRightCoordinate();
		this.pathRow = tiWrapper.getPathRow();

		this.tileHeight = tiWrapper.getLines();
		this.tileWidth = tiWrapper.getSamples();
		this.xPixelWidth = tiWrapper.getPixelWidth();
		this.yPixelWidth = tiWrapper.getPixelHeight();
	}

	public TileWrapper(TileWrapper tile) {
		this.acquisitionDate = tile.getAcquisitionDate();
		this.band = tile.getBand();
		this.luCord = tile.getNWCoord().copy();
		this.pathRow = tile.getPathRow();
		this.rlCord = tile.getSECoord().copy();

		short[] content = tile.getS16Tile();
		short[] newContent = new short[content.length];
		System.arraycopy(content, 0, newContent, 0, content.length);
		this.s16Tile = newContent;

		this.tileHeight = tile.getTileHeight();
		this.tileInfo = tile.getTileInfo().copy();
		this.tileWidth = tile.getTileWidth();
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
		return this.s16Tile[width + (height * this.tileWidth)];
	}

	/**
	 * Returns the width of the tile in pixels, i.e. the number of pixels
	 * representing one row in the tile array.
	 */
	public int getTileWidth() {
		return this.tileWidth;
	}

	/**
	 * Returns the height of the tile in pixels, i.e. the number of rows in the
	 * tile array.
	 */
	public int getTileHeight() {
		return this.tileHeight;
	}

	/**
	 * Return the coordinate of the north-west boundary point of this tile.
	 */
	public Coordinate getNWCoord() {
		return this.luCord;
	}

	/**
	 * Return the coordinate of the south-east boundary point of this tile.
	 */
	public Coordinate getSECoord() {
		return this.rlCord;
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
	public void update(TileInfoWrapper tileInfo, Coordinate leftUpper, Coordinate rightLower, int width, int height, int band, String pathRow, String acqDate, double xPixelWidth, double yPixelWidth) {
		this.tileInfo = tileInfo;
		this.luCord = leftUpper;
		this.rlCord = rightLower;
		this.tileWidth = width;
		this.tileHeight = height;
		this.band = band;
		this.pathRow = pathRow;
		this.acquisitionDate = acqDate;
		this.xPixelWidth = xPixelWidth;
		this.yPixelWidth = yPixelWidth;
	}

	public Long getAcquisitionDateAsLong() {
		if (this.tileInfo == null) {
			return new Long(-1);
		} else {
			return this.tileInfo.getAcquisitionDateAsLong();
		}
	}

	public String getPathRow() {
		return pathRow;
	}

	public void setPathRow(String pathRow) {
		this.pathRow = pathRow;
	}

	public void setAcquisitionDate(String acquisitionDate) {
		this.acquisitionDate = acquisitionDate;
	}

	public String getAcquisitionDate() {
		return acquisitionDate;
	}

	/**
	 * Given the number of samples, the pixel dimensions and the upper left
	 * reference point we calculate the geographical coordinate;
	 * 
	 * @param contentIndex
	 * @return
	 */
	public Coordinate getCoordinate(int contentIndex) {
		int x = contentIndex % tileWidth;
		int y = (int) (contentIndex / tileWidth);
		double newLon = this.luCord.lon + this.xPixelWidth * x;
		double newLat = this.luCord.lat - this.yPixelWidth * y;

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
		int latDiff = (int) (this.luCord.lat - coord.lat);
		int lonDiff = (int) (coord.lon - this.luCord.lon);

		// check if coord is fully contained in this tile
		if (latDiff < 0 || lonDiff < 0) {
			return -1;
		}

		int x = (int) (lonDiff / this.xPixelWidth);
		int y = (int) (latDiff / this.yPixelWidth);

		return y * this.tileWidth + x;
	}

	public TileWrapper createCopy() {
		return new TileWrapper(this);
	}

	public void copyTo(TileWrapper target) {
		target.acquisitionDate = this.acquisitionDate;
		target.band = this.band;
		target.luCord = this.luCord.copy();
		target.pathRow = this.pathRow;
		target.rlCord = this.rlCord.copy();

		short[] content = this.s16Tile;
		short[] newContent = new short[content.length];
		System.arraycopy(content, 0, newContent, 0, content.length);
		target.s16Tile = newContent;

		target.tileHeight = this.tileHeight;
		target.tileInfo = this.getTileInfo().copy();
		target.tileWidth = this.tileWidth;
		target.xPixelWidth = this.xPixelWidth;
		target.yPixelWidth = this.yPixelWidth;

	}

	public void serialize(DataOutputView target) throws IOException {
		if (this.acquisitionDate != null) {
			target.writeBoolean(true);
//			writeString(target, acquisitionDate);
			target.writeUTF(this.acquisitionDate);
		} else {
			target.writeBoolean(false);
		}
		
		target.writeInt(this.band);
		
		this.luCord.serialize(target);
		this.rlCord.serialize(target);
		
		if (this.pathRow != null) {
			target.writeBoolean(true);
//			writeString(target, pathRow);
			target.writeUTF(this.pathRow);
		} else {
			target.writeBoolean(false);
		}
		

		target.writeInt(this.tileHeight);
		target.writeInt(this.tileWidth);
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
		if (source.readBoolean()) {
			this.acquisitionDate = source.readUTF();
		}
		
		this.band = source.readInt();

		this.luCord = new Coordinate();
		this.luCord.deserialize(source);
		
		this.rlCord = new Coordinate();
		this.rlCord.deserialize(source);

		
		if (source.readBoolean()) {
			this.pathRow = source.readUTF();
		}
		
		
		this.tileHeight = source.readInt();
		this.tileWidth = source.readInt();
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
		this.luCord = luCord;
	}

	public void setRlCord(Coordinate rlCord) {
		this.rlCord = rlCord;
	}

	public Coordinate getLuCord() {
		return luCord;
	}

	public Coordinate getRlCord() {
		return rlCord;
	}

	public void setBand(int band) {
		this.band = band;
	}

	public void setTileHeight(int tileHeight) {
		this.tileHeight = tileHeight;
	}

	public void setTileWidth(int tileWidth) {
		this.tileWidth = tileWidth;
	}

	public void setxPixelWidth(Double xPixelWidth) {
		this.xPixelWidth = xPixelWidth;
	}

	public void setyPixelWidth(Double yPixelWidth) {
		this.yPixelWidth = yPixelWidth;
	}

	public double getxPixelWidth() {	return xPixelWidth;}

	public double getyPixelWidth() {return yPixelWidth;	}

	public Tuple3<String, byte[], byte[]> toTuple() {
		byte[] content = new byte[this.s16Tile.length * 2];
		ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN)
			.asShortBuffer().put(s16Tile);
		return new Tuple3<String, byte[], byte[]>(this.acquisitionDate, this.tileInfo.toBytes(), content);
	}
}
