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
 * Common super class to all the spatial objects which represent (partial) satellite images. 
 * Such images have an extent and content, which is defined by an array of pixels 
 * with certain dimensions. 
 *
 */
public abstract class SpatialObject  implements Serializable {

	protected String pathRow;
	protected String aqcuisitionDate;
	/**
	 * Tile data in 16-bit signed integers. It is organized in rows of pixels.
	 */
	protected short[] s16Tile = null;
	protected Coordinate luCord = null;
	protected Coordinate rlCord = null;
	protected double xPixelWidth = -1.0;
	protected double yPixelWidth = -1.0;
	protected int tileWidth = -1;
	protected int tileHeight = -1;
	protected TileInfo tileInfo = null;

	public SpatialObject() {
		super();
	}

	public SpatialObject(SpatialObject spatialObject) {
		this.aqcuisitionDate = spatialObject.getAqcuisitionDate();
		this.luCord = spatialObject.luCord.copy();
		this.pathRow = spatialObject.getPathRow();
		this.rlCord = spatialObject.rlCord.copy();

		short[] content = spatialObject.getS16Tile();
		short[] newContent = new short[content.length];
		System.arraycopy(content, 0, newContent, 0, content.length);
		this.s16Tile = newContent;

		this.tileHeight = spatialObject.getTileHeight();
		this.tileInfo = spatialObject.getTileInfo().copy();
		this.tileWidth = spatialObject.getTileWidth();
		this.xPixelWidth = spatialObject.xPixelWidth;
		this.yPixelWidth = spatialObject.yPixelWidth;
		
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
	 * Return the header associated with this stream, if present. Otherwise,
	 * null is returned.
	 */
	public TileInfo getTileInfo() {
		return this.tileInfo;
	}

	public String getPathRow() {
		return pathRow;
	}

	public void setPathRow(String pathRow) {
		this.pathRow = pathRow;
	}

	public void setAqcuisitionDate(String aqcuisitionDate) {
		this.aqcuisitionDate = aqcuisitionDate;
	}

	public String getAqcuisitionDate() {
		return aqcuisitionDate;
	}

	public void setTileInfo(TileInfo tileInfo) {
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
	
	public void serialize(DataOutputView target) throws IOException {
		if (this.aqcuisitionDate != null) {
			target.writeBoolean(true);
			target.writeUTF(this.aqcuisitionDate);
		} else {
			target.writeBoolean(false);
		}
		
		this.luCord.serialize(target);
		this.rlCord.serialize(target);
		
		if (this.pathRow != null) {
			target.writeBoolean(true);
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
			this.aqcuisitionDate = source.readUTF();
		}
		
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
		
		this.tileInfo = new TileInfo();
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

	public void copyTo(SpatialObject target) {
		target.aqcuisitionDate = this.aqcuisitionDate;
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
}
