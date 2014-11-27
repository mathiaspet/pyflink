package org.apache.flink.api.java.spatial;

import java.io.Serializable;

/**
 * Spatial tile
 * This class represents a spatial tile.
 * TODO: describe me
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 *
 */
public class Tile implements Serializable {
	private static final long serialVersionUID = 3999969290376342375L;
	
	private int band = -1;
	
	/**
	 *  Tile data in 16-bit signed integers.
	 *  It is organized in rows of pixels. 
	 */
	private short[] s16Tile = null;
	
	// Coordinates of north-east and south-west corner of this tile
	private Coordinate nwCord = null, seCord = null;

	private TileInfo tileInfo = null;
	
	// Tile width and height in pixels
	private int tileWidth = -1, tileHeight = -1;
	
	//TODO: decide whether to keep this public or not
	public Tile() { }
	
	public Tile(Coordinate leftUpper, Coordinate rightLower, short[] content, int width, int height) {
		this.nwCord = leftUpper;
		this.seCord = rightLower;
		this.s16Tile = content;
		this.tileWidth = width;
		this.tileHeight = height;
	}
	
	
	public int getBand() {
		return this.band;
	}
	
	/**
	 * Returns the tile contents as 1-dimensional array of pixels organised in rows.
	 * The pixels are addressed by converting a coordinate (x, y) starting at (0, 0)
	 * to the array offset x  + (y * this.getTileWidth()). 
	 * @return
	 */
	public short[] getS16Tile() {
		return this.s16Tile;
	}
	
	/**
	 * Convenience function to retrieve a single pixel by pixel coordinates.
	 * Coordinates start at (0, 0).
	 */
	public short getPixel(int width, int height) {
		return this.s16Tile[width + (height * this.tileWidth)];
	}
	
	/**
	 * Returns the width of the tile in pixels,
	 * i.e. the number of pixels representing one row in the tile array.
	 */
	public int getTileWidth() {
		return this.tileWidth;
	}

	/**
	 * Returns the height of the tile in pixels,
	 * i.e. the number of rows in the tile array.
	 */
	public int getTileHeight() {
		return this.tileHeight;
	}

	/**
	 * Return the coordinate of the north-west boundary point of this tile.
	 */
	public Coordinate getNWCoord() {
		return this.nwCord;
	}

	/**
	 * Return the coordinate of the south-east boundary point of this tile.
	 */
	public Coordinate getSECoord() {
		return this.seCord;
	}
	
	/**
	 * Return the header associated with this stream, if present.
	 * Otherwise, null is returned.
	 */
	public TileInfo getTileInfo() {
		return this.tileInfo;
	}
}
