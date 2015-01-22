package org.apache.flink.api.java.spatial;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TileTest {

	private TileInfo info;
	private Tile out;
	private Tile out2;
	
	@Before
	public void setUp() {
		this.info = new TileInfo();
		
		out = new Tile();
		Coordinate leftUpper = new Coordinate(430404.0572, 3120036.4653);
		Coordinate rightLower = new Coordinate(430404.0572 + 8002*30.0, 3120036.4653 - 7232 * 30.0);
		out.setLuCord(leftUpper);
		out.update(info, leftUpper, rightLower, 8002, 7232, 1, "227064", "000202", 30.0, 30.0);
		
		this.out2 = new Tile();
		Coordinate leftUpper2 = new Coordinate(430404.0572 + 3500*30.0, 3120036.4653 - 3500 * 30.0);
		Coordinate rightLower2 = new Coordinate(430404.0572 + 4500*30.0, 3120036.4653 - 4500 * 30.0);
		out2.setLuCord(leftUpper);
		out2.update(info, leftUpper2, rightLower2, 1000, 1000, 1, "227064", "000202", 30.0, 30.0);
		
	}
	
	
	@Test
	public void testCoordinate() {
		
		Coordinate coordinate = this.out.getCoordinate(8000);
		assertThat(coordinate.lat, is(equalTo(this.out.getLuCord().lat)));
		assertThat(coordinate.lon, is(equalTo(this.out.getLuCord().lon + 8000 * 30.0)));
		
		Coordinate secondCoord = this.out.getCoordinate(10000);
		assertThat(secondCoord.lat, is(equalTo(this.out.getLuCord().lat - 30.0)));
		assertThat(secondCoord.lon, is(equalTo(this.out.getLuCord().lon + (10000-8002) * 30.0)));
		
		Coordinate thirdCoord = this.out.getCoordinate(10001);
		assertThat(thirdCoord.lat, is(equalTo(this.out.getLuCord().lat - 30.0)));
		assertThat(thirdCoord.lon, is(equalTo(this.out.getLuCord().lon + (10001-8002) * 30.0)));
		int thirdIndex = this.out.getContentIndexFromCoordinate(thirdCoord);
		assertThat(thirdIndex, is(equalTo(10001)));
		
		Coordinate fourthCoord = new Coordinate(thirdCoord.lon + 30.0, thirdCoord.lat);
		int fourthIndex = this.out.getContentIndexFromCoordinate(fourthCoord);
		assertThat(fourthIndex, is(equalTo(10002)));
	}
	
	@Test
	public void testCoordinateFromDifferentTiles() {
		int contentIndex = 4000 * 8002 + 4000;
		Coordinate insideCoord = this.out.getCoordinate(contentIndex);
		
		int contentIndexFromCoordinate = this.out2.getContentIndexFromCoordinate(insideCoord);
		int line = contentIndexFromCoordinate / this.out2.getTileWidth();
		int column = contentIndexFromCoordinate % this.out2.getTileWidth();
		assertThat(line, is(equalTo(500)));
		assertThat(column, is(equalTo(500)));
		
		Coordinate rightNext = this.out.getCoordinate(contentIndex+1);
		int rightNextIndex = this.out2.getContentIndexFromCoordinate(rightNext);
		assertThat(rightNextIndex, is(equalTo(contentIndexFromCoordinate + 1)));
		
		int line2 = rightNextIndex / this.out2.getTileWidth();
		int column2 = rightNextIndex % this.out2.getTileWidth();
		assertThat(line2, is(equalTo(500)));
		assertThat(column2, is(equalTo(501)));
	}

}
