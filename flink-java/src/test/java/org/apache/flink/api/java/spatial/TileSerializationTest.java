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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TileSerializationTest {

	private Tile out;
	
	private static final String testHeader =
			"ENVI\n" +
			"description = {\n" +
			" Scene id: LE72270642000033AGS00, \n" +
			"	HDF file: M:\\PreprocessedLandsatData\\227_064\\LE72270642000033AGS00\\lndsr.LE72270642000033AGS00.hdf}\n" +
			"samples = 4\r\n" +
			"; this is a valid comment\n" +
			"lines   = 4\r\n" +
			"bands   = 6\n" +
			"  ; this is an indented comment\n" +
			"data type = 2\n" +
			"interleave = bsq\r\n" +
			"file type = ENVI Standard\n" +
			"header offset = 0\n" +
			"byte order = 0\n" +
			"map info = {South_America_Albers_Equal_Area_Conic, 1.0000, 1.0000, 430404.0572, 3120036.4653, 3.000000e+001, 3.000000e+001, South American 1969 mean, units=Meters}\n" +
			"projection info = {9, 6378160.0, 6356774.7, -32.000000, -60.000000, 0.0, 0.0, -5.000000, -42.000000, South American 1969 mean, South_America_Albers_Equal_Area_Conic, units=Meters}\n" +
			"coordinate system string = {PROJCS[\"South_America_Albers_Equal_Area_Conic\",GEOGCS[\"GCS_South_American_1969\",DATUM[\"D_South_American_1969\",SPHEROID[\"GRS_1967_Truncated\",6378160.0,298.25]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Albers\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"central_meridian\",-60.0],PARAMETER[\"Standard_Parallel_1\",-5.0],PARAMETER[\"Standard_Parallel_2\",-42.0],PARAMETER[\"latitude_of_origin\",-32.0],UNIT[\"Meter\",1.0]]}\n" +
			"default bands = {4,5,3}\n" +
			"band names = {\n" +
			"band1, band2, band3, band4, band5, band7}\n" +
			"wavelength = {\n" +
			"0.483000, 0.560000, 0.662000, 0.835000, 1.648000, 2.206000}\n" +
			"wavelength units = micrometers\n" +
			"data ignore value = -9999\n" +
			"sensor = Landsat ETM\n" +
			"dataprovider = USGS/EROS\n" +
			"satellite = LANDSAT_7\n" +
			"instrument = ETM\n" +
			"acquisitiondate = 2000-02-02T13:41:47.870870Z\n" +
			"level1productiondate = 2014-01-16T00:00:00.000000Z\n" +
			"solarzenith = 34.136707\n" +
			"solarazimuth = 111.804741\n" +
			"wrs_system = 2\n" +
			"wrs_path = 227\n" +
			"wrs_row = 64\n" +
			"reflgains = {\r\n" +
			"0.778740, 0.798819, 0.621653, 0.969291, 0.126220, 0.043898}\n" +
			"reflbias = {\n" +
			"  -6.978740, -7.198819, -5.621654, -6.069291, -1.126220, -0.393898}\r\n" +
			"thermalgain = 0.067087\n" +
			"thermalbias = -0.067087\n" +
			"shortname = L7ESR\n" +
			"localgranuleid = L7ESR.a2000033.w2p227r064.020.2014057162928.hdf\n" +
			"productiondate = 2014-02-26T16:29:28Z\n" +
			"ledapsversion = 1.3.1\n" +
			"lpgsmetadatafile = LE72270642000033AGS00_MTL.txt\n" +
			"upperleftcornerlatlong = {\n" +
			"-4.835949, -56.076531}\n" +
			"lowerrightcornerlatlong = {\n" +
			"-6.721377, -53.949345}\n" +
			"westboundingcoordinate = -56.076667\n" +
			"eastboundingcoordinate = -53.949208\n" +
			"northboundingcoordinate = -4.829621\n" +
			"southboundingcoordinate = -6.730149\n" +
			"orientationangle = 0.000000\n" +
			"pixelsize = 30.000000\n" +
			"hdfversion = 4.2r4\n" +
			"hdfeosversion = 4.2\n" +
			"scene id = LE72270642000033AGS00\n";
		
	
	@Before
	public void setUp() {
		out = new Tile();
		
		Coordinate lu = new Coordinate(1.0, 1.0);
		out.setLuCord(lu);
		out.setAcquisitionDate("000202");
		out.setPathRow("227064");
		
		TileInfoWrapper info = new TileInfoWrapper(testHeader.getBytes());
		out.setTileInfo(info);
		
		short[] content = {1, 2, 3, 4, 5 ,6 ,7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
		out.setS16Tile(content);
		
	}
	
	@Test
	public void test() throws IOException {
		MockDataOutputView mock = new MockDataOutputView();
		
		this.out.serialize(mock);
		
		
		Tile tile = new Tile();
		tile.deserialize(mock);
		
		assertThat(this.out.getAcquisitionDate(), is(equalTo(tile.getAcquisitionDate())));
		assertThat(this.out.getPathRow(), is(equalTo(tile.getPathRow())));
		
		TileInfoWrapper outInfo = this.out.getTileInfo();
		TileInfoWrapper tileInfo = tile.getTileInfo();
		
		assertThat(outInfo.getAcquisitionDate(), is(equalTo(tileInfo.getAcquisitionDate())));
		assertThat(outInfo.getBands(), is(equalTo(tileInfo.getBands())));
		assertThat(outInfo.getDataIgnoreValue(), is(equalTo(tileInfo.getDataIgnoreValue())));
		assertThat(outInfo.getDataType(), is(equalTo(tileInfo.getDataType())));
		assertThat(outInfo.getHeaderOffset(), is(equalTo(tileInfo.getHeaderOffset())));
		assertThat(outInfo.getInterleave(), is(equalTo(tileInfo.getInterleave())));
		assertThat(outInfo.getLines(), is(equalTo(tileInfo.getLines())));
		assertThat(outInfo.getPixelSize(), is(equalTo(tileInfo.getPixelSize())));
		assertThat(outInfo.getSamples(), is(equalTo(tileInfo.getSamples())));
		
		short[] s16Tile = this.out.getS16Tile();
		short[] s16Tile2 = tile.getS16Tile();
		assertThat(s16Tile.length, is(equalTo(s16Tile2.length)));
		for(int i = 0; i < 16; i++) {
			assertThat(s16Tile[i], is(equalTo(s16Tile2[i])));
		}
	}


	private static class MockDataOutputView implements DataOutputView, DataInputView {

		private List<Integer> intList = new ArrayList<Integer>();
		private int i = 0;
		private List<String> stringList = new ArrayList<String>();
		private int s = 0;
		
		private List<Boolean> boolList = new ArrayList<Boolean>();
		private int b =0;
		
		private List<Long> longList = new ArrayList<Long>();
		private int l = 0;
		
		private List<byte[]> byteList = new ArrayList<byte[]>();
		private int by = 0;
		
		@Override
		public void write(int b) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(byte[] b) throws IOException {
			this.byteList.add(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			this.boolList.add(new Boolean(v));
		}

		@Override
		public void writeByte(int v) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeShort(int v) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeChar(int v) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeInt(int v) throws IOException {
			this.intList.add(new Integer(v));
		}

		@Override
		public void writeLong(long v) throws IOException {
			this.longList.add(new Long(v));
		}

		@Override
		public void writeFloat(float v) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeDouble(double v) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeBytes(String s) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeChars(String s) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeUTF(String s) throws IOException {
			this.stringList.add(s);
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(DataInputView source, int numBytes)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int skipBytes(int n) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean readBoolean() throws IOException {
			return this.boolList.get(b++).booleanValue();
		}

		@Override
		public byte readByte() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int readUnsignedByte() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public short readShort() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int readUnsignedShort() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public char readChar() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int readInt() throws IOException {
			return this.intList.get(i++).intValue();
		}

		@Override
		public long readLong() throws IOException {
			return this.longList.get(l).longValue();
		}

		@Override
		public float readFloat() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public double readDouble() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String readLine() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String readUTF() throws IOException {
			return this.stringList.get(s++);
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int read(byte[] b) throws IOException {
			byte[] readByte = this.byteList.get(by++);
			System.arraycopy(readByte, 0, b, 0, readByte.length);
			return readByte.length;
		}
		
	}
}
