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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class EnviOutputFormat extends FileOutputFormat<Tile> {

	private static final long serialVersionUID = 1L;
	List<String> bands = new ArrayList<String>();
	private int height = -1, width = -1;
	private String mapInfoString;
	private String projectionInfo = null;
	private String coordinateSystemString = null;

	public EnviOutputFormat(Path filePath) {
		super(filePath);
	}

	@Override
	public void writeRecord(Tile tile) throws IOException {
		this.height = tile.getTileHeight();
		this.width = tile.getTileWidth();
		String pathRow = tile.getPathRow();
		int band = tile.getBand();
		if (this.projectionInfo == null) {
			this.projectionInfo = "projection info = "
					+ "projection info = {9, 6378160.0, 6356774.7, -32.000000, -60.000000, 0.0, 0.0, -5.000000, -42.000000, South American 1969 mean, South_America_Albers_Equal_Area_Conic, units=Meters}";
		}

		if (this.coordinateSystemString == null) {
			this.coordinateSystemString = "coordinate system string = "
					+ "{PROJCS[\"South_America_Albers_Equal_Area_Conic\",GEOGCS[\"GCS_South_American_1969\",DATUM[\"D_South_American_1969\",SPHEROID[\"GRS_1967_Truncated\",6378160.0,298.25]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Albers\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"central_meridian\",-60.0],PARAMETER[\"Standard_Parallel_1\",-5.0],PARAMETER[\"Standard_Parallel_2\",-42.0],PARAMETER[\"latitude_of_origin\",-32.0],UNIT[\"Meter\",1.0]]}";
		}

		if (this.mapInfoString == null) {
			String[] split = "{South_America_Albers_Equal_Area_Conic, 1.0000, 1.0000, 430404.0572, 3120036.4653, 3.000000e+001, 3.000000e+001, South American 1969 mean, units=Meters}"
					.split(",");
			this.mapInfoString = "map info = " + split[0] + "," + split[1]
					+ "," + split[2] + "," + tile.getNWCoord().lon + ", "
					+ tile.getNWCoord().lat + ", " + split[4] + ", " + split[5]
					+ ", " + split[6] + ", " + split[7] + ", " + split[8];
		}
		String aqcuisitionDate = tile.getAqcuisitionDate();
		this.bands.add(pathRow + "_" + aqcuisitionDate + "_B" + band);

		short[] s16Tile = tile.getS16Tile();
		byte[] byteContent = new byte[s16Tile.length * 2];

		ByteBuffer.wrap(byteContent).order(ByteOrder.LITTLE_ENDIAN)
				.asShortBuffer().put(s16Tile);
		this.stream.write(byteContent);
		this.stream.flush();

	}

	/**
	 * TODO: generate header file and write that on disk after the content of a
	 * tile has bin written.
	 */
	@Override
	public void close() throws IOException {
		super.close();

		Path p = this.outputFilePath;
		if (p == null) {
			throw new IOException("The file path is null.");
		}

		final FileSystem fs = p.getFileSystem();

		Path headerPath = new Path(this.outputFilePath.toUri() + ".hdr");
		FSDataOutputStream headerStream = fs.create(headerPath, true);

		// BufferedWriter writer = new BufferedWriter();
		//
		// writer.append("ENVI\n");
		// writer.append("samples = " + this.width + "\n");
		// writer.append("lines = " + this.height + "\n");
		// writer.append("bands = " + this.bands.size() + "\n");
		// // TODO: make this multi type capable
		// writer.append("data type = 2\n");
		// writer.append("interleave = bsq\n");
		// writer.append("file type = ENVI Standard\n");
		// writer.append("header offset = 0\n");
		// writer.append("byte order = 0\n");
		// writer.append("map info = " + this.mapInfoString + "\n");
		// writer.append(this.projectionInfo + "\n");
		// writer.append(this.coordinateSystemString + "\n");
		// writer.append("band names = {\n");
		// int index = 0;
		// for (String b : bands) {
		// writer.append(b);
		// if (index++ < this.bands.size()) {
		// writer.append(", ");
		// }
		// if (index % 4 == 0) {
		// writer.append("\n");
		// }
		// }
		//
		// writer.append("}");

		headerStream.write("ENVI\n".getBytes());
		headerStream.write(("samples = " + this.width + "\n").getBytes());
		headerStream.write(("lines = " + this.height + "\n").getBytes());
		headerStream.write(("bands = " + this.bands.size() + "\n").getBytes());
		// TODO: make this multi type capable
		headerStream.write(("data type = 2\n").getBytes());
		headerStream.write(("interleave = bsq\n").getBytes());
		headerStream.write(("file type = ENVI Standard\n").getBytes());
		headerStream.write(("header offset = 0\n").getBytes());
		headerStream.write(("byte order = 0\n").getBytes());
		headerStream.write(("map info = " + this.mapInfoString + "\n")
				.getBytes());
		headerStream.write((this.projectionInfo + "\n").getBytes());
		headerStream.write((this.coordinateSystemString + "\n").getBytes());
		headerStream.write(("band names = {\n").getBytes());
		int index = 0;
		for (String b : bands) {
			headerStream.write((b).getBytes());
			if (index++ < this.bands.size()) {
				headerStream.write((", ").getBytes());
			}
			if (index % 4 == 0) {
				headerStream.write(("\n").getBytes());
			}
		}

		headerStream.write(("}").getBytes());
		headerStream.flush();
		headerStream.close();

		// writer.flush();
		// writer.close();
	}

}
