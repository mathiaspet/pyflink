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
package org.apache.flink.api.java.spatial.envi;

import java.io.IOException;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.spatial.ImageInfoWrapper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class ImageOutputFormat<T extends Tuple3<String, byte[], byte[]>> extends FileOutputFormat<T> {
	private static final long serialVersionUID = 1L;

	private byte[] header;

	public ImageOutputFormat() {
		super();
	}

	public ImageOutputFormat(Path filePath) {
		super(filePath);
	}

	@Override
	public void writeRecord(T record) throws IOException {
		this.header = record.f1;
		this.stream.write(record.f2);
		this.stream.flush();
	}

	@Override
	public void close() throws IOException {
		super.close();

		Path p = this.outputFilePath;
		if (p == null) {
			throw new IOException("The file path is null.");
		}

		final FileSystem fs = p.getFileSystem();

		Path headerPath = new Path(this.outputFilePath.toUri() + ".hdr");
		// fs.mkdirs
		FSDataOutputStream headerStream = fs.create(headerPath, true);

		ImageInfoWrapper info = new ImageInfoWrapper(header);

		headerStream.write("ENVI\n".getBytes());
		headerStream.write(info.toString().getBytes());

		headerStream.flush();
		headerStream.close();
	}
}
