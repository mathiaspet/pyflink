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
package org.apache.flink.examples.java.spatial;

import org.apache.flink.api.common.functions.FilterFunction;
// import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
// import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.envi.ImageInputFormat;
import org.apache.flink.api.java.spatial.envi.ImageOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
// import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;


public class ImageTest {
	private static int dop;
	private static String filePath;
	private static String outputFilePath;

	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(dop);

		DataSet<Tuple3<String, byte[], byte[]>> images = readImages(env);
		DataSet<Tuple3<String, byte[], byte[]>> filtered = images.filter(new FilterFunction<Tuple3<String, byte[], byte[]>>() {
			int count = 0;
			@Override
			public boolean filter(Tuple3<String, byte[], byte[]> value) throws Exception {
				count++;
				System.out.println("counted: " + count);
				return true;
			}
		});
		filtered.write(new ImageOutputFormat(), outputFilePath);
		env.execute("Image Test");
	}

	private static boolean parseParameters(String[] params) {
		if (params.length > 0) {
			if (params.length != 3) {
				System.out.println("Usage: <dop> <input directory> <output path>");
				return false;
			} else {
				dop = Integer.parseInt(params[0]);
				filePath = params[1];
				outputFilePath = params[2];
			}
		} else {
			System.out.println("Usage: <dop> <input directory> <output path>");
			return false;
		}

		return true;
	}

	private static DataSet<Tuple3<String, byte[], byte[]>> readImages(ExecutionEnvironment env) {
		ImageInputFormat imageFormat = new ImageInputFormat(new Path(filePath));
		// TupleTypeInfo<Tuple3<String, Byte[], Byte[]>> typeInfo = new TupleTypeInfo<Tuple3<String, Byte[], Byte[]>>(BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO, BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO);
		TupleTypeInfo<Tuple3<String, byte[], byte[]>> typeInfo = new TupleTypeInfo<Tuple3<String, byte[], byte[]>>(BasicTypeInfo.STRING_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		return new DataSource<Tuple3<String, byte[], byte[]>>(env, imageFormat, typeInfo, "imageSource");
	}

}
