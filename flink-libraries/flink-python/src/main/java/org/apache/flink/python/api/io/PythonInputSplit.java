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
package org.apache.flink.python.api.io;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

public class PythonInputSplit extends FileInputSplit implements Serializable{

	private byte[] additional;

	/**
	 * Constructs a split with host information and a non-serialized string of additional
	 * information used by the python side of the input format.
	 *
	 * @param num    the number of this input split
	 * @param file   the file name
	 * @param start  the position of the first byte in the file to process
	 * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
	 * @param hosts
	 */
	public PythonInputSplit(int num, Path file, long start, long length, String[] hosts, byte[] additional) {
		super(num, file, start, length, hosts);
		this.additional = additional;
	}

	public byte[] getAdditional() {
		return additional;
	}
}
