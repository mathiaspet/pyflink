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
package org.apache.flink.python.api;


import org.apache.flink.runtime.io.network.api.reader.BufferReader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by mathiasp on 07.11.16.
 */
public class MrcCatFixLIne {

	public static void main(String[] args) throws IOException {
		int count = 1000000;

		System.out.println("MRC Catalog Export Fix.");

		if(args == null || args.length != 2) {
			System.out.println("Invalid arguments. Filenames (input & output) needed");
			return;
		}

		File inFile = new File(args[0]);
		File outFile = new File(args[1]);
		BufferedReader reader = new BufferedReader(new FileReader(inFile));

		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		String line;
		String lastLine = null;
		int i = 0;
		while ((line = reader.readLine()) != null) {
			if(lastLine != null && lastLine.contains("dir") && lastLine.contains("name=\"\""))
			{
				System.out.println(lastLine);
				System.out.println(line);
				int begin = line.indexOf("name=") + 6;
				int end = line.indexOf(".", begin);
				if(begin >= 0 && end >= 0) {
					String name = line.substring(begin, end);
					System.out.println(name);

					int lastLineBegin = lastLine.indexOf("name=") + 6;
					lastLine = lastLine.substring(0, lastLineBegin) + name + lastLine.substring(lastLineBegin);
				}
			}
			if(lastLine != null) {
				writer.write(lastLine + "\n");
			}
			if(i % 100000 == 0)
				writer.flush();

			lastLine = line;
			i++;
			//if(i == count)
			//	return;

		}

		writer.write(lastLine);
		writer.flush();
		writer.close();

		System.out.println("done. Leaving...");
	}
}
