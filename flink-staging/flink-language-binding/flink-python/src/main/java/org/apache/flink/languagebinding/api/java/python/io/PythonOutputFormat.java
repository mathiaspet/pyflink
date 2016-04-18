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

package org.apache.flink.languagebinding.api.java.python.io;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PythonOutputFormat extends FileOutputFormat{

	private PythonStreamer streamer;
	private boolean streamerOpen;
	private int taskNumber, numTasks;
	private Collector<Integer> collector;

	public PythonOutputFormat(Path outputPath, int id) {
		super();
		super.setOutputFilePath(outputPath);
		this.streamer = new PythonStreamer(this, id);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.taskNumber = taskNumber;
		this.numTasks = numTasks;
		if (!this.streamerOpen) {
			this.streamer.open();
			Configuration config = new Configuration();
			//TODO: add params
			this.streamer.sendBroadCastVariables(config);
			this.streamerOpen = true;
		}

	}

	@Override
	public void writeRecord(Object record) throws IOException {
		//TODO: implement buffer logic to send a chunk of records at once instead of sending each record at a time
		//TODO: this is ugly to make it work; create a proper method for the streamer
		List<Object> input = new ArrayList<Object>(1);
		input.add(record);
		this.collector = new PythonCollector<Integer>();
		this.streamer.streamBufferWithoutGroups(input.iterator(), collector);
	}

	@Override
	public void close() throws IOException {
		this.streamer.sendCloseMessage("close");
		this.streamer.close();

		super.close();
	}
}
