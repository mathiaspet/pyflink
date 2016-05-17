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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.python.api.streaming.io.PythonSplitProcessorStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class PythonInputSplitProcessor<T> implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(PythonInputFormat.class);
	private final RichInputFormat format;
	private final int id;

	private Configuration configuration;

	private PythonCollector<T> collector;
	private PythonSplitProcessorStreamer streamer;

	public PythonInputSplitProcessor(RichInputFormat format, int id, boolean asByteArray) {
		this.format = format;
		this.id = id;
		this.streamer = new PythonSplitProcessorStreamer(format, id, asByteArray);
	}

	public void configure(Configuration parameters) {
		this.configuration = parameters;
	}

	public void open() throws IOException {
		this.collector = new PythonCollector<>();
		this.streamer.open();
		this.streamer.sendBroadCastVariables(this.configuration);
	}

	public void openSplit(FileInputSplit split) throws IOException {
		this.streamer.transmitSplit(split);
	}

	public void closeSplit() {
	}

	public void close() {
		try {
			this.streamer.close();
		} catch (IOException ex) {
			LOG.error("error closing python IF: " + format.getRuntimeContext().getTaskName() + ": " + ex.getMessage());
		}
	}

	public boolean reachedEnd() throws IOException {
		if (this.collector.isEmpty()) {
			return !this.streamer.receiveResults(this.collector);
		} else {
			return this.collector.isEmpty();
		}
	}

	public T nextRecord(T record) throws IOException {
		return this.collector.poll();
	}
}
