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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PythonInputFormat<T extends Tuple> extends FileInputFormat<T> implements ResultTypeQueryable<T> {

	//exclusively needed for executing split logic in python
	private final PythonStreamer splitStreamer;
	private PythonStreamer streamer;
	private boolean streamerOpen;
	private boolean readForSplit;
	static final Logger LOG = LoggerFactory.getLogger(PythonInputFormat.class);
	private TypeInformation<T> typeInformation;
	private Path path;
	private Configuration configuration;
	private PythonCollector<T> collector;
	protected int readRecords = 0;
	private String filter;
	private boolean splitsInPython;

	public void setTypeInformation(TypeInformation<T> typeInformation) {
		this.typeInformation = typeInformation;
	}

	public PythonInputFormat(Path path, int id, TypeInformation<T> info, String filter, boolean computeSplitsInPython) {
		super(path);
		this.path = path;
		this.filter = filter;
		this.streamer = new PythonStreamer(this, id);
		this.splitStreamer = new PythonStreamer(this, id, true);
		this.typeInformation = info;
		this.collector  = new PythonCollector<T>();
		this.splitsInPython = computeSplitsInPython;
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if(this.splitsInPython) {
			//do fancy stuff in python here
			//TODO: make sure runtime context is not used
			//TODO: make special methods to execute this part on the JobManager
			this.splitStreamer.open();
			//TODO send computation request here
			//TODO: refactor sendCloseMessage to send Message
			this.splitStreamer.sendMessage("compute_splits");
			return null;
		}else {
			FileInputSplit[] inputSplits = super.createInputSplits(minNumSplits);
			System.out.println("break");
			return inputSplits;
		}
	}

	@Override
	protected boolean acceptFile(FileStatus fileStatus) {
		String name = fileStatus.getPath().getName();
		return name.matches(this.filter) && super.acceptFile(fileStatus);
	}

	@Override
	public T nextRecord(T record) throws IOException {
		if(!readForSplit) {
			readEnviTile();
			this.readForSplit = true;
		}

		if(!this.reachedEnd()) {
			record = this.collector.poll();
			this.readRecords++;
		}else {
			record = null;
		}
		return record;
	}
	
	@Override
	public void configure(Configuration parameters) {
		// TODO Auto-generated method stub
		super.configure(parameters);
		this.configuration = parameters;
	}

	private void readEnviTile() throws IOException {
		List<String> pathList = new ArrayList<String>();
		pathList.add(this.path.toString());
		this.streamer.streamBufferWithoutGroups(pathList.iterator(), collector);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.typeInformation;
	}
	
	@Override
	public void open(FileInputSplit split) throws IOException {
		// TODO Auto-generated method stub
		super.open(split);
		this.path = split.getPath();
		this.readForSplit = false;
		this.collector.clear();
		if (!this.streamerOpen) {
			this.streamer.open();
			this.streamer.sendMessage("open_task");
			this.streamer.sendBroadCastVariables(this.configuration);
			System.out.println("Java: sent BC vars");
			this.streamerOpen = true;
		}

	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.collector.isEmpty() && this.readForSplit;
	}

	public void destroy() throws Exception{
		if(this.streamerOpen) {
			this.streamer.sendMessage("close");
			this.streamer.close();
		}
		super.destroy();
	}
}
