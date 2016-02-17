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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.util.Collector;

/**
 * Simple queue implementation of a {@link Collector}.
 *
 */
public class PythonCollector<T> implements Collector<T> {
	
	private Queue<T> content = new ConcurrentLinkedQueue<T>();
	
	@Override
	public void collect(T record) {
		this.content.offer(record);
	}

	@Override
	public void close() {
		//TODO: alert the caller, that content is not empty
		this.content = null;
	}
	
	public T poll()	{
		return this.content.poll();
	}

}
