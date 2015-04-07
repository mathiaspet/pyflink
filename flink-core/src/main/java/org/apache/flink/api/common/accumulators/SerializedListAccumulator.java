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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This accumulator stores a collection of objects in serialized form, so that the stored objects
 * are not affected by modifications to the original objects.
 *
 * Objects may be deserialized on demand with a specific classloader.
 *
 * @param <T> The type of the accumulated objects
 */
public class SerializedListAccumulator<T> implements Accumulator<T, ArrayList<byte[]>> {

	private static final long serialVersionUID = 1L;

	private ArrayList<byte[]> localValue = new ArrayList<byte[]>();

	@Override
	public void add(T value) {
		if (value == null) {
			throw new NullPointerException("Value to accumulate must nor be null");
		}

		try {
			byte[] byteArray = InstantiationUtil.serializeObject(value);
			localValue.add(byteArray);
		}
		catch (IOException e) {
			throw new RuntimeException("Serialization of accumulated value failed", e);
		}
	}

	@Override
	public ArrayList<byte[]> getLocalValue() {
		return localValue;
	}

	public ArrayList<T> deserializeLocalValue(ClassLoader classLoader) {
		try {
			ArrayList<T> arrList = new ArrayList<T>(localValue.size());
			for (byte[] byteArr : localValue) {
				@SuppressWarnings("unchecked")
				T item = (T) InstantiationUtil.deserializeObject(byteArr, classLoader);
				arrList.add(item);
			}
			return arrList;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot deserialize accumulator list element", e);
		}
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<byte[]>> other) {
		localValue.addAll(other.getLocalValue());
	}

	@Override
	public SerializedListAccumulator<T> clone() {
		SerializedListAccumulator<T> newInstance = new SerializedListAccumulator<T>();
		newInstance.localValue = new ArrayList<byte[]>(localValue);
		return newInstance;
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> deserializeList(ArrayList<byte[]> data, ClassLoader loader)
			throws IOException, ClassNotFoundException
	{
		List<T> result = new ArrayList<T>(data.size());
		for (byte[] bytes : data) {
			result.add((T) InstantiationUtil.deserializeObject(bytes, loader));
		}
		return result;
	}
}
