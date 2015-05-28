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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * @author mathiasp
 *
 */
public class SceneSerializer extends TypeSerializer<Scene> {

	
	private static SceneSerializer INSTANCE = new SceneSerializer();
	
	private SceneSerializer() {
	}
	
	public static SceneSerializer getInstance() {
		return INSTANCE;
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Scene> duplicate() {
		return this;
	}

	@Override
	public Scene createInstance() {
		return new Scene();
	}

	@Override
	public Scene copy(Scene from) {
		return from.createCopy();
	}

	@Override
	public Scene copy(Scene from, Scene reuse) {
		from.copyTo(reuse);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Scene record, DataOutputView target)
			throws IOException {
		record.serialize(target);
	}

	@Override
	public Scene deserialize(DataInputView source) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Scene deserialize(Scene reuse, DataInputView source)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
