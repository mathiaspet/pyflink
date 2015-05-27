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

	@Override
	public boolean isImmutableType() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public TypeSerializer<Scene> duplicate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Scene createInstance() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Scene copy(Scene from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Scene copy(Scene from, Scene reuse) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void serialize(Scene record, DataOutputView target)
			throws IOException {
		// TODO Auto-generated method stub
		
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
