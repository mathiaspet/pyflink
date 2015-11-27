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
 * (De-)Serialization for {@link Tile}s.
 *
 */
public class TileTypeSerializer extends TypeSerializer<Tile> {

	private static final long serialVersionUID = 1L;
	
	private static TileTypeSerializer INSTANCE = new TileTypeSerializer();
	
	private TileTypeSerializer() {
	}
	
	public static TileTypeSerializer get() {
		return INSTANCE;
	}
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Tile createInstance() {
		return new Tile();
	}

	@Override
	public Tile copy(Tile from) {
		return from.createCopy();
	}

	@Override
	public Tile copy(Tile from, Tile reuse) {
		from.copyTo(reuse);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Tile record, DataOutputView target) throws IOException {
		record.serialize(target);
	}

	@Override
	public Tile deserialize(DataInputView source) throws IOException {
		return deserialize(new Tile(), source);
	}

	@Override
	public Tile deserialize(Tile reuse, DataInputView source) throws IOException {
		reuse.deserialize(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target)
			throws IOException {
		Tile intermediate = new Tile();
		intermediate.deserialize(source);
		intermediate.serialize(target);
		intermediate = null;
	}

	@Override
	public TileTypeSerializer duplicate() {
		return this;
	}

	@Override
	public int hashCode() {
		return 23;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TileTypeSerializer;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TileTypeSerializer) {
			TileTypeSerializer other = (TileTypeSerializer) obj;

			return other.canEqual(this);
		} else {
			return false;
		}
	}
}
