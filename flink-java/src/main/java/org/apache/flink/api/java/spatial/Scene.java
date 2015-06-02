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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Represents a complete Scene including possibly all the bands
 *
 */
public class Scene extends SpatialObject {
	/**
	 * 
	 */
	private static final long serialVersionUID = -937223003083256908L;

	private int numBands = -1;
	
	public int getNumBands() {
		return numBands;
	}

	public void setNumBands(int numBands) {
		this.numBands = numBands;
	}

	public Scene(){}
	
	public Scene(Scene scene) {
		super(scene);
		this.numBands = scene.numBands;
	}


	/**
	 * Update the tile information to the given object.
	 * 
	 * @param aqcDate
	 */
	public void update(TileInfo info) {
		this.tileInfo = info;
		this.luCord = info.getUpperLeftCoordinate();
		this.rlCord = info.getLowerRightCoordinate();
		this.tileWidth = info.getSamples();
		this.tileHeight = info.getLines();
		this.numBands = info.getBands();
		this.aqcuisitionDate = info.getAcqDateAsString();
		this.xPixelWith = info.getPixelWidth();
		this.yPixelWidth = info.getPixelHeight();
	}
	
	
	public Scene createCopy() {
		return new Scene(this);
	}
	
	public void copyTo(Scene target) {
		super.copyTo(target);
		target.numBands = this.numBands;
	}
	
	@Override
	public void serialize(DataOutputView target) throws IOException {
		super.serialize(target);
		target.writeInt(this.numBands);
	}
	
	@Override
	public void deserialize(DataInputView source) throws IOException {
		super.deserialize(source);
		this.numBands = source.readInt();
	}
}
