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
import java.io.Serializable;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A representation of a 2-dimensional coordinate.
 * 
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 *
 */
public class Coordinate implements Serializable {
	private static final long serialVersionUID = -6428372453643359884L;

	//TODO: figure out de-/serialization and make lat an dlon final again
	public double lat;
	public double lon;

	public Coordinate() {
		this.lat = Double.NaN;
		this.lon = Double.NaN;
	}

	public Coordinate(double lon, double lat) {
		this.lat = lat;
		this.lon = lon;
	}
	
	public Coordinate(Coordinate copy) {
		this.lon = copy.lon;
		this.lat = copy.lat;
	}

	@Override
	public boolean equals(Object o) {
		if(o == null || !(o instanceof Coordinate)) {
			return false;
		}
		Coordinate other = (Coordinate) o;
		return other.lat == this.lat && other.lon == this.lon;
	}
	
	
	/**
	 * Returns the coordinate difference of this coordinate minus the given coordinate.
	 */
	public Coordinate diff(Coordinate other) {
		return new Coordinate(this.lat - other.lat, this.lon - other.lon);
	}

	/**
	 * Returns the coordinate difference scaled by the given factors in both dimensions
	 */
	public Coordinate scale(double latScale, double lonScale) {
		return new Coordinate(this.lat * latScale, this.lon * lonScale);
	}

	/**
	 * Returns the coordinate advanced by a coordinate difference.
	 */
	public Coordinate add(Coordinate other) {
		return new Coordinate(this.lat + other.lat, this.lon + other.lon);
	}
	
	/**
	 * Returns the coordinate advanced by a coordinate difference scaled by the given factors.
	 */
	public Coordinate addScaled(Coordinate diff, double scaleLat, double scaleLon) {
		return new Coordinate(this.lat + diff.lat * scaleLat, this.lon + diff.lon * scaleLon);
	}
	
	@Override
	public String toString() {
		return "(" + this.lon + ", " + this.lat + ")";
	}

	public Coordinate copy() {
		return new Coordinate(this.lon, this.lat);
	}

	public void serialize(DataOutputView target) throws IOException {
		target.writeDouble(this.lon);
		target.writeDouble(this.lat);
	}
	
	public void deserialize(DataInputView source) throws IOException {
		this.lon = source.readDouble();
		this.lat = source.readDouble();
	}
}
