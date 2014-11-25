package org.apache.flink.api.java.spatial;

import java.io.Serializable;

/**
 * A representation of a 2-dimensional coordinate.
 * @author Dennis Schneider <dschneid@informatik.hu-berlin.de>
 *
 */
public class Coordinate implements Serializable {
	private static final long serialVersionUID = -6428372453643359884L;

	public final double lat; 
	public final double lon; 
	
	public Coordinate() {
		 this.lat = Double.NaN;
		 this.lon = Double.NaN;
	}
	
	public Coordinate(double lat, double lon) {
		this.lat = lat;
		this.lon = lon;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null || !(o instanceof Coordinate)) return false;
		Coordinate other = (Coordinate) o;
		return other.lat == this.lat && other.lon == this.lon;
	}
}
