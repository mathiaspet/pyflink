package org.apache.flink.api.java.spatial.envi;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.api.java.spatial.TileInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class SceneInputSplit extends FileInputSplit implements Serializable{
	private static final long serialVersionUID = -9205048860784884871L;
	public TileInfo info;

	public SceneInputSplit() {
		super();
		this.info = null;
	}
	
	public SceneInputSplit(int num, Path file, long start, long length, String[] hosts, TileInfo info) {
		super(num, file, start, length, hosts);
		this.info = info;
	}
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.info = new TileInfo();
		this.info.deserialize(in);
		
		super.read(in);
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		this.info.serialize(out);
		super.write(out);
	}
}