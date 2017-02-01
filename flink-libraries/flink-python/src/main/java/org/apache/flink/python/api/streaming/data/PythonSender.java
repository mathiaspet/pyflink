/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.python.api.streaming.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.MAPPED_FILE_SIZE;

/**
 * General-purpose class to write data to memory-mapped files.
 */
public class PythonSender<IN> implements Serializable {
	public static final byte TYPE_ARRAY = (byte) 63;
	public static final byte TYPE_KEY_VALUE = (byte) 62;
	public static final byte TYPE_VALUE_VALUE = (byte) 61;
	public static final byte TYPE_STRING_VALUE = (byte) 28;

	private static final byte SIGNAL_LAST = 32;
	private static final int SIGNAL_BUFFER_REQUEST = 0;

	private File outputFile;
	private RandomAccessFile outputRAF;
	private FileChannel outputChannel;
	private MappedByteBuffer fileBuffer;

	/* TODO: moved here from PythonStreamer */
	private DataOutputStream out;
	private DataInputStream in;
	private boolean largeTuples;


	private final ByteBuffer[] saved = new ByteBuffer[2];

	private final Serializer[] serializer = new Serializer[2];

	//=====Setup========================================================================================================
	public void open(String path) throws IOException {
		setupMappedFile(path);
	}

	private void setupMappedFile(String outputFilePath) throws FileNotFoundException, IOException {
		File x = new File(FLINK_TMP_DATA_DIR);
		x.mkdirs();

		outputFile = new File(outputFilePath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		outputFile.createNewFile();
		outputRAF = new RandomAccessFile(outputFilePath, "rw");
		outputRAF.setLength(MAPPED_FILE_SIZE);
		outputRAF.seek(MAPPED_FILE_SIZE - 1);
		outputRAF.writeByte(0);
		outputRAF.seek(0);
		outputChannel = outputRAF.getChannel();
		fileBuffer = outputChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_FILE_SIZE);
	}

	public void setOut(DataOutputStream out){this.out = out;}

	public void setIn(DataInputStream in){this.in = in;}

	public void setLargeTuples(boolean large){this.largeTuples = large;}

	public void close() throws IOException {
		closeMappedFile();
	}

	private void closeMappedFile() throws IOException {
		outputChannel.close();
		outputRAF.close();
	}

	/**
	 * Resets this object to the post-configuration state.
	 */
	public void reset() {
		serializer[0] = null;
		serializer[1] = null;
		fileBuffer.clear();
	}

	//=====IO===========================================================================================================
	/**
	 * Writes a single record to the memory-mapped file. This method does NOT take care of synchronization. The user
	 * must guarantee that the file may be written to before calling this method. This method essentially reserves the
	 * whole buffer for one record. As such it imposes some performance restrictions and should only be used when
	 * absolutely necessary.
	 *
	 * @param value record to send
	 * @return size of the written buffer
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public int sendRecord(Object value, boolean hasMore) throws IOException {
		fileBuffer.clear();
		int group = 0;

		serializer[group] = getSerializer(value);
		ByteBuffer bb = serializer[group].serialize(value);
		if (bb.remaining() > MAPPED_FILE_SIZE) {
			throw new RuntimeException("Serialized object does not fit into a single buffer.");
		}
		fileBuffer.put(bb);

		int size = fileBuffer.position();

		reset();
		sendWriteNotification(size, hasMore);
		return size;
	}

	public boolean hasRemaining(int group) {
		return saved[group] != null;
	}

	/**
	 * Extracts records from an iterator and writes them to the memory-mapped file. This method assumes that all values
	 * in the iterator are of the same type. This method does NOT take care of synchronization. The caller must
	 * guarantee that the file may be written to before calling this method.
	 *
	 * @param i iterator containing records
	 * @param group group to which the iterator belongs, most notably used by CoGroup-functions.
	 * @return size of the written buffer
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public int sendBuffer(Iterator i, int group) throws IOException {
		if(this.largeTuples) {
			return sendLargeTuples(i, group);
		}else {
			fileBuffer.clear();

			Object value;
			ByteBuffer bb;
			if (serializer[group] == null) {
				value = i.next();
				serializer[group] = getSerializer(value);
				bb = serializer[group].serialize(value);
				if (bb.remaining() > MAPPED_FILE_SIZE) {
					throw new RuntimeException("Serialized object does not fit into a single buffer.");
				}
				fileBuffer.put(bb);

			}
			if (saved[group] != null) {
				fileBuffer.put(saved[group]);
				saved[group] = null;
			}
			while (i.hasNext() && saved[group] == null) {
				value = i.next();
				bb = serializer[group].serialize(value);
				if (bb.remaining() > MAPPED_FILE_SIZE) {
					throw new RuntimeException("Serialized object does not fit into a single buffer.");
				}
				if (bb.remaining() <= fileBuffer.remaining()) {
					fileBuffer.put(bb);
				} else {
					saved[group] = bb;
				}
			}

			int size = fileBuffer.position();
			sendWriteNotification(size, this.hasRemaining(0) || i.hasNext());
			return size;
		}
	}

	private int sendLargeTuples(Iterator i, int group) throws IOException {
		//while iterator is not empty
		//get next tuple
		//serialize tuple
		//chunk tuple
		//transmit chunks

		while(i.hasNext()) {
			fileBuffer.clear();
			Object value;
			ByteBuffer bb;
			value = i.next();
			if (serializer[group] == null) {
				serializer[group] = getSerializer(value);
			}
			bb = serializer[group].serialize(value);
			System.out.println("java\tserialized: " + bb.get(0));
			System.out.println("java\tserialized: " + bb.get(1));
			System.out.println("java\tserialized: " + bb.get(2));
			System.out.println("java\tserialized: " + bb.get(3));
			System.out.println("java\tserialized: " + bb.get(4));
			System.out.println("java\tserialized: " + bb.get(5));
			System.out.println("java\tserialized: " + bb.get(6));
			System.out.println("java\tserialized: " + bb.get(7));
			int tupleSize = bb.limit();
			//send size
			sendWriteNotification(tupleSize, true);

			int numTrips = tupleSize / MAPPED_FILE_SIZE;
			//send numTrips?
			int remainder = tupleSize % MAPPED_FILE_SIZE;

			byte[] chunk = new byte[MAPPED_FILE_SIZE];
			for (int j = 0; j < numTrips; j++) {
				int nextBuffer = in.readInt();
				if (nextBuffer != SIGNAL_BUFFER_REQUEST) {
					System.out.println("false signal: " + nextBuffer);
				}
				bb.get(chunk);
				fileBuffer.put(chunk);

				sendWriteNotification(MAPPED_FILE_SIZE, true);

				fileBuffer.clear();
			}

			if (remainder != 0) {
				int nextBuffer = in.readInt();
				chunk = new byte[remainder];
				bb.get(chunk, 0, remainder);
				fileBuffer.put(chunk);
				sendWriteNotification(remainder, i.hasNext());
			}

			int multiplesLast = in.readInt();

		}
		return 1;
	}

	private void sendWriteNotification(int size, boolean hasNext) throws IOException {
		System.out.println("java\tsending write notification: " + size);
		out.writeInt(size);
		out.writeByte(hasNext ? 0 : SIGNAL_LAST);
		out.flush();
	}

	//=====Serializer===================================================================================================
	private Serializer getSerializer(Object value) {
		if (value instanceof byte[]) {
			return new ArraySerializer();
		}

		if (((Tuple2) value).f0 instanceof byte[]) {
			return new ValuePairSerializer();
		}
		if (((Tuple2) value).f0 instanceof Tuple) {
			return new KeyValuePairSerializer();
		}
		throw new IllegalArgumentException("This object can't be serialized: " + value.toString());
	}

	public ByteBuffer serialize(Object next) {
		return getSerializer(next).serialize(next);
	}

	public void transmitChunk(byte[] chunk) {
		//write this into the fileBuffer
	}

	private abstract class Serializer<T> {
		protected ByteBuffer buffer;

		public ByteBuffer serialize(T value) {
			serializeInternal(value);
			buffer.flip();
			return buffer;
		}

		public abstract void serializeInternal(T value);
	}

	private class ArraySerializer extends Serializer<byte[]> {
		@Override
		public void serializeInternal(byte[] value) {
			buffer = ByteBuffer.allocate(value.length + 1);
			buffer.put(TYPE_ARRAY);
			buffer.put(value);
		}
	}

	private class ValuePairSerializer extends Serializer<Tuple2<byte[], byte[]>> {
		@Override
		public void serializeInternal(Tuple2<byte[], byte[]> value) {
			buffer = ByteBuffer.allocate(1 + value.f0.length + value.f1.length);
			buffer.put(TYPE_VALUE_VALUE);
			buffer.put(value.f0);
			buffer.put(value.f1);
		}
	}

	private class KeyValuePairSerializer extends Serializer<Tuple2<Tuple, byte[]>> {
		@Override
		public void serializeInternal(Tuple2<Tuple, byte[]> value) {
			int keySize = 0;
			for (int x = 0; x < value.f0.getArity(); x++) {
				keySize += ((byte[]) value.f0.getField(x)).length;
			}
			buffer = ByteBuffer.allocate(5 + keySize + value.f1.length);
			buffer.put(TYPE_KEY_VALUE);
			buffer.put((byte) value.f0.getArity());
			for (int x = 0; x < value.f0.getArity(); x++) {
				buffer.put((byte[]) value.f0.getField(x));
			}
			buffer.put(value.f1);
		}
	}
}
