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
package org.apache.flink.python.api.streaming.io;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.python.api.PythonPlanBinder;
import org.apache.flink.python.api.io.PythonInputSplit;
import org.apache.flink.python.api.streaming.data.PythonReceiver;
import org.apache.flink.python.api.streaming.data.PythonSender;
import org.apache.flink.python.api.streaming.util.SerializationUtils;
import org.apache.flink.python.api.streaming.util.SerializationUtils.IntSerializer;
import org.apache.flink.python.api.streaming.util.SerializationUtils.StringSerializer;
import org.apache.flink.python.api.streaming.util.StreamPrinter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;

import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_DC_ID;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON3_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON2_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_COUNT;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_NAME_PREFIX;

import static org.apache.flink.python.api.streaming.util.SerializationUtils.getSerializer;

/**
 * This streamer is used by functions to send/receive data to/from an external python process.
 */
public class PythonSplitProcessorStreamer implements Serializable {
	protected static final Logger LOG = LoggerFactory.getLogger(PythonSplitProcessorStreamer.class);
	private static final int SIGNAL_BUFFER_REQUEST = 0;
	private static final int SIGNAL_BUFFER_REQUEST_G0 = -3;
	private static final int SIGNAL_BUFFER_REQUEST_G1 = -4;
	private static final int SIGNAL_FINISHED = -1;
	private static final int SIGNAL_ERROR = -2;
	private static final byte SIGNAL_LAST = 32;

	private static final int SIGNAL_MULTIPLES = -5;
	private static final int SIGNAL_MULTIPLES_END = -6;

	private final int id;
	private final boolean usePython3;
	private final String planArguments;

	private String inputFilePath;

	private Process process;
	private Thread shutdownThread;
	protected ServerSocket server;
	protected Socket socket;
	protected DataInputStream in;
	protected DataOutputStream out;
	protected int port;

	protected PythonSender sender;
	protected PythonReceiver receiver;

	protected StringBuilder msg = new StringBuilder();

	protected RichInputFormat format;
	private String outputFilePath;

	public PythonSplitProcessorStreamer(RichInputFormat format, int id, boolean asByteArray) {
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		planArguments = PythonPlanBinder.arguments.toString();
		sender = new PythonSender();
		receiver = new PythonReceiver(asByteArray);
		this.format = format;
	}


	/**
	 * Starts the python script.
	 *
	 * @throws IOException
	 */
	public void open() throws IOException {
		server = new ServerSocket(0);
		startPython();
		System.out.println("Started python");

	}

	private void startPython() throws IOException {
		System.out.println("starting python for IF");
		RuntimeContext ctx = this.format.getRuntimeContext();
		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + ctx.getIndexOfThisSubtask() + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + ctx.getIndexOfThisSubtask() + "input";
		System.out.println("paths");
		sender.open(inputFilePath);
		System.out.println("sender");
		receiver.open(outputFilePath);
		System.out.println("receiver");

		String path = ctx.getDistributedCache().getFile(FLINK_PYTHON_DC_ID).getAbsolutePath();
		String planPath = path + FLINK_PYTHON_PLAN_NAME;
		System.out.println(planPath);
		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}
		System.out.println("pythontest");
		process = Runtime.getRuntime().exec(pythonBinaryPath + " -O -B " + planPath + planArguments);
		System.out.println(pythonBinaryPath + " -O -B " + planPath + planArguments);
		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		shutdownThread = new Thread() {
			@Override
			public void run() {
				try {
					destroyProcess();
				} catch (IOException ex) {
				}
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownThread);

		OutputStream processOutput = process.getOutputStream();
		processOutput.write("format\n".getBytes());
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes());
		processOutput.write((id + "\n").getBytes());
		processOutput.write((inputFilePath + "\n").getBytes());
		processOutput.write((outputFilePath + "\n").getBytes());
		processOutput.flush();

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		try {
			process.exitValue();
			throw new RuntimeException("External process for task " + ctx.getTaskName() + " terminated prematurely." + msg);
		} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
		}

		socket = server.accept();
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
		this.sender.setOut(this.out);
		this.sender.setIn(this.in);
		this.receiver.setOut(this.out);
		this.receiver.setIn(this.in);
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		try {
			int signal = in.readInt(); //for debugging
			if (signal != SIGNAL_BUFFER_REQUEST) {
				throw new RuntimeException("yo aint getting no buffer");
			}
			Tuple3<String, Long, Long> closeTuple = new Tuple3<>("close", -1L, -1L);
			int size = this.sender.sendRecord(getSerializer(closeTuple).serialize(closeTuple), true);
			socket.close();
			sender.close();
			receiver.close();
		} catch (Exception e) {
			LOG.error("Exception occurred while closing Streamer. :" + e.getMessage());
		}
		destroyProcess();
		if (shutdownThread != null) {
			Runtime.getRuntime().removeShutdownHook(shutdownThread);
		}
	}

	private void destroyProcess() throws IOException {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException ise) { //process still active
			if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
				int pid;
				try {
					Field f = process.getClass().getDeclaredField("pid");
					f.setAccessible(true);
					pid = f.getInt(process);
				} catch (Throwable e) {
					process.destroy();
					return;
				}
				String[] args = new String[]{"kill", "-9", "" + pid};
				Runtime.getRuntime().exec(args);
			} else {
				process.destroy();
			}
		}
	}

	@Deprecated
	private void sendWriteNotification(int size, boolean hasNext) throws IOException {
		out.writeInt(size);
		out.writeByte(hasNext ? 0 : SIGNAL_LAST);
		out.flush();
	}

	private void sendReadConfirmation() throws IOException {
		out.writeByte(1);
		out.flush();
	}

	/**
	 * Sends all broadcast-variables encoded in the configuration to the external process.
	 *
	 * @param config configuration object containing broadcast-variable count and names
	 * @throws IOException
	 */
	public final void sendBroadCastVariables(Configuration config) throws IOException {
		try {
			int broadcastCount = config.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);

			String[] names = new String[broadcastCount];

			for (int x = 0; x < names.length; x++) {
				names[x] = config.getString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + x, null);
			}

			out.write(new IntSerializer().serializeWithoutTypeInfo(broadcastCount));

			StringSerializer stringSerializer = new StringSerializer();
			for (String name : names) {
				Iterator bcv = this.format.getRuntimeContext().getBroadcastVariable(name).iterator();

				out.write(stringSerializer.serializeWithoutTypeInfo(name));

				while (bcv.hasNext()) {
					out.writeByte(1);
					out.write((byte[]) bcv.next());
				}
				out.writeByte(0);
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + this.format.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

	private SerializationUtils.Serializer serializer;

	public final void transmitSplit(FileInputSplit inputSplit) throws IOException {
		PythonInputSplit split = (PythonInputSplit)inputSplit;

		int signal = in.readInt(); //for debugging
		if (signal != SIGNAL_BUFFER_REQUEST) {
			throw new RuntimeException("yo aint getting no buffer");
		}
		Tuple4<String, Long, Long, byte[]> tuple = new Tuple4<>(split.getPath().toString(), split.getStart(), split.getLength(), split.getAdditional());
		if (serializer == null) {
			serializer = getSerializer(tuple);
		}
		int size = sender.sendRecord(this.serializer.serialize(tuple), true);
	}

	public final boolean receiveResults(Collector c) throws IOException {
		if(this.format.getRuntimeContext().getExecutionConfig().isLargeTuples()) {
			this.receiver.setLargeTuples(true);
		}

		try {
			while(true) {
				int sig = in.readInt();
				switch (sig) {
					case SIGNAL_FINISHED:
						return true;
					case SIGNAL_ERROR:
						try { //wait before terminating to ensure that the complete error message is printed
							Thread.sleep(2000);
						} catch (InterruptedException ex) {
						}
						throw new RuntimeException(
							"External process for task " + this.format.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
					default:
						receiver.collectBuffer(c, sig);
				}
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + this.format.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

}
