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
package org.apache.flink.languagebinding.api.java.python.streaming;

import static org.apache.flink.languagebinding.api.java.common.PlanBinder.DEBUG;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON2_BINARY_PATH;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON3_BINARY_PATH;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON_DC_ID;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_DIR;


import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.languagebinding.api.java.common.streaming.StreamPrinter;
import org.apache.flink.languagebinding.api.java.common.streaming.Streamer;
import org.apache.flink.languagebinding.api.java.python.PythonPlanBinder;

/**
 * This streamer is used by functions to send/receive data to/from an external python process.
 */
public class PythonStreamer extends Streamer {
	private Process process;
	private final int id;
	private final boolean usePython3;
	private final boolean debug;
	private Thread shutdownThread;
	private final String planArguments;

	private String inputFilePath;
	private String outputFilePath;

	/*
	 * This code duplication of constructors is ugly and only necessary because the 
	 * RuntimeContext is not available during construction. 
	 */
	public PythonStreamer(RichInputFormat format, int id) {
		super(format, false);
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		this.debug = DEBUG;
		planArguments = PythonPlanBinder.arguments.toString();
	}

	public PythonStreamer(RichInputFormat format, int id, boolean atJobManager) {
		super(format, atJobManager);
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		this.debug = DEBUG;
		planArguments = PythonPlanBinder.arguments.toString();
	}

	public PythonStreamer(RichOutputFormat format, int id) {
		super(format);
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		this.debug = DEBUG;
		planArguments = PythonPlanBinder.arguments.toString();
	}



	public PythonStreamer(AbstractRichFunction function, int id) {
		super(function);
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		this.debug = DEBUG;
		planArguments = PythonPlanBinder.arguments.toString();
	}
	
	/**
	 * Starts the python script.
	 *
	 * @throws IOException
	 */
	@Override
	public void setupProcess() throws IOException {
		startPython();
	}

	private void startPython() throws IOException {
		if(this.atJobManager) {
			startJobProcess();
		}else {
			startTaskProcess();
		}
	}

	/**
	 * Does essentially the same as startTaskProcess() but without
	 * references to subtasks. This method is used for executing Python code
	 * directly at the job manager during job creation.
	 * @throws IOException
	 */
	private void startJobProcess() throws IOException {


		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + "input";

		sender.open(inputFilePath);
		receiver.open(outputFilePath);
		//TODO: use the systems value here
		String path = System.getProperty("java.io.tmpdir") + "/" + FLINK_PYTHON_DC_ID;
		String planPath = path + FLINK_PYTHON_PLAN_NAME;

		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}

		if (debug) {
			socket.setSoTimeout(0);
			LOG.info("Waiting for Python Process : " + this.context.getTaskName()
					+ " Run python " + planPath + planArguments);
		} else {
			process = Runtime.getRuntime().exec(pythonBinaryPath + " -O -B " + planPath + planArguments);
			new StreamPrinter(process.getInputStream()).start();
			new StreamPrinter(process.getErrorStream(), true, msg).start();
		}

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
		processOutput.write("operator\n".getBytes());
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes());
		processOutput.write((id + "\n").getBytes());
		processOutput.write((inputFilePath + "\n").getBytes());
		processOutput.write((outputFilePath + "\n").getBytes());
		processOutput.flush();

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		if (!debug) {
			try {
				process.exitValue();
				throw new RuntimeException("External process for task " + this.id + " terminated prematurely." + msg);
			} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
			}
		}

		socket = server.accept();
		in = socket.getInputStream();
		out = socket.getOutputStream();
	}


	private void startTaskProcess() throws IOException {
		this.setContext();
		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.context.getIndexOfThisSubtask() + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.context.getIndexOfThisSubtask() + "input";

		sender.open(inputFilePath);
		receiver.open(outputFilePath);

		String path = this.context.getDistributedCache().getFile(FLINK_PYTHON_DC_ID).getAbsolutePath();
		String planPath = path + FLINK_PYTHON_PLAN_NAME;

		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}

		if (debug) {
			socket.setSoTimeout(0);
			LOG.info("Waiting for Python Process : " + this.context.getTaskName()
					+ " Run python " + planPath + planArguments);
		} else {
			process = Runtime.getRuntime().exec(pythonBinaryPath + " -O -B " + planPath + planArguments);
			new StreamPrinter(process.getInputStream()).start();
			new StreamPrinter(process.getErrorStream(), true, msg).start();
		}

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
		processOutput.write("operator\n".getBytes());
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes());
		processOutput.write((id + "\n").getBytes());
		processOutput.write((inputFilePath + "\n").getBytes());
		processOutput.write((outputFilePath + "\n").getBytes());
		processOutput.flush();

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		if (!debug) {
			try {
				process.exitValue();
				throw new RuntimeException("External process for task " + this.context.getTaskName() + " terminated prematurely." + msg);
			} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
			}
		}

		socket = server.accept();
		in = socket.getInputStream();
		out = socket.getOutputStream();
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		try {
			super.close();
		} catch (Exception e) {
			LOG.error("Exception occurred while closing Streamer. :" + e.getMessage());
		}
		if (!debug) {
			destroyProcess();
		}
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
}
