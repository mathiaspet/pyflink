/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.LocalStreamEnvironment;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.junit.Test;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;

public class FlatMapTest {

	public static final class MyFlatMap extends FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Integer> value, Collector<Tuple1<Integer>> out) throws Exception {
			out.collect(new Tuple1<Integer>(value.f0 * value.f0));

		}

	}

	public static final class ParallelFlatMap extends
			FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Integer> value, Collector<Tuple1<Integer>> out) throws Exception {
			numberOfElements++;

		}

	}

	public static final class GenerateSequenceFlatMap extends
			FlatMapFunction<Tuple1<Long>, Tuple1<Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Long> value, Collector<Tuple1<Long>> out) throws Exception {
			out.collect(new Tuple1<Long>(value.f0 * value.f0));

		}

	}

	public static final class MySink extends SinkFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			result.add(tuple.f0);
		}

	}

	public static final class FromElementsSink extends SinkFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			fromElementsResult.add(tuple.f0);
		}

	}

	public static final class FromCollectionSink extends SinkFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			fromCollectionResult.add(tuple.f0);
		}

	}

	public static final class GenerateSequenceSink extends SinkFunction<Tuple1<Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Long> tuple) {
			generateSequenceResult.add(tuple.f0);
		}

	}

	private static void fillExpectedList() {
		for (int i = 0; i < 10; i++) {
			expected.add(i * i);
		}
	}

	private static void fillFromElementsExpected() {
		fromElementsExpected.add(4);
		fromElementsExpected.add(25);
		fromElementsExpected.add(81);
	}

	private static void fillSequenceSet() {
		for (int i = 0; i < 10; i++) {
			sequenceExpected.add(i * i);
		}
	}

	private static void fillLongSequenceSet() {
		for (int i = 0; i < 10; i++) {
			sequenceLongExpected.add((long) (i * i));
		}
	}

	private static void fillFromCollectionSet() {
		if (fromCollectionSet.isEmpty()) {
			for (int i = 0; i < 10; i++) {
				fromCollectionSet.add(i);
			}
		}
	}

	private static final int PARALLELISM = 1;
	private static final long MEMORYSIZE = 32;

	private static int numberOfElements = 0;
	private static Set<Integer> expected = new HashSet<Integer>();
	private static Set<Integer> result = new HashSet<Integer>();
	private static Set<Integer> fromElementsExpected = new HashSet<Integer>();
	private static Set<Integer> fromElementsResult = new HashSet<Integer>();
	private static Set<Integer> fromCollectionSet = new HashSet<Integer>();
	private static Set<Integer> sequenceExpected = new HashSet<Integer>();
	private static Set<Long> sequenceLongExpected = new HashSet<Long>();
	private static Set<Integer> fromCollectionResult = new HashSet<Integer>();
	private static Set<Long> generateSequenceResult = new HashSet<Long>();

	@Test
	public void test() throws Exception {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
		// flatmapTest

		fillFromCollectionSet();

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream = env.fromCollection(fromCollectionSet)
				.flatMap(new MyFlatMap()).addSink(new MySink());

		fillExpectedList();

		// parallelShuffleconnectTest
		fillFromCollectionSet();

		DataStream<Tuple1<Integer>> source = env.fromCollection(fromCollectionSet);
		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> map = source
				.flatMap(new ParallelFlatMap())
				.addSink(new MySink());
		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> map2 = source
				.flatMap(new ParallelFlatMap())
				.addSink(new MySink());

		// fromElementsTest
		DataStream<Tuple1<Integer>> fromElementsMap = env
				.fromElements(2, 5, 9)
				.flatMap(new MyFlatMap());
		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> sink = fromElementsMap.addSink(new FromElementsSink());

		fillFromElementsExpected();

		// fromCollectionTest
		fillFromCollectionSet();

		DataStream<Tuple1<Integer>> fromCollectionMap = env
				.fromCollection(fromCollectionSet)
				.flatMap(new MyFlatMap());
		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> fromCollectionSink = fromCollectionMap
				.addSink(new FromCollectionSink());

		// generateSequenceTest
		fillSequenceSet();

		DataStream<Tuple1<Long>> generateSequenceMap = env
				.generateSequence(0, 9)
				.flatMap(new GenerateSequenceFlatMap());
		@SuppressWarnings("unused")
		DataStream<Tuple1<Long>> generateSequenceSink = generateSequenceMap
				.addSink(new GenerateSequenceSink());

		fillLongSequenceSet();

		env.executeTest(MEMORYSIZE);

		assertTrue(expected.equals(result));
		assertEquals(20, numberOfElements);
		assertEquals(fromElementsExpected, fromElementsResult);
		assertEquals(sequenceExpected, fromCollectionResult);
		assertEquals(sequenceLongExpected, generateSequenceResult);
	}
}
