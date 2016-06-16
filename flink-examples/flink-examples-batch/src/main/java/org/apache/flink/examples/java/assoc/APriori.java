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

package org.apache.flink.examples.java.assoc;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mathiasp on 31.05.16.
 */
public class APriori {



	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		if (!params.has("input") && !params.has("confidence") && !params.has("minsupport") && !params.has("output")) {
			System.err.println("  Usage: APriori --input <path> --confidence <conf> --minsupport <support> --output <path>");
			return;
		}



		DataSet<Tuple2<Integer, Integer>> transactions = getTransactionsDataSet(env, params.get("input"));

		long transCount = transactions.groupBy(0).max(1).count();
		final long minSupport = (long)(transCount * params.getDouble("minsupport", 0.7));

		System.out.println("Counted: " + transCount + " ergo minsupport is :" + minSupport);

		DataSet<Tuple2<Integer, Integer>> heavyOneSets = transactions.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return new Tuple2<Integer, Integer>(value.f1, 1);
			}
		}).groupBy(0).sum(1).filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return value.f1 >= minSupport;
			}
		});

		DataSet<Tuple3<String, String, Integer>> twoSets = heavyOneSets.cross(heavyOneSets).with(new CrossFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple3<String, String, Integer>>() {

			/**
			 * Performs a triangular self join
			 * @param val1 Element from first input.
			 * @param val2 Element from the second input.
			 * @return
			 * @throws Exception
			 */
			@Override
			public Tuple3<String, String, Integer> cross(Tuple2<Integer, Integer> val1, Tuple2<Integer, Integer> val2) throws Exception {
				if(val1.f0 < val2.f0) {
					return new Tuple3<String, String, Integer>("", "" + val1.f0 + "-" + val2.f0, 1);
				} else {
					return new Tuple3<String, String, Integer>("", "invalid", -1);
				}
			}
		}).filter(new FilterFunction<Tuple3<String, String, Integer>>() {

			/**
			 * Filters the invalid results that would cause duplicates
			 * @param value The value to be filtered.
			 * @return
			 * @throws Exception
			 */
			@Override
			public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
				return !value.f1.equals("invalid");
			}
		});

		System.out.println("Found two set candidates: ");
		twoSets.print();

		DataSet<Transaction> trans = transactions.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Transaction>() {
			@Override
			public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Transaction> out) throws Exception {
				List<Integer> items = new ArrayList<Integer>();
				int tid = 0;
				for(Tuple2<Integer, Integer> t : values) {
					tid = t.f0;
					items.add(t.f1);
				}

				Collections.sort(items);
				out.collect(new Transaction(tid, items.toArray(new Integer[]{})));
			}
		});

		DataSet<Tuple3<String, String, Integer>> heavyTwosets = trans.crossWithTiny(twoSets).with(new TransactionJoin()).filter(new InvalidFilter())
			.groupBy(1).sum(2).filter(new MinSupportFilter(minSupport));

		System.out.println("Refined to heavy two sets:");
		heavyTwosets.print();

		DataSet<Tuple3<String, String, Integer>> preparedSets = heavyTwosets.map(new KeySetMap());

		System.out.println("Created prep sets:");
		preparedSets.print();

		DataSet<Tuple3<String, String, Integer>> lkCandidates = preparedSets.join(preparedSets).where(0).equalTo(0).with(new LKCandidateCreator());

		System.out.println("lkCandidates: ");
		lkCandidates.print();

		DataSet<Tuple3<String, String, Integer>> lkHeavySets = trans.crossWithTiny(lkCandidates).with(new TransactionJoin()).filter(new InvalidFilter())
			.groupBy(1).sum(2).filter(new MinSupportFilter(minSupport));

		System.out.println("heavy 2 sets:");
		lkHeavySets.print();

		preparedSets.writeAsCsv(params.get("output"));

		env.execute();
		//use delta iteration
		//group by transaction
		//build every possible n+1 item set
		//group by n+1-itemset, count and filter by min support
		//create association rules
		//repeat
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	/**
	 * The result is a data set of pairs (Tid, Iid)
	 * @param env
	 * @param inputPath
	 * @return
	 */
	private static DataSet<Tuple2<Integer, Integer>> getTransactionsDataSet(ExecutionEnvironment env, String inputPath) {
		return env.readCsvFile(inputPath)
			.fieldDelimiter("\t")
			.includeFields("11")
			.types(Integer.class, Integer.class);
	}

	public static class Transaction extends Tuple2<Integer, Integer[]> {

		public Transaction(){}

		public Transaction(int i, Integer[] transItems) {
			this.f0 = i;
			this.f1 = transItems;
		}

		public Integer getTID() {return this.f0;}

		/**
		 * Assumption: the array is sorted in ascending order
		 * @return
		 */
		public Integer[] getItems() {return this.f1;}

		/**
		 * TODO: implement bin search
		 * TODO: change signature to return the position at which the item was found
		 * @param item
		 * @return
		 */
		public boolean containsItem(Integer item) {

			if(this.f1 == null || this.f1.length == 0)
			{
				return false;
			}

			for(Integer i : this.f1){
				if(i == item) {
					return true;
				}
			}

			return false;
		}

		/**
		 * TODO: should be implemented using a prefix tree
		 * however, for the time being as array making use if the sortedness
		 * @return
		 */
		public boolean containsItemSet(Integer[] itemSet) {
			int currentPos = 0;

			for(; currentPos < itemSet.length; currentPos++) {
				for(Integer transItem : this.f1){
					if(currentPos == itemSet.length) {
						return true;
					}

					if(transItem == itemSet[currentPos]){
						currentPos++;
						continue;
					}

					if(transItem > itemSet[currentPos]) {
						return false;
					}
				}
				//since both arrays, this.f1 and the itemSet param, are sorted
				//one pass over both is enough
				if(currentPos == itemSet.length)
				{
					return true;
				}
				else
				{
					return false;
				}
			}

			return false;
		}

	}

	public static class MinSupportFilter implements FilterFunction<Tuple3<String, String, Integer>> {

		private long minSupport;

		public MinSupportFilter(long minSupp){
			this.minSupport = minSupp;
		}

		@Override
		public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
			return value.f2 >= this.minSupport;
		}
	}

	public static class InvalidFilter implements FilterFunction<Tuple3<String, String, Integer>> {

		//Filter out the invalid ones
		@Override
		public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
			return value.f2 > 0;
		}
	}

	public static class LKCandidateCreator implements FlatJoinFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> {
		@Override
		public void join(Tuple3<String, String, Integer> first, Tuple3<String, String, Integer> second, Collector<Tuple3<String, String, Integer>> out) throws Exception {


			if(first.f1.equals(second.f1))
			{
				return;
			}

			Integer leftSuffix = Integer.parseInt(first.f1.substring(first.f0.length()+1));
			Integer rightSuffix = Integer.parseInt(second.f1.substring(second.f0.length()+1));

			if(leftSuffix < rightSuffix) {
				//append right to left
				first.f0 = first.f1;
				first.f1 = first.f1 + "-" + rightSuffix;
				first.f2 = 1;
				out.collect(first);
			}
		}
	}

	public static class KeySetMap implements MapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> {
		@Override
		public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {
			//extract the key prefix and put it into the first field of the out tuple
			int splitPoint = value.f1.lastIndexOf("-");
			value.f0 = value.f1.substring(0, splitPoint);
			return value;
		}
	}

	public static class TransactionJoin implements CrossFunction<Transaction, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> {

		public Tuple3<String, String, Integer> cross(Transaction val1, Tuple3<String, String, Integer> val2) throws Exception {
			String[] items = val2.f1.split("-");
			Integer[] itemSet = new Integer[items.length];
			for(int i = 0;i < items.length;i++) {
				itemSet[i] = Integer.parseInt(items[i]);
			}

			if(val1.containsItemSet(itemSet)){
				val2.f2 = 1;
			} else {
				val2.f2 = -1;
			}
			return val2;
		}
	}


	public static class TransactionBuilder implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer[]>> out) throws Exception {
			int tid = 0;

			List<Integer> items = new ArrayList<Integer>();
			for(Tuple2<Integer, Integer> v : values) {
				tid = v.f0;
				items.add(v.f1);
			}

			Collections.sort(items);

			out.collect(new Tuple2<Integer, Integer[]>(new Integer(tid), (Integer[])items.toArray()));

		}
	}

	/**
	 * Homegrown join implementation
	 * has quadratic complexity as it loops twice
	 */
	@Deprecated
	public static class CandidateGenerator implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> values, Collector<Tuple2<Integer, Integer[]>> out) throws Exception {

		}
	}
}

