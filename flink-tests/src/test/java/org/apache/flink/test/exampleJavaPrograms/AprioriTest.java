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
package org.apache.flink.test.exampleJavaPrograms;

import org.apache.flink.examples.java.assoc.APriori;
import org.junit.Test;

/**
 * Created by mathiasp on 05.06.16.
 */
public class AprioriTest {

	@Test
	public void testTransactionFindItems() {

		Integer[] transItems = new Integer[]{1, 3, 5, 8, 9, 10};
		APriori.Transaction transaction = new APriori.Transaction(1, transItems);

		Integer[] itemSet = new Integer[]{1,5,9};

		//TODO: find out how to configure favorites
		boolean found = transaction.containsItemSet(itemSet);
		if(found)
			System.out.println("correct");
		else
			System.out.println("error");


		itemSet = new Integer[]{1, 2, 3};
		found = transaction.containsItemSet(itemSet);
		if(!found)
			System.out.println("correct");
		else
			System.out.println("error");

		itemSet = new Integer[]{1, 3, 11};
		found = transaction.containsItemSet(itemSet);
		if(!found)
			System.out.println("correct");
		else
			System.out.println("error");


	}
}
