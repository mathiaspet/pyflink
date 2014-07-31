/**
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

package org.apache.flink.api.common.functions;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.util.Collector;

/**
 * Generic interface used for combine functions ("combiners"). Combiners act as auxiliaries to a {@link GroupReduceFunction}
 * and "pre-reduce" the data. The combine functions typically do not see the entire group of elements, but
 * only a sub-group.
 * <p>
 * Combine functions are frequently helpful in increasing the program efficiency, because they allow the system to
 * reduce the data volume earlier, before the entire groups have been collected.
 * <p>
 * This special variant of the combine function supports to return more than one element per group.
 * It is frequently less efficient to use than the {@link CombineFunction}.
 * 
 * @param <T> The data type processed by the combine function.
 */
public interface FlatCombineFunction<T> extends Function, Serializable {

	/**
	 * The combine method, called (potentially multiple timed) with subgroups of elements.
	 * 
	 * @param values The elements to be combined.
	 * @param out The collector to use to return values from the function.
	 * 
	 * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
	 *                   and may trigger the recovery logic.
	 */
	void combine(Iterator<T> values, Collector<T> out) throws Exception;
}
