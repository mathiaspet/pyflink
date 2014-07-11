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


package org.apache.flink.api.common;

/**
 * Implementing this interface allows a Program to have a description
 * of the plan which can be shown to the user. For a more detailed description
 * of what should be included in the description see getDescription().
 */
public interface ProgramDescription {
	
	/**
	 * Returns a description of the plan that is generated by the assembler and
	 * also of the arguments if they are available. The description should be simple
	 * text as it may be rendered in different environments (console, web interface, ...).
	 * Typical things that should be included are:
	 * <ul>
	 *   <li>expected input format</li>
	 *   <li>description of output</li>
	 *   <li>description of what is done</li>
	 *   <li>description of arguments to customize plan if available</li>
	 * </ul>
	 * 
	 * @return A description of the program and of its arguments if available.
	 */
	String getDescription();
}
