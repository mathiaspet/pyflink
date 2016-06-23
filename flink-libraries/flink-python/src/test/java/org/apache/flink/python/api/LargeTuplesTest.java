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
package org.apache.flink.python.api;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

import java.util.Arrays;


public class LargeTuplesTest {


	/**
	 * Simple test to serialize and deserialize a &quot;large&quot; string and transfer
	 * it in chunks.
	 */
	@Test
	public void testTransferLargeString(){


		String largeString = "Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE\n" +
			" * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file\n" +
			" * to you under the Apache License, Version 2.0 (the \"License\"); you may not use this file except in compliance with the\n" +
			" * License. You may obtain a copy of the License at\n" +
			" *\n" +
			" * http://www.apache.org/licenses/LICENSE-2.0\n" +
			" *\n" +
			" * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on\n" +
			" * an \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the\n" +
			" * specific language governing permissions and limitations under the License.";


		byte[] largeStringBytes = largeString.getBytes();
		byte[] receivingBuffer = new byte[largeStringBytes.length];

		int numTransfers = (int)(largeStringBytes.length / 100);
		if(largeStringBytes.length % 100 != 0) {
			++numTransfers;
		}

		assertThat(numTransfers, is(equalTo(8)));

		for(int i = 0; i < numTransfers; i++){
			//first approach: fill buffer byte by byte
			//second approach: copy buffer in chunks

			//normal case: fill buffer with 100-er chunk
			int currStart = i *100;
			int currEnd = (i+1)*100;
			if(currEnd <= receivingBuffer.length) {
				System.arraycopy(largeStringBytes, currStart, receivingBuffer, currStart, 100);
			}else {
				int length = largeStringBytes.length - currStart;
				System.arraycopy(largeStringBytes, currStart, receivingBuffer, currStart, length);
			}
			//last incomplete chunk

		}
		String newString = new String(receivingBuffer);
		assertThat(newString, is(equalToIgnoringCase(largeString)));
	}
}
