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

package org.apache.flink.api.java.io;

import java.util.ArrayList;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.spatial.Tile;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;

//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import org.apache.flink.api.java.tuple.*;

/**
 * A builder class to instantiate a data source that parses ENVI files.
 * TODO: Describe parameters
 */
public class EnviReader {

	private final Path path, headerPath;

	private final ExecutionEnvironment executionContext;
	
	// --------------------------------------------------------------------------------------------
	
	public EnviReader(Path filePath, ExecutionEnvironment executionContext) {
		Validate.notNull(filePath, "The file path may not be null.");
		Validate.notNull(executionContext, "The execution context may not be null.");
		
		this.path = filePath;
		this.headerPath = filePath.suffix(".hdr");
		this.executionContext = executionContext;
	}
	
	public EnviReader(String filePath, ExecutionEnvironment executionContext) {
		this(new Path(Validate.notNull(filePath, "The file path may not be null.")), executionContext);
	}
	
	public Path getFilePath() {
		return this.path;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures the delimiter that separates the lines/rows. The linebreak character
	 * ({@code '\n'}) is used by default.
	 * 
	 * @param delimiter The delimiter that separates the rows.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader lineDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}
		
		//this.lineDelimiter = delimiter;
		return this;
	}
	
	/**
	 * Configures the delimiter that separates the fields within a row. The comma character
	 * ({@code ','}) is used by default.
	 * 
	 * @param delimiter The delimiter that separates the fields in one row.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader fieldDelimiter(char delimiter) {
		//this.fieldDelimiter = delimiter;
		return this;
	}
	
	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the boolean
	 * array. The parser will skip over all fields where the boolean value at the corresponding position
	 * in the array is {@code false}. The result contains the fields where the corresponding position in
	 * the boolean array is {@code true}.
	 * The number of fields in the result is consequently equal to the number of times that {@code true}
	 * occurs in the fields array.
	 * 
	 * @param fields The array of flags that describes which fields are to be included and which not.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader includeFields(boolean ... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("The set of included fields must not be null or empty.");
		}
		
		int lastTruePos = -1;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i]) {
				lastTruePos = i;
			}
		}
		
		if (lastTruePos == -1) {
			throw new IllegalArgumentException("The description of fields to parse excluded all fields. At least one fields must be included.");
		}
		if (lastTruePos == fields.length - 1) {
			//this.includedMask = fields;
		} else {
			//this.includedMask = Arrays.copyOfRange(fields, 0, lastTruePos + 1);
		}
		return this;
	}

	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
	 * positions in the string (read from position 0 to its length) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the length of the mask string
	 * The parser will skip over all fields where the character at the corresponding position
	 * in the string is {@code '0'}, {@code 'F'}, or {@code 'f'} (representing the value
	 * {@code false}). The result contains the fields where the corresponding position in
	 * the boolean array is {@code '1'}, {@code 'T'}, or {@code 't'} (representing the value {@code true}).
	 * 
	 * @param mask The string mask defining which fields to include and which to skip.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader includeFields(String mask) {
		boolean[] includedMask = new boolean[mask.length()];
		
		for (int i = 0; i < mask.length(); i++) {
			char c = mask.charAt(i);
			if (c == '1' || c == 'T' || c == 't') {
				includedMask[i] = true;
			} else if (c != '0' && c != 'F' && c != 'f') {
				throw new IllegalArgumentException("Mask string may contain only '0' and '1'.");
			}
		}
		
		return includeFields(includedMask);
	}
	
	/**
	 * Configures which fields of the CSV file should be included and which should be skipped. The
	 * bits in the value (read from least significant to most significant) define whether the field at
	 * the corresponding position in the CSV schema should be included.
	 * parser will look at the first {@code n} fields, where {@code n} is the position of the most significant
	 * non-zero bit.
	 * The parser will skip over all fields where the character at the corresponding bit is zero, and
	 * include the fields where the corresponding bit is one.
	 * <p>
	 * Examples:
	 * <ul>
	 *   <li>A mask of {@code 0x7} would include the first three fields.</li>
	 *   <li>A mask of {@code 0x26} (binary {@code 100110} would skip the first fields, include fields
	 *       two and three, skip fields four and five, and include field six.</li>
	 * </ul>
	 * 
	 * @param mask The bit mask defining which fields to include and which to skip.
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader includeFields(long mask) {
		if (mask == 0) {
			throw new IllegalArgumentException("The description of fields to parse excluded all fields. At least one fields must be included.");
		}
		
		ArrayList<Boolean> fields = new ArrayList<Boolean>();

		while (mask != 0) {
			fields.add((mask & 0x1L) != 0);
			mask >>>= 1;
		}
		
		boolean[] fieldsArray = new boolean[fields.size()];
		for (int i = 0; i < fieldsArray.length; i++) {
			fieldsArray[i] = fields.get(i);
		}
		
		return includeFields(fieldsArray);
	}

	/**
	 * Sets the CSV reader to ignore the first line. This is useful for files that contain a header line.
	 * 
	 * @return The CSV reader instance itself, to allow for fluent function chaining.
	 */
	public EnviReader ignoreFirstLine() {
		//skipFirstLineAsHeader = true;
		return this;
	}
	/**
	 * Configures the reader to read the CSV data and parse it to the given type. The type must be a subclass of
	 * {@link Tuple}. The type information for the fields is obtained from the type class. The type
	 * consequently needs to specify all generic field types of the tuple.
	 * 
	 * @param targetType The class of the target type, needs to be a subclass of Tuple.
	 * @return The DataSet representing the parsed CSV data.
	 */
	public <T extends Tuple> DataSource<T> tupleType(Class<T> targetType) {
		Validate.notNull(targetType, "The target type class must not be null.");
		if (!Tuple.class.isAssignableFrom(targetType)) {
			throw new IllegalArgumentException("The target type must be a subclass of " + Tuple.class.getName());
		}
		
		@SuppressWarnings("unchecked")
		TupleTypeInfo<T> typeInfo = (TupleTypeInfo<T>) TypeExtractor.createTypeInfo(targetType);
		CsvInputFormat<T> inputFormat = new CsvInputFormat<T>(path);
		
		Class<?>[] classes = new Class<?>[typeInfo.getArity()];
		for (int i = 0; i < typeInfo.getArity(); i++) {
			classes[i] = typeInfo.getTypeAt(i).getTypeClass();
		}
		
		configureInputFormat(inputFormat, classes);
		return new DataSource<T>(executionContext, inputFormat, typeInfo, Utils.getCallLocationName());
	}
	
	// --------------------------------------------------------------------------------------------
	// Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private void configureInputFormat(CsvInputFormat<?> format, Class<?>... types) {
	/*	format.setDelimiter(this.lineDelimiter);
		format.setFieldDelimiter(this.fieldDelimiter);
		format.setSkipFirstLineAsHeader(skipFirstLineAsHeader);
		if (this.includedMask == null) {
			format.setFieldTypes(types);
		} else {
			format.setFields(this.includedMask, types);
		}*/
	}

	/**
	 * Specifies the types for the CSV fields. This method parses the CSV data to a 1-tuple
	 * which has fields of the specified types.
	 * This method is overloaded for each possible length of the tuples to support type safe
	 * creation of data sets through CSV parsing.
	 *
	 * @param type0 The type of CSV field 0 and the type of field 0 in the returned tuple type.
	 * @return The {@link org.apache.flink.api.java.DataSet} representing the parsed CSV data.
	 */
	public DataSource<Tile> types() {
		EnviInputFormat<Tile> inputFormat = new EnviInputFormat<Tile>(path);
		//configureInputFormat(inputFormat, null);
		return new DataSource<Tile>(executionContext, inputFormat, null, Utils.getCallLocationName());
	}
}
