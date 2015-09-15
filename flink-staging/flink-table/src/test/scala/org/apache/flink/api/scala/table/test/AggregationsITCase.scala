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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.{ExpressionException, Row}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Assert._
import org.junit._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class AggregationsITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = ""
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  def testAggregationTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('_1.sum, '_1.min, '_1.max, '_1.count, '_1.avg).toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "231,1,21,21,11"
  }

  @Test(expected = classOf[ExpressionException])
  def testAggregationOnNonExistingField(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('foo.avg).toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test
  def testWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable
      .select('_1.avg, '_2.avg, '_3.avg, '_4.avg, '_5.avg, '_6.avg, '_7.count)
      .toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "1,1,1,1,1.5,1.5,2"
  }

  @Test
  def testAggregationWithArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable
      .select(('_1 + 2).avg + 2, '_2.count + " THE COUNT").toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "5.5,2 THE COUNT"
  }

  @Test(expected = classOf[ExpressionException])
  def testNonWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("Hello", 1)).toTable
      .select('_1.sum).toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test(expected = classOf[ExpressionException])
  def testNoNestedAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("Hello", 1)).toTable
      .select('_2.sum.sum).toDataSet[Row]

    ds.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = ""
  }

  @Test
  def testAggregationWithNullValues(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements[(Integer, String)](
      (123, "a"), (234, "b"), (345, "c"), (0, "d"))

    implicit val rowInfo: TypeInformation[Row] = new RowTypeInfo(
      Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), Seq("id", "name"))

    val rowDataSet = dataSet.map {
      entry =>
        val row = new Row(2)
        val amount = if (entry._1 > 200) entry._1 else null
        row.setField(0, amount)
        row.setField(1, entry._2)
        row
    }

    val entries = rowDataSet.toTable.select('id.avg, 'id.sum, 'id.count).collect().head
    val mean = entries.productElement(0).toString.toInt
    val sum = entries.productElement(1).toString.toInt
    val count = entries.productElement(2).toString.toInt

    assertEquals(4,count)

    val computedMean = sum / 2
    assertEquals(computedMean, mean)
  }

  @Test
  def testAggregationWhenAllValuesAreNull(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements[(Integer, String)](
      (123, "a"), (234, "b"), (345, "c"), (0, "d"))

    implicit val rowInfo: TypeInformation[Row] = new RowTypeInfo(
      Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO), Seq("id", "name"))

    val rowDataSet = dataSet.map {
      entry =>
        val row = new Row(2)
        row.setField(0, null)
        row.setField(1, entry._2)
        row
    }

    val entries = rowDataSet.toTable.select('id.max).collect().head.productElement(0)
    assertEquals(entries, null)
  }

}
