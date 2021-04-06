/*
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
 */

package org.apache.carbondata.examplesCI

import java.io.File

import org.apache.spark.sql.SparkSqlAdapter
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples._
import org.apache.carbondata.examples.sdk.CarbonReaderExample
import org.apache.carbondata.examples.sql.JavaCarbonSessionExample

/**
 * Test suite for examples
 */

class RunExamples extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession

  override def beforeAll: Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val targetLoc = s"$rootPath/examples/spark/target"

    System.setProperty("derby.system.home", s"$targetLoc")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  override def afterAll {
    sql("USE default")

  }

  test("AlterTableExample") {
    AlterTableExample.exampleBody(spark)
  }

  test("CarbonDataFrameExample") {
    CarbonDataFrameExample.exampleBody(spark)
  }

  test("CarbonSessionExample") {
    CarbonSessionExample.exampleBody(spark)
  }

  test("JavaCarbonSessionExample") {
    JavaCarbonSessionExample.exampleBody(spark)
  }

  test("CarbonSortColumnsExample") {
    CarbonSortColumnsExample.exampleBody(spark)
  }

  test("CaseClassDataFrameAPIExample") {
    CaseClassDataFrameAPIExample.exampleBody(spark)
  }

  test("DataFrameComplexTypeExample") {
    DataFrameComplexTypeExample.exampleBody(spark)
  }

  test("DataManagementExample") {
    DataManagementExample.exampleBody(spark)
  }

  test("DataUpdateDeleteExample") {
    DataUpdateDeleteExample.exampleBody(spark)
  }

  test("QuerySegmentExample") {
    QuerySegmentExample.exampleBody(spark)
  }

  test("StandardPartitionExample") {
    StandardPartitionExample.exampleBody(spark)
  }

  test("TableLevelCompactionOptionExample") {
    TableLevelCompactionOptionExample.exampleBody(spark)
  }

  // Below test case ignored due to the Deadlock in spark code
  // TODO: To be fixed when spark removes deadlock in opensource code.
  ignore("LuceneIndexExample") {
    LuceneIndexExample.exampleBody(spark)
  }

  test("ExternalTableExample") {
    ExternalTableExample.exampleBody(spark)
  }

  test("CarbonReaderExample") {
    CarbonReaderExample.main(null)
  }

  test("DirectSQLExample") {
    DirectSQLExample.exampleBody(spark)
  }

  // Ignoring because HiveExample depends on Hadoop ENV, but CI doesn't meet the running conditions.
  ignore("HiveExample") {
    SparkSqlAdapter.initSparkSQL()
    HiveExample.createCarbonTable(spark)
    HiveExample.readFromHive
  }
}
