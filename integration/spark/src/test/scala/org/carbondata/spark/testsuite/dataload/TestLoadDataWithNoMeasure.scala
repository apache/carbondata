/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading with hive syntax and old syntax
 *
 */
class TestLoadDataWithNoMeasure extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE nomeasureTest (empno String, doj String) STORED BY 'org.apache.carbondata.format'")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val testData = currentDirectory + "/src/test/resources/datasample.csv"
    sql("LOAD DATA LOCAL INPATH '"+ testData +"' into table nomeasureTest")
  }

  test("test data loading and validate query output") {

    checkAnswer(
      sql("select empno from nomeasureTest"),
      Seq(Row("11"),Row("12"))
    )
  }

  override def afterAll {
    sql("drop table nomeasureTest")
  }
}
