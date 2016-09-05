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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadDataGeneral extends QueryTest with BeforeAndAfterAll {

  var currentDirectory: String = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS loadtest")
    sql(
      """
        | CREATE TABLE loadtest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
  }

  test("test data loading CSV file") {
    val testData = currentDirectory + "/src/test/resources/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading GZIP compressed CSV file") {
    val testData = currentDirectory + "/src/test/resources/sample.csv.gz"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(8))
    )
  }

  override def afterAll {
    sql("DROP TABLE loadtest")
  }
}
