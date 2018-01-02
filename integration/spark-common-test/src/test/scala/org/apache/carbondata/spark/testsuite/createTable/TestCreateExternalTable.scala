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

package org.apache.carbondata.spark.testsuite.createTable

import java.io.File

import org.apache.spark.sql.{AnalysisException, CarbonEnv}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateExternalTable extends QueryTest with BeforeAndAfterAll {

  var originDataPath: String = _

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS origin")
    // create carbon table and insert data
    sql("CREATE TABLE origin(key INT, value STRING) STORED BY 'carbondata'")
    sql("INSERT INTO origin select 100,'spark'")
    sql("INSERT INTO origin select 200,'hive'")
    originDataPath = s"$storeLocation/origin"
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS origin")
  }

  test("create external table with existing files") {
    assert(new File(originDataPath).exists())
    sql("DROP TABLE IF EXISTS source")

    // create external table with existing files
    sql(
      s"""
         |CREATE EXTERNAL TABLE source
         |STORED BY 'carbondata'
         |LOCATION '$storeLocation/origin'
       """.stripMargin)
    checkAnswer(sql("SELECT count(*) from source"), sql("SELECT count(*) from origin"))

    val carbonTable = CarbonEnv.getCarbonTable(None, "source")(sqlContext.sparkSession)
    assert(carbonTable.isExternalTable)
    
    sql("DROP TABLE IF EXISTS source")

    // DROP TABLE should not delete data
    assert(new File(originDataPath).exists())
  }

  test("create external table with empty folder") {
    val exception = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE EXTERNAL TABLE source
           |STORED BY 'carbondata'
           |LOCATION './nothing'
         """.stripMargin)
    }
    assert(exception.getMessage().contains("Invalid table path provided"))
  }

  test("create external table with CTAS") {
    val exception = intercept[AnalysisException] {
      sql(
        """
          |CREATE EXTERNAL TABLE source
          |STORED BY 'carbondata'
          |LOCATION './nothing'
          |AS
          | SELECT * FROM origin
        """.stripMargin)
    }
    assert(exception.getMessage().contains("Create external table as select"))
  }

}
