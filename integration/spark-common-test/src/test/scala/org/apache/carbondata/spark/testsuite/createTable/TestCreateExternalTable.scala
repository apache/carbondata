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

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestCreateExternalTable extends QueryTest with BeforeAndAfterAll {

  var originDataPath: String = _

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS origin")
    sql("drop table IF EXISTS rsext")
    sql("drop table IF EXISTS rstest1")
    // create carbon table and insert data
    sql("CREATE TABLE origin(key INT, value STRING) STORED AS carbondata")
    sql("INSERT INTO origin select 100,'spark'")
    sql("INSERT INTO origin select 200,'hive'")
    originDataPath = s"$storeLocation/origin"
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS origin")
    sql("drop table IF EXISTS rsext")
    sql("drop table IF EXISTS rstest1")
  }

  test("create external table with existing files") {
    assert(new File(originDataPath).exists())
    sql("DROP TABLE IF EXISTS source")
    if (System
          .getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT).equalsIgnoreCase("true") ||
        CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT).equalsIgnoreCase("true")) {

      intercept[Exception] {
        // create external table with existing files
        sql(
          s"""
             |CREATE EXTERNAL TABLE source
             |STORED AS carbondata
             |LOCATION '$storeLocation/origin'
       """.stripMargin)
      }
    } else {

      // create external table with existing files
      sql(
        s"""
           |CREATE EXTERNAL TABLE source
           |STORED AS carbondata
           |LOCATION '$storeLocation/origin'
       """.stripMargin)
      checkAnswer(sql("SELECT count(*) from source"), sql("SELECT count(*) from origin"))

      checkExistence(sql("describe formatted source"), true, storeLocation + "/origin")

      val carbonTable = CarbonEnv.getCarbonTable(None, "source")(sqlContext.sparkSession)
      assert(carbonTable.isExternalTable)

      sql("DROP TABLE IF EXISTS source")

      // DROP TABLE should not delete data
      assert(new File(originDataPath).exists())

    }
  }

  ignore("create external table with specified schema") {
    assert(new File(originDataPath).exists())
    sql("DROP TABLE IF EXISTS source")
    val ex = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE EXTERNAL TABLE source (key INT)
           |STORED AS carbondata
           |LOCATION '$storeLocation/origin'
     """.stripMargin)
    }
    assert(ex.message.contains("Schema must not be specified for external table"))

    sql("DROP TABLE IF EXISTS source")

    // DROP TABLE should not delete data
    assert(new File(originDataPath).exists())
  }

  test("create external table with empty folder") {
    val exception = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE EXTERNAL TABLE source
           |STORED AS carbondata
           |LOCATION './nothing'
         """.stripMargin)
    }
    assert(exception.getMessage().contains("Unable to infer the schema"))
  }

  test("create external table with CTAS") {
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        """
          |CREATE EXTERNAL TABLE source
          |STORED AS carbondata
          |LOCATION './nothing'
          |AS
          | SELECT * FROM origin
        """.stripMargin)
    }
    assert(exception.getMessage().contains("Create external table as select"))
  }
  test("create external table with post schema resturcture") {
    sql("create table rstest1 (c1 string,c2 int) STORED AS carbondata")
    sql("Alter table rstest1 drop columns(c2)")
    sql(
      "Alter table rstest1 add columns(c4 string) TBLPROPERTIES( " +
      "'DEFAULT.VALUE.c4'='def')")
    sql(s"""CREATE EXTERNAL TABLE rsext STORED AS carbondata LOCATION '$storeLocation/rstest1'""")
    sql("insert into rsext select 'shahid', 1")
    checkAnswer(sql("select * from rstest1"),  sql("select * from rsext"))
  }

}
