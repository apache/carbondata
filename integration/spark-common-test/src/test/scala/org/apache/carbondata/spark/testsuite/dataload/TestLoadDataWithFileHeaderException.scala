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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException

class TestLoadDataWithFileHeaderException extends QueryTest with BeforeAndAfterAll{
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
           """)
  }

  test("test load data both file and ddl without file header exception") {
    val e = intercept[CarbonDataLoadingException] {
      sql(
        s"""LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3""")
    }
    assert(e.getMessage.contains(
      "CSV header in input file is not proper. Column names in schema and csv header are not the same."))
  }

  test("test load data ddl provided wrong file header exception") {
    val e = intercept[CarbonDataLoadingException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
           options('fileheader'='no_column')
           """)
    }
    assert(e.getMessage.contains("CSV header in DDL is not proper. Column names in schema and CSV header are not the same"))
  }

  test("test load data with wrong header , but without fileheader") {
    val e = intercept[InvalidLoadOptionException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' into table t3
           options('header'='abc')
           """)
    }
    assert(e.getMessage.contains("'header' option should be either 'true' or 'false'"))
  }

  test("test load data with wrong header and fileheader") {
    val e = intercept[InvalidLoadOptionException] {
      sql(
        s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
         options('header'='', 'fileheader'='ID,date,country,name,phonetype,serialname,salary')
         """)
    }
    assert(e.getMessage.contains("'header' option should be either 'true' or 'false'"))
  }

  test("test load data with header=false, but without fileheader") {
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
         options('header'='False')
         """)
  }

  test("test load data with header=false and fileheader") {
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
         options('header'='false', 'fileheader'='ID,date,country,name,phonetype,serialname,salary')
         """)
  }

  test("test load data with header=false and wrong fileheader") {
    val e = intercept[Exception] {
      sql(
        s"""
        LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
        options('header'='false', 'fileheader'='ID1,date2,country,name,phonetype,serialname,salary')
        """)
    }
    assert(e.getMessage.contains("CSV header in DDL is not proper. Column names in schema and CSV header are not the same"))
  }

  test("test load data with header=true, but without fileheader") {
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' into table t3
         options('header'='True')
         """)
  }

  test("test load data with header=true and fileheader") {

    val e = intercept[Exception] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' into table t3
           options('header'='true', 'fileheader'='ID,date,country,name,phonetype,serialname,salary')
           """)
    }
    assert(e.getMessage.contains("When 'header' option is true, 'fileheader' option is not required."))
  }

  test("test load data with header=true and wrong fileheader") {

    val e = intercept[Exception] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' into table t3
           options('header'='true', 'fileheader'='ID1,date1,country,name,phonetype,serialname,salary')
           """)
    }
    assert(e.getMessage.contains("When 'header' option is true, 'fileheader' option is not required."))
  }

  test("test load data without header and fileheader") {
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' into table t3
         """)
  }

  test("test load data without header, but with fileheader") {
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' into table t3
         options('fileheader'='ID,date,country,name,phonetype,serialname,salary')
         """)
  }


  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}
