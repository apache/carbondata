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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadDataWithFileHeaderException extends QueryTest with BeforeAndAfterAll{
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)
  }

  test("test load data both file and ddl without file header exception") {
    try {
      sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/windows.csv' into table t3
           """)
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.equals("DataLoad failure: CSV File provided is not proper. " +
          "Column names in schema and csv header are not same. CSVFile Name : windows.csv"))
    }
  }

  test("test load data ddl provided  wrong file header exception") {
    try {
      sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/windows.csv' into table t3
           options('fileheader'='no_column')
           """)
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.equals("DataLoad failure: CSV header provided in DDL is not proper. " +
          "Column names in schema and CSV header are not the same."))
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}
