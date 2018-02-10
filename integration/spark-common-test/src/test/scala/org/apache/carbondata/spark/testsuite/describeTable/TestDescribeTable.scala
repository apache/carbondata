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
package org.apache.carbondata.spark.testsuite.describeTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * test class for describe table .
  */
class TestDescribeTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS Desc1")
    sql("DROP TABLE IF EXISTS Desc2")
    sql("CREATE TABLE Desc1(Dec1Col1 String, Dec1Col2 String, Dec1Col3 int, Dec1Col4 double) stored by 'carbondata'")
    sql("DESC Desc1")
    sql("DROP TABLE Desc1")
    sql("CREATE TABLE Desc1(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) stored by 'carbondata'")
    sql("CREATE TABLE Desc2(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) stored by 'carbondata'")
  }

  test("test describe table") {
    checkAnswer(sql("DESC Desc1"), Seq(Row("dec2col1","bigint",null),
      Row("dec2col2","string",null),
      Row("dec2col3","bigint",null),
      Row("dec2col4","decimal(10,0)",null)))
  }

  test("test describe formatted table") {
    checkExistence(sql("DESC FORMATTED Desc1"), true,
      "Table Block Size")
  }

  test("test describe formatted table desc1") {

    val resultCol = Seq("", "", "##Detailed Column property", "##Detailed Table Information", "ADAPTIVE", "CARBON Store Path", "Comment", "Database Name", "Last Update Time",
    "SORT_COLUMNS", "SORT_SCOPE", "Streaming", "Table Block Size", "Table Data Size", "Table Index Size", "Table Name", "dec2col1", "dec2col2", "dec2col3", "dec2col4")
    val resultRow: Seq[Row] = resultCol map(propName => Row(f"$propName%-36s"))
    checkAnswer(sql("desc formatted DESC1").select("col_name"), resultRow)
    assert(sql("desc formatted desc1").count() == 20)
  }

  override def afterAll: Unit = {
    sql("DROP TABLE Desc1")
    sql("DROP TABLE Desc2")
  }

}
