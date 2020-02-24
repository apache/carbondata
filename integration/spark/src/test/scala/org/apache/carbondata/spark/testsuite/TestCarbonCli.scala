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
package org.apache.carbondata.spark.testsuite

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCarbonCli extends QueryTest with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    sql("drop table if exists OneRowTable")
    sql("create table OneRowTable(col1 string, col2 string, col3 int, col4 double) STORED AS carbondata")
    sql("insert into OneRowTable select '0.1', 'a.b', 1, 1.2")
  }

  test("CarbonCli table summary") {
    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -a')"),
      true, "## Summary")

    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -v')"),
      true, "## version Details")

    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -s')"),
      true, "## Schema")

    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -t')"),
      true, "## Table Properties")

    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -m')"),
      true, "## Segment")
  }

  test("CarbonCli column details") {
    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd summary -c col1')"),
      true, "## Column Statistics for 'col1'")
  }

  test("CarbonCli benchmark") {
    checkExistence(
      sql("carboncli for table OneRowTable options('-cmd benchmark -c col1')"),
      true, "## Benchmark")
  }

  test("CarbonCli invalid cmd"){

    assert(intercept[AnalysisException] {
      sql("carboncli for table OneRowTable").show()
    }.getMessage().contains("mismatched input 'carboncli'"))

    assert(intercept[Exception] {
      sql("carboncli for table OneRowTable options('')")
    }.getMessage().contains("Missing required option: cmd"))

    checkExistence(sql("carboncli for table OneRowTable options('-cmd test')"),
      true, "command test is not supported")
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists OneRowTable")
  }
}
