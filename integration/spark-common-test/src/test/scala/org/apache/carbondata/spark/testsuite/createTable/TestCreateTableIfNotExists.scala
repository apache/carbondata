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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestCreateTableIfNotExists extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop table if exists test")
    sql("drop table if exists sourceTable")
    sql("drop table if exists targetTable")
  }

  test("test create table if not exists") {
    sql("create table test(a int, b string) stored by 'carbondata'")
    try {
      // table creation should be successful
      sql("create table if not exists test(a int, b string) stored by 'carbondata'")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  test("test blocking of create table like command") {
    sql("create table sourceTable(name string) stored by 'carbondata'")
    val exception = intercept[MalformedCarbonCommandException] {
      sql("create table targetTable like sourceTable")
    }
    assert(exception.getMessage.contains("Operation not allowed, when source table is carbon table"))
  }

  override def afterAll {
    sql("use default")
    sql("drop table if exists test")
    sql("drop table if exists sourceTable")
    sql("drop table if exists targetTable")
  }

}
