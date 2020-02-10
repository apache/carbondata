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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class DropTableTest extends QueryTest with BeforeAndAfterAll {

  test("test to drop parent table with all indexes") {
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("show tables in cd").show()
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) AS 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) AS 'carbondata'")
    sql("show tables in cd").show()
    sql("drop table cd.t1")
    assert(sql("show tables in cd").collect()
      .forall(row => row.getString(1) != "i2" && row != Row("cd", "i1", "false") && row != Row("cd", "t1", "false")))
  }


  /*test("test to drop one index table out of two"){
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("show tables in cd").show()
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) as 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) as 'carbondata'")
    sql("show tables in cd").show()
    sql("drop index i1 on cd.t1")
    sql("show tables in cd").show()
    sql("select * from i2").show()
  }*/

  test("test to drop index tables") {
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) AS 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) AS 'carbondata'")
    sql("show tables in cd").show()
    sql("drop index i1 on cd.t1")
    sql("drop index i2 on cd.t1")
    assert(sql("show tables in cd").collect()
      .forall(row => !row.getString(1).equals("i1") && !row.getString(1).equals("i2") && row.getString(1).equals("t1")))
    assert(sql("show indexes on cd.t1").collect().isEmpty)
  }

  test("test drop index command") {
    sql("drop table if exists testDrop")
    sql("create table testDrop (a string, b string, c string) STORED AS carbondata")
    val exception = intercept[MalformedCarbonCommandException] {
      sql("drop index indTestDrop on testDrop")
    }
    assert(exception.getMessage.contains("Index table [default.indtestdrop] does not exist on parent table [default.testdrop]"))
    sql("drop table if exists testDrop")
  }
}
