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
package org.apache.spark.sql

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class PartitionPruningTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop table if exists dpp_table1")
    sql("drop table if exists dpp_table2")
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists dpp_table1")
    sql("drop table if exists dpp_table2")
  }

  test("test partition pruning") {
    sql("create table dpp_table1(col1 int) partitioned by (col2 string) stored as carbondata")
    sql("insert into dpp_table1 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")
    sql("create table dpp_table2(col1 int, col2 string) ")
    sql("insert into dpp_table2 values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')")
    sql("select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
        "where t1.col2=t2.col2 and t2.col1 in (1,2)").show(false)
    sql("explain extended select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
        "where t1.col2=t2.col2 and t2.col1 in (1,2)").show(false)
    sql("select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
        "where t1.col2=t2.col2 and t2.col2 in ('b','c') and t2.col1 in (1,2)").show(false)
    sql("explain extended select t1.col1 from dpp_table1 t1, dpp_table2 t2 " +
        "where t1.col2=t2.col2 and t2.col2 in ('b','c') and t2.col1 in (1,2)").show(false)
  }
}
