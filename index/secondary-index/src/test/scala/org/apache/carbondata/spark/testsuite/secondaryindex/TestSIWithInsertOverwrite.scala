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
package org.apache.carbondata.spark.testsuite.secondaryindex;

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

class TestSIWithInsertOverwrite extends QueryTest with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, Id int, address string) stored as carbondata")
    sql("drop index if exists maintable_si on maintable")
    sql("CREATE INDEX maintable_si  on table maintable (address) as 'carbondata'")
  }

  test("test insert overwrite with SI") {
    sql("insert into maintable select 'nihal',1,'nko'")
    sql("insert into maintable select 'brinjal',2,'valid'")
    sql("insert overwrite table maintable select 'nihal', 1, 'asdfa'")
    checkAnswer(sql("select count(*) from maintable_si WHERE address='nko'"), Seq(Row(0)))
    checkAnswer(sql("select address from maintable_si"), Seq(Row("asdfa")))
  }

  test("test insert overwrite with CTAS and SI") {
    sql("insert into maintable select 'nihal',1,'nko'")
    sql("drop table if exists ctas_maintable")
    sql("CREATE TABLE ctas_maintable " +
      "STORED AS carbondata as select * from maintable")
    sql("CREATE INDEX ctas_maintable_si  on table ctas_maintable (address) as 'carbondata'")
    checkAnswer(sql("select address from ctas_maintable_si"), Seq(Row("nko")))
    sql("insert overwrite table ctas_maintable select 'nihal', 1, 'asdfa'")
    checkAnswer(sql("select count(*) from ctas_maintable_si WHERE address='nko'"), Seq(Row(0)))
    checkAnswer(sql("select address from ctas_maintable_si"), Seq(Row("asdfa")))
    sql("drop index if exists ctas_maintable_si on ctas_maintable")
    sql("drop table if exists ctas_maintable")
  }

  test("test insert overwrite with table having partition with SI") {
    sql("drop table if exists partitionwithSI")
    sql(
      """
        | CREATE TABLE if not exists partitionwithSI (empname String, age int, address string,
        |  year int, month int) PARTITIONED BY (day int)
        | STORED AS carbondata
      """.stripMargin)
    sql("CREATE INDEX partition_si  on table partitionwithSI (address) as 'carbondata'")
    sql("insert into partitionwithSI values('k',2,'some add',2014,1,1)")
    sql("insert overwrite table partitionwithSI values('v',3,'changed add',2014,1,1)")
    checkAnswer(sql("select address from partition_si"), Seq(Row("changed add")))
    sql("drop table if exists partitionwithSI")
  }

  test("test insert overwrite with SI global sort") {
    sql("drop index if exists maintable_si on maintable")
    sql("CREATE INDEX maintable_si  on table maintable (address) as 'carbondata' " +
      "properties('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    sql("insert into maintable select 'nihal',1,'nko'")
    sql("insert into maintable select 'brinjal',2,'valid'")
    sql("insert overwrite table maintable select 'nihal', 1, 'asdfa'")
    checkAnswer(sql("select count(*) from maintable_si WHERE address='nko'"), Seq(Row(0)))
    checkAnswer(sql("select address from maintable_si"), Seq(Row("asdfa")))
  }

  override protected def afterEach(): Unit = {
    sql("drop index if exists maintable_si on maintable")
    sql("drop table if exists maintable")
  }
}
