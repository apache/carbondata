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

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, Row}
import org.scalatest.BeforeAndAfterAll

class TestCarbonJoin extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
  }

  test("test broadcast FilterPushDown with alias") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS ptable")
    sql("DROP TABLE IF EXISTS result")
    sql("create table if not exists table1 (ID string) STORED AS carbondata")
    sql("insert into table1 select 'animal'")
    sql("insert into table1 select 'person'")
    sql("create table ptable(pid string) stored as parquet")
    sql("insert into table ptable values('person')")

    val df2 = sql("select id as f91 from table1")
    df2.createOrReplaceTempView("tempTable_2")
    sql("select t1.f91 from tempTable_2 t1, ptable t2 where t1.f91 = t2.pid ").write.saveAsTable("result")
    checkAnswer(sql("select count(*) from result"), Seq(Row(1)))
    checkAnswer(sql("select * from result"), Seq(Row("person")))

    sql("DROP TABLE IF EXISTS result")
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS patble")
  }

  test("test broadcast FilterPushDown with alias with SI") {
    sql("drop index if exists cindex on ctable")
    sql("DROP TABLE IF EXISTS ctable")
    sql("DROP TABLE IF EXISTS ptable")
    sql("DROP TABLE IF EXISTS result")
    sql("create table if not exists ctable (type int, id1 string, id string) stored as " +
        "carbondata")
    sql("create index cindex on table ctable (id) AS 'carbondata'")
    sql("insert into ctable select 0, 'animal1', 'animal'")
    sql("insert into ctable select 1, 'person1', 'person'")
    sql("create table ptable(pid string) stored as parquet")
    sql("insert into table ptable values('person')")
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), "ctable")(sqlContext.sparkSession)
    val df2 = sql("select id as f91 from ctable")
    df2.createOrReplaceTempView("tempTable_2")
    sql("select t1.f91 from tempTable_2 t1, ptable t2 where t1.f91 = t2.pid ").write
      .saveAsTable("result")
    checkAnswer(sql("select count(*) from result"), Seq(Row(1)))
    checkAnswer(sql("select * from result"), Seq(Row("person")))
    sql("DROP TABLE IF EXISTS result")
    sql("drop index if exists cindex on ctable")
    sql("DROP TABLE IF EXISTS ctable")
    sql("DROP TABLE IF EXISTS patble")
  }
}
