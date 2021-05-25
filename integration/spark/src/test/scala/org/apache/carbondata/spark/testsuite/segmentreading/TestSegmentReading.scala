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

package org.apache.carbondata.spark.testsuite.segmentreading

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.view.rewrite.TestUtil

/**
 * Created by rahul on 19/9/17.
 */
class TestSegmentReading extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    cleanAllTable()
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data1.csv' INTO TABLE carbon_table OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  private def cleanAllTable(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists carbon_table_join")
    sql("drop table if exists carbon_table_update")
    sql("drop table if exists carbon_table_delete")
    sql("drop table if exists carbon_table_show_seg")
    sql("drop table if exists carbon_table_compact")
    sql("drop table if exists carbon_table_alter")
    sql("drop table if exists carbon_table_alter_new")
    sql("drop table if exists carbon_table_recreate")
  }

  override def afterAll(): Unit = {
    cleanAllTable()
    // reset
    defaultConfig()
    sqlContext.sparkSession.conf.unset("carbon.input.segments.default.carbon_table")
  }

  test("test SET -V for segment reading property") {
    sql("SET -v").collect()
    try {
      checkExistence(sql("SET -v"), true, "Property to configure the list of segments to query.")
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test count(*) for segment reading property") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test SET propertyname for segment reading property") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("SET carbon.input.segments.default.carbon_table"),
        Seq(Row("carbon.input.segments.default.carbon_table", "1"))
      )
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("set valid segments and query from table") {
    try {
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(20)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
      sql("SET carbon.input.segments.default.carbon_table=*")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(20)))
      sql("SET carbon.input.segments.default.carbon_table=0")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test Multiple times set segment") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=0")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
      sql("SET carbon.input.segments.default.carbon_table=1,0,1")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(20)))
      sql("SET carbon.input.segments.default.carbon_table=2,0")
      checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))
      val trapped = intercept[Exception] {
        sql("SET carbon.input.segments.default.carbon_table=2,a")
      }
      val trappedAgain = intercept[Exception] {
        sql("SET carbon.input.segments.default.carbon_table=,")
      }
      assert(trapped.getMessage
        .equalsIgnoreCase(
          "carbon.input.segments.<database_name>.<table_name> value range is not valid"))
      assert(trappedAgain.getMessage.equalsIgnoreCase(
        "carbon.input.segments.<database_name>.<table_name> value can't be empty."))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test filter with segment reading") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=*")
      checkAnswer(sql("select count(empno) from carbon_table where empno = 15"), Seq(Row(2)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select count(empno) from carbon_table where empno = 15"), Seq(Row(1)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test group by with segment reading") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=*")
      checkAnswer(
        sql("select empno,count(empname) from carbon_table where empno = 15 group by empno"),
        Seq(Row(15, 2)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(
        sql("select empno,count(empname) from carbon_table where empno = 15 group by empno"),
        Seq(Row(15, 1)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test join with segment reading") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("drop table if exists carbon_table_join")
      sql("create table carbon_table_join(empno int, empname String, designation String, doj " +
          "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, " +
          "deptname String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
          "attendance int,utilization int,salary int) STORED AS carbondata")
      sql("insert into carbon_table_join select * from carbon_table").collect()
      checkAnswer(
        sql("select count(a.empno) from carbon_table a " +
            "inner join carbon_table_join b on a.empno = b.empno"),
        Seq(Row(22)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(
        sql("select count(a.empno) from carbon_table a " +
            "inner join carbon_table_join b on a.empno = b.empno"),
        Seq(Row(11)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test aggregation with segment reading") {
    try {
      sql("SET carbon.input.segments.default.carbon_table=*")
      checkAnswer(sql("select sum(empno) from carbon_table"), Seq(Row(1411)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select sum(empno) from carbon_table"), Seq(Row(1256)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test update query with segment reading") {
    try {
      sql("drop table if exists carbon_table_update")
      sql(
        "create table carbon_table_update(empno int, empname String, designation String, doj " +
        "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
        "String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
        "int,utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_update OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data1.csv' INTO TABLE carbon_table_update OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql("SET carbon.input.segments.default.carbon_table=1")
      intercept[MalformedCarbonCommandException]{
        sql("update carbon_table_update a set(a.empname) = " +
            "(select b.empname from carbon_table b where a.empno=b.empno)").collect()
      }
      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("SET carbon.input.segments.default.carbon_table_update=1")
      intercept[MalformedCarbonCommandException]{
        sql("update carbon_table_update a set(a.empname) = " +
            "(select b.empname from carbon_table b where a.empno=b.empno)").collect()
      }
      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("SET carbon.input.segments.default.carbon_table_update=*")
      checkAnswer(sql("select count(*) from carbon_table_update where empname='rahul'"),
        Seq(Row(0)))
      sql("update carbon_table_update a set(a.empname) = ('rahul')").collect()
      checkAnswer(sql("select count(*) from carbon_table_update where empname='rahul'"),
        Seq(Row(20)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test delete query with segment reading") {
    try {
      sql("drop table if exists carbon_table_delete")
      sql(
        "create table carbon_table_delete(empno int, empname String, designation String, doj " +
        "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
        "String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
        "int,utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_delete OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_delete OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("SET carbon.input.segments.default.carbon_table=1")
      intercept[MalformedCarbonCommandException]{
        sql("delete from carbon_table_delete " +
            "where empno IN (select empno from carbon_table where empname='ayushi')").collect()
      }
      sql("SET carbon.input.segments.default.carbon_table_delete=1")
      intercept[MalformedCarbonCommandException]{
        sql("delete from carbon_table_delete " +
            "where empno IN (select empno from carbon_table where empname='ayushi')").collect()
      }

      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("SET carbon.input.segments.default.carbon_table_delete=*")
      checkAnswer(sql("select count(*) from carbon_table_delete"), Seq(Row(20)))
      sql("delete from carbon_table_delete " +
          "where empno IN (select empno from carbon_table where empname='ayushi')").collect()
      checkAnswer(sql("select count(*) from carbon_table_delete"), Seq(Row(18)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test show segments") {
    try {
      sql("drop table if exists carbon_table_show_seg")
      sql(
        "create table carbon_table_show_seg(empno int, empname String, designation String, doj " +
        "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
        "String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
        "int,utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_show_seg
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_show_seg
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql("alter table carbon_table_show_seg compact 'major'")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_show_seg
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      val df = sql("SHOW SEGMENTS for table carbon_table_show_seg " +
                   "as select * from carbon_table_show_seg_segments")
      val col = df.collect().map{
        row => Row(row.getString(0), row.getString(1), row.getString(7))
      }.toSeq
      assert(col.equals(Seq(
        Row("2", "Success", "NA"),
        Row("1", "Compacted", "0.1"),
        Row("0.1", "Success", "NA"),
        Row("0", "Compacted", "0.1")
      )))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test segment reading after compaction") {
    sql("drop table if exists carbon_table_compact")
    sql(
      "create table carbon_table_compact(empno int, empname String, designation String, doj " +
      "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String,projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_compact OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_compact OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table carbon_table_compact compact 'major'")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_compact OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    checkAnswer(sql("select count(*) from carbon_table_compact"), Seq(Row(30)))
    sql(" SET carbon.input.segments.default.carbon_table_compact=0.1")
    checkAnswer(sql("select count(*) from carbon_table_compact"), Seq(Row(20)))
  }
  test("set segment id then alter table name and check select query") {
    try {
      sql("drop table if exists carbon_table_alter")
      sql("drop table if exists carbon_table_alter_new")
      sql(
        "create table carbon_table_alter(empno int, empname String, designation String, doj " +
        "Timestamp," +

        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_alter OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_alter OPTIONS
            |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      checkAnswer(sql("select count(*) from carbon_table_alter"),
        Seq(Row(20)))
      sql(
        "SET carbon.input.segments.default.carbon_table_alter=1")
      checkAnswer(sql(
        "select count(*) from carbon_table_alter"), Seq(Row(10)))
      sql(
        "alter table carbon_table_alter rename to carbon_table_alter_new")
      checkAnswer(sql(
        "select count(*) from carbon_table_alter_new")
        , Seq(Row(20)))
    }
    finally {
      sql(
        "SET carbon.input.segments.default.carbon_table=*")
    }
  }

  test("drop and recreate table to check segment reading") {
    try {
      sql("drop table if exists carbon_table_recreate")
      sql(
        "create table carbon_table_recreate(empno int, empname String, designation String, doj " +
        "Timestamp," +

        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_recreate
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_recreate
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      checkAnswer(sql("select count(*) from carbon_table_recreate"),
        Seq(Row(20)))
      sql(
        "SET carbon.input.segments.default.carbon_table_recreate=1")
      checkAnswer(sql(
        "select count(*) from carbon_table_recreate"), Seq(Row(10)))
      sql("drop table if exists carbon_table_recreate")
      sql(
        "create table carbon_table_recreate(empno int, empname String, designation String, doj " +
        "Timestamp," +

        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED AS carbondata")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_recreate
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_recreate
            | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".
          stripMargin)
      checkAnswer(sql(
        "select count(*) from carbon_table_recreate"), Seq(Row(10)))
    }
    finally {
      sql(
        "SET carbon.input.segments.default.carbon_table=*")
    }
  }
  test("test with the adaptive execution") {
    sql("set spark.sql.adaptive.enabled=true")

    sql("SET carbon.input.segments.default.carbon_table=1")
    checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))

    // segment doesn't exist
    sql("SET carbon.input.segments.default.carbon_table=5")
    checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(0)))

    sql("SET carbon.input.segments.default.carbon_table=1")
    checkAnswer(sql("select count(*) from carbon_table"), Seq(Row(10)))

    sql("set spark.sql.adaptive.enabled=false")
  }

  test("Read marked for delete segments") {
    sql("drop table if exists carbonTable")
    sql(
      "create table carbonTable(a string, b int, c string) stored as carbondata ")
    sql("insert into carbonTable values ('k',1,'k'), ('k',1,'b')")
    sql("insert into carbonTable values ('a',2,'a')")
    sql("insert into carbonTable values ('b',2,'b'),('b',2,'b')")
    sql("insert into carbonTable values ('c',2,'c')")

    sql("delete from table carbonTable where segment.id in (0,3)")
    sql("set carbon.input.segments.default.carbonTable = 0,2,3")

    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(2)))
  }

  test("Read marked for delete segments after SI creation") {
    sql("drop table if exists carbonTable")
    sql(
      "create table carbonTable(a string, b int, c string) stored as carbondata ")
    sql("insert into carbonTable values ('k',1,'k'), ('k',1,'b')")
    sql("insert into carbonTable values ('a',2,'a')")
    sql("insert into carbonTable values ('b',2,'b'),('b',2,'b')")
    sql("insert into carbonTable values ('c',2,'c')")

    sql("drop index if exists indextable1 on carbonTable")
    sql("create index indextable1 on table carbonTable (c) AS 'carbondata'")

    sql("delete from table carbonTable where segment.id in (0,3)")
    sql("set carbon.input.segments.default.carbonTable = 0,2,3")

    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(2)))
  }

  test("Read compacted segments") {
    sql("drop table if exists carbonTable")
    sql(
      "create table carbonTable(a string, b int, c string) stored as carbondata")
    sql("insert into carbonTable values ('k',5,'k'), ('k',5,'b')")
    sql("insert into carbonTable values ('a',1,'a')")
    sql("insert into carbonTable values ('b',2,'b'),('b',2,'b')")
    sql("insert into carbonTable values ('c',3,'c')")
    sql("alter table carbonTable compact 'major'")

    sql("set carbon.input.segments.default.carbonTable = 0,1,2,3,0.1")
    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(6)))
  }

  test("Read compacted segments after SI creation") {
    sql("drop table if exists carbonTable")
    sql(
      "create table carbonTable(a string, b int, c string) stored as carbondata")
    sql("insert into carbonTable values ('k',5,'k'), ('k',5,'b')")
    sql("insert into carbonTable values ('a',1,'a')")
    sql("insert into carbonTable values ('b',2,'b'),('b',2,'b')")
    sql("insert into carbonTable values ('c',3,'c')")

    sql("drop index if exists indextable1 on carbonTable")
    sql("create index indextable1 on table carbonTable (c) AS 'carbondata'")

    sql("alter table carbonTable compact 'major'")
    sql("set carbon.input.segments.default.carbonTable = 0,1,2,3,0.1")
    checkAnswer(sql("select count(*) from carbonTable where c = 'b'"), Seq(Row(3)))
  }

  test("Read marked for delete segments for partition tables") {
    sql("drop table if exists carbonTable")
    sql(
      "create table carbonTable(c1 string, c2 int) PARTITIONED by (c3 string) stored as " +
      "carbondata ")
    sql("insert into carbonTable values ('k',1,'k'), ('k',2,'k')")
    sql("insert into carbonTable values ('a',2,'a')")
    sql("insert into carbonTable values ('b',2,'b'),('b',2,'b')")
    sql("insert into carbonTable values ('c',2,'c')")

    sql("delete from table carbonTable where segment.id in (0,3)")
    sql("set carbon.input.segments.default.carbonTable = 0,1,2,3")

    checkAnswer(sql("select count(*) from carbonTable where c3 = 'k' or c3 = 'a' or c3 = 'c'"),
      Seq(Row(1)))
  }

  test("Read marked for delete segments with MV creation") {
    sql("drop table if exists carbonTable")
    sql(" CREATE TABLE carbonTable (id int, name string)  STORED AS carbondata")
    sql("insert into carbonTable values(1,'a')")
    sql("insert into carbonTable values(2,'b')")
    sql("insert into carbonTable values(3,'c')")
    sql("insert into carbonTable values(4,'d')")
    sql("delete from table carbonTable where segment.id in (0,3)")

    sql("drop materialized view if exists agg_sale")
    sql(
      "CREATE MATERIALIZED VIEW agg_sale AS SELECT id, name, sum(id) FROM carbonTable GROUP BY " +
      "id, name")

    sql("set carbon.input.segments.default.carbonTable = 0,1,2,3")
    checkAnswer(sql("SELECT count(*)  FROM carbonTable where name = 'a' or name = 'd' or name = " +
                    "'b'"),
      Seq(Row(1)))
  }

}
