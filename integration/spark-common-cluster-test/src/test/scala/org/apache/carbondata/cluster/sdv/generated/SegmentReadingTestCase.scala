
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException


class SegmentReadingTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    cleanAllTable()
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  private def cleanAllTable(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists carbon_table_upd")
  }

  override def afterAll(): Unit = {
    cleanAllTable()
  }

  test("test valid segments and query from table") {
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
      assert(trappedAgain.getMessage
        .equalsIgnoreCase("carbon.input.segments.<database_name>.<table_name> value can't be empty."))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }

  test("test filter with segment reading"){
    try {
      sql("SET carbon.input.segments.default.carbon_table=*")
      checkAnswer(sql("select count(empno) from carbon_table where empno = 15"),Seq(Row(2)))
      sql("SET carbon.input.segments.default.carbon_table=1")
      checkAnswer(sql("select count(empno) from carbon_table where empno = 15"),Seq(Row(1)))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }

  test("test update and delete query with segment reading"){
    try {
      sql("drop table if exists carbon_table_upd")
      sql(
        "create table carbon_table_upd(empno int, empname String, designation String, doj Timestamp," +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_upd OPTIONS
           |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon_table_upd OPTIONS
           |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
      sql("SET carbon.input.segments.default.carbon_table_upd=1")

      sql("SET carbon.input.segments.default.carbon_table=1")
      val updateExceptionMsg = intercept[MalformedCarbonCommandException]{
        sql("update carbon_table_upd a set(a.empname) = (select b.empname from carbon_table b where a.empno=b.empno)").show()
      }
      assert(updateExceptionMsg.getMessage.startsWith("carbon.input.segments.default.carbon_tableshould not be set for table used in UPDATE query"))

      val deleteExceptionMsg = intercept[MalformedCarbonCommandException]{
        sql("delete from carbon_table_upd where empno IN (select empno from carbon_table where empname='ayushi')").show()
      }
      assert(deleteExceptionMsg.getMessage.startsWith("carbon.input.segments.default.carbon_tableshould not be set for table used in DELETE query"))

      sql("SET carbon.input.segments.default.carbon_table=*")
      sql("SET carbon.input.segments.default.carbon_table_upd=1")
      val exceptionMsg = intercept[MalformedCarbonCommandException]{
        sql("update carbon_table_upd a set(a.empname) = (select b.empname from carbon_table b where a.empno=b.empno)").show()
      }
      assert(exceptionMsg.getMessage.startsWith("carbon.input.segments.default.carbon_table_updshould not be set for table used in UPDATE query"))

      val delExceptionMsg = intercept[MalformedCarbonCommandException]{
        sql("delete from carbon_table_upd where empno IN (select empno from carbon_table where empname='ayushi')").show()
      }
      assert(delExceptionMsg.getMessage.startsWith("carbon.input.segments.default.carbon_table_updshould not be set for table used in DELETE query"))
    }
    finally {
      sql("SET carbon.input.segments.default.carbon_table=*")
    }
  }
}
