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
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

/**
 * test cases for testing reindex command on index table/main table/DB level
 */
class TestIndexRepair extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop index if exists indextable1 on maintable")
    sql("drop index if exists indextable2 on maintable")
    sql("drop table if exists maintable")
        CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
  }

  test("reindex command after deleting segments from SI table") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(0,1)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    val df1 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    val postDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments!=postDeleteSegments)
    sql("REINDEX INDEX TABLE indextable1 ON MAINTABLE")
    val df2 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    val postRepairSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments == postRepairSegments)
    assert(isFilterPushedDownToSI(df2))
    sql("drop table if exists maintable")
  }


  test("reindex command after deleting segments from SI table on other database without use") {
    sql("drop table if exists test.maintable")
    sql("drop database if exists test cascade")
    sql("create database test")
    sql("CREATE TABLE test.maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table test.maintable(c) as 'carbondata'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO test.maintable SELECT 1,'string1', 'string2'")

    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE1").count()
    sql("DELETE FROM TABLE test.INDEXTABLE1 WHERE SEGMENT.ID IN(0,1,2)")
    sql("CLEAN FILES FOR TABLE test.INDEXTABLE1 options('force'='true')")
    val df1 = sql("select * from test.maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    val postDeleteSegments = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE1").count()
    assert(preDeleteSegments!=postDeleteSegments)
    sql("REINDEX INDEX TABLE indextable1 ON test.MAINTABLE")
    val df2 = sql("select * from test.maintable where c = 'string2'").queryExecution.sparkPlan
    val postRepairSegments = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE1").count()
    assert(preDeleteSegments == postRepairSegments)
    assert(isFilterPushedDownToSI(df2))
    sql("drop index if exists indextable1 on test.maintable")
    sql("drop table if exists test.maintable")
    sql("drop database if exists test cascade")
  }

  test("reindex command using segment.id after deleting segments from SI table") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")

    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(0,1,2)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    val postDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments!=postDeleteSegments)
    val df1 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    sql("REINDEX INDEX TABLE indextable1 ON MAINTABLE WHERE SEGMENT.ID IN (0,1)")
    val postFirstRepair = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(postDeleteSegments + 2 == postFirstRepair)
    val df2 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df2))
    sql("REINDEX INDEX TABLE indextable1 ON MAINTABLE WHERE SEGMENT.ID IN (2)")
    val postRepairSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments == postRepairSegments)
    val df3 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df3))
    sql("drop table if exists maintable")
  }

  test("reindex command with stale files") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(0,1,2)")
    sql("REINDEX INDEX TABLE indextable1 ON MAINTABLE WHERE SEGMENT.ID IN (0,1)")
    assert(sql("select * from maintable where c = 'string2'").count() == 3)
    sql("drop table if exists maintable")
  }

  test("insert command after deleting segments from SI table") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")

    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(1,2,3)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    val postDeleteSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments!=postDeleteSegments)
    val df1 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2'")
    val postLoadSegments = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments + 1 == postLoadSegments)
    val df2 = sql("select * from maintable where c = 'string2'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df2))
    sql("drop table if exists maintable")
  }

  test("reindex command on main table") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING, d STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("CREATE INDEX indextable2 on table maintable(d) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2', 'string3'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2', 'string3'")
    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE MAINTABLE").count()
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(0)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    sql("DELETE FROM TABLE INDEXTABLE2 WHERE SEGMENT.ID IN(0,1)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE2 options('force'='true')")
    val postDeleteSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    val postDeleteSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE2").count()
    assert(preDeleteSegments!=postDeleteSegmentsIndexOne)
    assert(preDeleteSegments!=postDeleteSegmentsIndexTwo)
    sql("REINDEX ON TABLE MAINTABLE")
    val postRepairSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    val postRepairSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE2").count()
    assert(preDeleteSegments == postRepairSegmentsIndexOne)
    assert(preDeleteSegments == postRepairSegmentsIndexTwo)
    sql("drop table if exists maintable")
  }

  test("reindex command on main table with delete command") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable(a INT, b STRING, c STRING, d STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table maintable(c) as 'carbondata'")
    sql("CREATE INDEX indextable2 on table maintable(d) as 'carbondata'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2', 'string3'")
    sql("INSERT INTO maintable SELECT 1,'string1', 'string2', 'string3'")
    val preDeleteSegments = sql("SHOW SEGMENTS FOR TABLE MAINTABLE").count()
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.ID IN(0)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    sql("DELETE FROM TABLE INDEXTABLE2 WHERE SEGMENT.ID IN(1)")
    sql("CLEAN FILES FOR TABLE INDEXTABLE2 options('force'='true')")
    var postDeleteSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    val postDeleteSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE2").count()
    assert(preDeleteSegments != postDeleteSegmentsIndexOne)
    assert(preDeleteSegments != postDeleteSegmentsIndexTwo)
    sql("REINDEX ON TABLE MAINTABLE WHERE SEGMENT.ID IN(0,1)")
    var postRepairSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    val postRepairSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE2").count()
    assert(preDeleteSegments == postRepairSegmentsIndexOne)
    assert(preDeleteSegments == postRepairSegmentsIndexTwo)
    sql("DELETE FROM TABLE INDEXTABLE1 WHERE SEGMENT.STARTTIME BEFORE '2099-01-01 01:00:00'")
    sql("CLEAN FILES FOR TABLE INDEXTABLE1 options('force'='true')")
    postDeleteSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments != postDeleteSegmentsIndexOne)
    sql("REINDEX ON TABLE MAINTABLE WHERE SEGMENT.ID IN(0,1)")
    postRepairSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE INDEXTABLE1").count()
    assert(preDeleteSegments == postRepairSegmentsIndexOne)
    sql("drop table if exists maintable")
  }


  test("reindex command on database") {
    sql("drop database if exists test cascade")
    sql("create database test")
    sql("drop table if exists maintable1")

    // table 1
    sql("CREATE TABLE test.maintable1(a INT, b STRING, c STRING, d STRING) stored as carbondata")
    sql("CREATE INDEX indextable1 on table test.maintable1(c) as 'carbondata'")
    sql("CREATE INDEX indextable2 on table test.maintable1(d) as 'carbondata'")
    sql("INSERT INTO test.maintable1 SELECT 1,'string1', 'string2', 'string3'")
    sql("INSERT INTO test.maintable1 SELECT 1,'string1', 'string2', 'string3'")

    val preDeleteSegmentsTableOne = sql("SHOW SEGMENTS FOR TABLE test.MAINTABLE1").count()
    sql("DELETE FROM TABLE test.INDEXTABLE1 WHERE SEGMENT.ID IN(0)")
    sql("CLEAN FILES FOR TABLE test.INDEXTABLE1 options('force'='true')")
    sql("DELETE FROM TABLE test.INDEXTABLE2 WHERE SEGMENT.ID IN(0,1)")
    sql("CLEAN FILES FOR TABLE test.INDEXTABLE2 options('force'='true')")
    val postDeleteSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE1").count()
    val postDeleteSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE2").count()

    // table 2
    sql("CREATE TABLE test.maintable2(a INT, b STRING, c STRING, d STRING) stored as carbondata")
    sql("CREATE INDEX indextable3 on table test.maintable2(c) as 'carbondata'")
    sql("CREATE INDEX indextable4 on table test.maintable2(d) as 'carbondata'")
    sql("INSERT INTO test.maintable2 SELECT 1,'string1', 'string2', 'string3'")
    sql("INSERT INTO test.maintable2 SELECT 1,'string1', 'string2', 'string3'")

    val preDeleteSegmentsTableTwo = sql("SHOW SEGMENTS FOR TABLE test.MAINTABLE2").count()
    sql("DELETE FROM TABLE test.INDEXTABLE3 WHERE SEGMENT.ID IN(1)")
    sql("CLEAN FILES FOR TABLE test.INDEXTABLE3 options('force'='true')")
    sql("DELETE FROM TABLE test.INDEXTABLE4 WHERE SEGMENT.ID IN(0,1)")
    sql("CLEAN FILES FOR TABLE test.INDEXTABLE4 options('force'='true')")
    val postDeleteSegmentsIndexThree = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE3").count()
    val postDeleteSegmentsIndexFour = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE4").count()

    assert(preDeleteSegmentsTableOne!=postDeleteSegmentsIndexOne)
    assert(preDeleteSegmentsTableOne!=postDeleteSegmentsIndexTwo)
    assert(preDeleteSegmentsTableTwo!=postDeleteSegmentsIndexThree)
    assert(preDeleteSegmentsTableTwo!=postDeleteSegmentsIndexFour)
    sql("REINDEX DATABASE TEST")
    val postRepairSegmentsIndexOne = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE1").count()
    val postRepairSegmentsIndexTwo = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE2").count()
    val postRepairSegmentsIndexThree = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE3").count()
    val postRepairSegmentsIndexFour = sql("SHOW SEGMENTS FOR TABLE test.INDEXTABLE4").count()
    assert(preDeleteSegmentsTableOne == postRepairSegmentsIndexOne)
    assert(preDeleteSegmentsTableOne == postRepairSegmentsIndexTwo)
    assert(preDeleteSegmentsTableTwo == postRepairSegmentsIndexThree)
    assert(preDeleteSegmentsTableTwo == postRepairSegmentsIndexFour)
    sql("drop index if exists indextable1 on test.maintable1")
    sql("drop index if exists indextable2 on test.maintable1")
    sql("drop table if exists test.maintable1")
    sql("drop index if exists indextable3 on test.maintable2")
    sql("drop index if exists indextable4 on test.maintable2")
    sql("drop table if exists test.maintable2")
    sql("drop database if exists test cascade")
  }


  override def afterAll {
    sql("drop index if exists indextable1 on maintable")
    sql("drop index if exists indextable2 on maintable")
    sql("drop table if exists maintable")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
  }

}
