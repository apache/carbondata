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

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.spark.sql.{CarbonThreadUtil, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Testcase for set segment in multhread env
 */
class TestSegmentReadingForMultiThreading extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_MulTI_THread")
    sql(
      "CREATE TABLE carbon_table_MulTI_THread (empno int, empname String, designation String, doj" +
      " Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED AS carbondata")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data1.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data1.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
  }

  test("test multithreading for segment reading") {
    CarbonThreadUtil.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1,2,3")
    val df = sql("select count(empno) from carbon_table_MulTI_THread")
    checkAnswer(df, Seq(Row(30)))

    val four = Future {
      CarbonThreadUtil.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1,3")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(20)))
    }

    val three = Future {
      CarbonThreadUtil.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "0,1,2")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(30)))
    }


    val one = Future {
      CarbonThreadUtil.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "0,2")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(20)))
    }

    val two = Future {
      CarbonThreadUtil.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(10)))
    }
    // scalastyle:off awaitresult
    Await.result(Future.sequence(Seq(one, two, three, four)), Duration(300, TimeUnit.SECONDS))
    // scalastyle:on awaitresult
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_MulTI_THread")
    CarbonThreadUtil.threadUnset("carbon.input.segments.default.carbon_table_MulTI_THread")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
  }
}
