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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
  * Test Class for filter expression query on String datatypes
  *
  */
class GrtLtFilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists a12_allnull")

    sql(
      "create table a12_allnull(empid String,ename String,sal double,deptno int,mgr string,gender" +
        " string," +
        "dob timestamp,comm decimal(4,2),desc string) stored by 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    val testData = s"$resourcesPath/filter/emp2allnull.csv"

    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12_allnull OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"','FILEHEADER'='empid,ename,sal,deptno,mgr,gender,dob,comm,desc')"""
        .stripMargin
    )
  }

  test("In condition With improper format query regarding Null filter") {
    checkAnswer(
      sql("select empid from a12_allnull " + "where empid not in ('china',NULL)"),
      Seq()
    )
  }

  override def afterAll {
    sql("drop table if exists a12_allnull")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, TestQueryExecutor.timestampFormat)
  }
}
