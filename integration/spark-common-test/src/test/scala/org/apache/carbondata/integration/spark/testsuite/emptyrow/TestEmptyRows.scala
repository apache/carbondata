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

package org.apache.carbondata.spark.testsuite.singlevaluerow

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class TestEmptyRows extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists emptyRowCarbonTable")
    sql("drop table if exists emptyRowHiveTable")

    sql(
      "create table if not exists emptyRowCarbonTable (eid int,ename String,sal decimal,presal " +
        "decimal,comm decimal" +
        "(37,37),deptno decimal(18,2),Desc String) STORED AS carbondata"
    )
    sql(
      "create table if not exists emptyRowHiveTable(eid int,ename String,sal decimal,presal " +
        "decimal,comm " +
        "decimal(37,37),deptno decimal(18,2),Desc String)row format delimited fields " +
        "terminated by ','"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val csvFilePath = s"$resourcesPath/emptyrow/emptyRows.csv"

    sql(
      s"""LOAD DATA INPATH '$csvFilePath' INTO table emptyRowCarbonTable OPTIONS('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'='eid,ename,sal,presal,comm,deptno,Desc')""")

    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table " +
        "emptyRowHiveTable"
    )
  }

  test("select eid from table") {
    checkAnswer(
      sql("select eid from emptyRowCarbonTable"),
      sql("select eid from emptyRowHiveTable")
    )
  }

  test("select Desc from emptyRowTable") {
    checkAnswer(
      sql("select Desc from emptyRowCarbonTable"),
      sql("select Desc from emptyRowHiveTable")
    )
  }

  test("select count(Desc) from emptyRowTable") {
    checkAnswer(
      sql("select count(Desc) from emptyRowCarbonTable"),
      sql("select count(Desc) from emptyRowHiveTable")
    )
  }

  test("select count(distinct Desc) from emptyRowTable") {
    checkAnswer(
      sql("select count(distinct Desc) from emptyRowCarbonTable"),
      sql("select count(distinct Desc) from emptyRowHiveTable")
    )
  }

  override def afterAll {
    sql("drop table emptyRowCarbonTable")
    sql("drop table emptyRowHiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
