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

package org.apache.carbondata.integration.spark.testsuite.emptyrow

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class TestCSVHavingOnlySpaceChar extends QueryTest with BeforeAndAfterAll {

  var csvFilePath : String = null

  override def beforeAll {
    sql("drop table if exists emptyRowCarbonTable")
    //eid,ename,sal,presal,comm,deptno,Desc
    sql(
      "create table if not exists emptyRowCarbonTable (eid int,ename String,sal decimal,presal " +
        "decimal,comm decimal" +
        "(37,37),deptno decimal(18,2),Desc String) STORED AS carbondata"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
     csvFilePath = s"$resourcesPath/emptyrow/csvwithonlyspacechar.csv"
      }


  test("dataload") {
    try {
      sql(
        s"""LOAD DATA INPATH '$csvFilePath' INTO table emptyRowCarbonTable OPTIONS('DELIMITER'=',','QUOTECHAR'='"')""")
    } catch {
      case e: Throwable =>
        System.out.println(e.getMessage)
        assert(e.getMessage.contains("First line of the csv is not valid."))
    }
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table emptyRowCarbonTable")
  }
}
