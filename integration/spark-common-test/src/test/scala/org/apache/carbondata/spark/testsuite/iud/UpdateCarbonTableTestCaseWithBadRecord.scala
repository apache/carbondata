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
package org.apache.carbondata.spark.testsuite.iud

import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class UpdateCarbonTableTestCaseWithBadRecord extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION , LoggerAction.FORCE.name())
  }


  test("test update operation with Badrecords action as force.") {
    sql("""drop table if exists badtable""").show
    sql("""create table badtable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/badrecord.csv' INTO table badtable""")
    sql("""update badtable d  set (d.c2) = (d.c2 / 1)""").show()
    checkAnswer(
      sql("""select c1,c2,c3,c5 from badtable"""),
      Seq(Row("ravi",null,"kiran","huawei"),Row("manohar",null,"vanam","huawei"))
    )
    sql("""drop table badtable""").show


  }
  test("test update operation with Badrecords action as FAIL.") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION , LoggerAction.FAIL.name())
    sql("""drop table if exists badtable""").show
    sql("""create table badtable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/badrecord.csv' INTO table badtable""")
    val exec = intercept[Exception] {
      sql("""update badtable d  set (d.c2) = (d.c2 / 1)""").show()
    }
    checkAnswer(
      sql("""select c1,c2,c3,c5 from badtable"""),
      Seq(Row("ravi",2,"kiran","huawei"),Row("manohar",4,"vanam","huawei"))
    )
    sql("""drop table badtable""").show


  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION , CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
  }
}