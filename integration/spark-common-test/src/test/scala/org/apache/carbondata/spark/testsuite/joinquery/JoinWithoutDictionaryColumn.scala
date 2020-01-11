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

package org.apache.carbondata.spark.testsuite.joinquery

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class JoinWithoutDictionaryColumn extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists mobile")
    sql("drop table if exists emp")

    sql("drop table if exists mobile_d")
    sql("drop table if exists emp_d")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
      )

    sql(
      """
        create table mobile (mid String,Mobileid String, Color String, id int) STORED AS carbondata
      """)
    sql(
      """
        create table emp (eid String,ename String, Mobileid String,Color String, id int) STORED AS carbondata
      """)

    sql(
      """
        create table mobile_d (mid String,Mobileid String, Color String, id int) STORED AS carbondata
      """)
    sql(
      """
        create table emp_d (eid String,ename String, Mobileid String,Color String, id int) STORED AS carbondata
      """)

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/join/mobile.csv' into table
           mobile
           OPTIONS('FILEHEADER'='mid,Mobileid,Color,id')
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' into table
           emp
           OPTIONS('FILEHEADER'='eid,ename,Mobileid,Color,id')
           """)

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/join/mobile.csv' into table
           mobile_d
           OPTIONS('FILEHEADER'='mid,Mobileid,Color,id')
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' into table
           emp_d
           OPTIONS('FILEHEADER'='eid,ename,Mobileid,Color,id')
           """)
  }

  test("select * from emp join mobile on emp.Mobileid=mobile.Mobileid") {
    checkAnswer(
      sql("select * from emp join mobile on emp.Mobileid=mobile.Mobileid"),
      sql("select * from emp_d join mobile_d on emp_d.Mobileid=mobile_d.Mobileid")
    )
  }

  override def afterAll {
    sql("drop table if exists emp")
    sql("drop table if exists mobile")
    sql("drop table if exists emp_d")
    sql("drop table if exists mobile_d")
  }
}
