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

package org.apache.carbondata.integration.spark.testsuite.complexType

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test class of creating and loading for carbon table with double
 *
 */
class TestCreateTableWithDouble extends QueryTest with BeforeAndAfterAll {

  var dataPath: String = _
  var countNum: Array[Row] = _
  var doubleField: Array[Row] = _

  override def beforeAll: Unit = {
    dataPath = s"$resourcesPath/sampleComplex.csv"
    countNum = Array(Row(0))
    doubleField = Array(Row(0))
    sql("drop table if exists doubleComplex")
    sql("drop table if exists doubleComplex2")
  }

  test("test creating carbon table with double in complex type") {
    try {
      sql("CREATE TABLE doubleComplex (Id int, number double, name string, " +
        "gamePoint array<double>, mac struct<num:double>) " +
        "STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$dataPath' INTO TABLE doubleComplex")
      countNum = sql(s"SELECT COUNT(*) FROM doubleComplex").collect
      doubleField = sql("SELECT number FROM doubleComplex SORT BY Id").collect
    } catch {
      case e : Throwable => fail(e)
    }
    // assert that load and query is successful
    assertResult(Array(Row(3)))(countNum)
    assertResult(Array(Row(1.5), Row(2.0), Row(3.0)))(doubleField)
  }

  test("test creating carbon table with double as dimension") {
    countNum = Array(Row(0))
    doubleField = Array(Row(0))
    try {
      sql("CREATE TABLE doubleComplex2 (Id int, number double, name string, " +
        "gamePoint array<double>, mac struct<num:double>) " +
        "STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$dataPath' INTO TABLE doubleComplex2")
      countNum = sql(s"SELECT COUNT(*) FROM doubleComplex2").collect
      doubleField = sql(s"SELECT number FROM doubleComplex2 SORT BY Id").collect
    } catch {
      case e : Throwable => fail(e)
    }
    // assert that load and query is successful
    assertResult(countNum)(Array(Row(3)))
    assertResult(doubleField)(Array(Row(1.5), Row(2.0), Row(3.0)))
  }

  override def afterAll {
    sql("drop table if exists doubleComplex")
    sql("drop table if exists doubleComplex2")
  }
}
