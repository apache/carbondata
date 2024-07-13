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
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test class of creating and loading for carbon table with float
 *
 */
class TestCreateTableWithFloat extends QueryTest with BeforeAndAfterAll {

  var dataPath: String = _
  var countNum: Array[Row] = _
  var floatFiled: Array[Row] = _
  var arrayField: Array[Row] = _
  var structField: Array[Row] = _

  override def beforeAll: Unit = {
    dataPath = s"$resourcesPath/sampleComplex.csv"
    sql("drop table if exists floatComplex")
  }

  test("test creating carbon table with float in complex type") {
    try {
      sql("CREATE TABLE floatComplex (Id int, number float, name string, " +
        "gamePoint array<float>, mac struct<num:float>) " +
        "STORED AS carbondata")
      val desc = sql("desc formatted floatComplex").collect()
      assert(desc.apply(1).get(1).equals("float"))
      assert(desc.apply(3).get(1).equals("array<float>"))
      assert(desc.apply(4).get(1).equals("struct<num:float>"))
      sql(s"LOAD DATA LOCAL INPATH '$dataPath' INTO TABLE floatComplex")
      countNum = sql(s"SELECT COUNT(*) FROM floatComplex").collect
      floatFiled = sql("SELECT number FROM floatComplex SORT BY Id").collect
      arrayField = sql("SELECT gamePoint FROM floatComplex SORT BY Id").collect
      structField = sql("SELECT mac.num FROM floatComplex SORT BY Id").collect
    } catch {
      case e : Throwable => fail(e)
    }
    // assert that load and query is successful
    assertResult(Array(Row(3)))(countNum)
    assertResult(Array(Row(1.5), Row(2.0), Row(3.0)))(floatFiled)
    assertResult(Array(Row(null), Row(null), Row(null)))(arrayField)
    assertResult(Array(Row(3.0), Row(1.5), Row(2.0)))(structField)
  }

  override def afterAll: Unit = {
    sql("drop table if exists floatComplex")
  }
}
