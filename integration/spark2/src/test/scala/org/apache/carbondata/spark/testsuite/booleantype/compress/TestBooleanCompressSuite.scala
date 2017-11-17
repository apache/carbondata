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

package org.apache.carbondata.spark.testsuite.booleantype.compress

import java.io.File

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.util.CarbonProperties

class TestBooleanCompressSuite extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../..").getCanonicalPath

  override def beforeEach(): Unit = {
    sql("drop table if exists boolean_table")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists boolean_table")
    assert(BooleanFile.deleteFile(randomBoolean))
  }

  val randomBoolean = s"$rootPath/src/test/resources/bool/supportRandomBooleanBigFile.csv"
  val trueNum = 10000000

  override def beforeAll(): Unit = {
    assert(BooleanFile.createBooleanFileRandom(randomBoolean, trueNum, 0.5))
    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", s"$rootPath/target/warehouse/")
  }

  test("test boolean compress rate: random file") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | booleanField BOOLEAN
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${randomBoolean}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='booleanField')
           """.stripMargin)

    //    Test for compress rate
    //    sql("select * from boolean_table").show(100)
    //    sql("select count(*) from boolean_table").show()
    //    sql("select count(*) from boolean_table where booleanField= true").show()
    //    sql("select count(*) from boolean_table where booleanField= false").show()
    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(trueNum))
  }

}


