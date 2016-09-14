/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test class of creating and loading for carbon table with auto load merge
  */
class TestLoadDataWithAutoLoadMerge extends QueryTest with BeforeAndAfterAll {

  var currentDirectory: String = _

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED BY 'org.apache.carbondata.format'
      """)
    currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
  }

  test("test data loading with auto load merge") {
    val testData = currentDirectory + "/src/test/resources/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge"),
      Seq(Row(4))
    )
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }
}
