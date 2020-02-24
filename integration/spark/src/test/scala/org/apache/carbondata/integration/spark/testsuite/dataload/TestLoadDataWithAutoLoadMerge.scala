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

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test class of creating and loading for carbon table with auto load merge
  */
class TestLoadDataWithAutoLoadMerge extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED AS carbondata
      """)
  }

  test("test data loading with auto load merge") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    checkAnswer(
      sql("SELECT COUNT(*) FROM automerge"),
      Seq(Row(6))
    )
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS automerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }
}
