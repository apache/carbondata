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

package org.apache.spark.carbondata.vectorreader

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class VectorReaderTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("DROP TABLE IF EXISTS vectorreader")
    // clean data folder

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("CREATE TABLE vectorreader (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE vectorreader")
  }

  test("test vector reader") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    val plan = sql(
      """select * from vectorreader""".stripMargin).queryExecution.executedPlan
    var batchReader = false
    plan.collect {
      case s: CarbonDataSourceScan => batchReader = true
    }
    assert(batchReader, "batch reader should exist when carbon.enable.vector.reader is true")
  }

  test("test without vector reader") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    val plan = sql(
      """select * from vectorreader""".stripMargin).queryExecution.executedPlan
    var rowReader = false
    plan.collect {
      case s: RowDataSourceScanExec => rowReader = true
    }
    assert(rowReader, "row reader should exist by default")
  }

  test("test vector reader for random measure selection") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("""select salary, ID from vectorreader where ID = 94""".stripMargin),
      Seq(Row(15093, 94)))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sqlContext.setConf(
      "carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    sql("DROP TABLE IF EXISTS vectorreader")
  }
}
