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

package org.apache.carbondata.examples

import org.apache.spark.sql.CarbonContext

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object ChoosingCompressor {
  val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CompareCompressor")

    loadWithSnappy(cc, "t1")
    cc.sql("""
           SELECT country, count(salary) AS amount
           FROM t1
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

    loadWithNoCompress(cc, "t2")
    cc.sql("""
           SELECT country, count(salary) AS amount
           FROM t2
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

//    cc.sql("DROP TABLE IF EXISTS t1")
//    cc.sql("DROP TABLE IF EXISTS t2")
  }

  def loadWithSnappy(cc: CarbonContext, tableName: String): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    loadTable(cc, tableName)
  }

  def loadWithNoCompress(cc: CarbonContext, tableName: String): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "dummy")
    loadTable(cc, tableName)
  }

  def loadTable(cc: CarbonContext, tableName: String): Unit = {
    cc.sql(s"DROP TABLE IF EXISTS $tableName")

    cc.sql(
      s"""
           CREATE TABLE IF NOT EXISTS $tableName
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """
    )

    cc.sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
  }
}
