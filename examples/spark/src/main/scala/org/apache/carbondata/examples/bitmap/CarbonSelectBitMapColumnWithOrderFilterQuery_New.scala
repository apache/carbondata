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

package org.apache.carbondata.examples.bitmap

import scala.collection.mutable.LinkedHashMap

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSelectBitMapColumnWithOrderFilterQuery_New {

  def main(args: Array[String]) {

    CarbonSelectBitMapColumnWithOrderFilterQuery_New.extracted("b3")
  }
  def extracted(tableName: String): Unit = {
    val cc = ExampleUtils.createCarbonContext("CarbonBitMapFilterQueryExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    /*    cc.sql("DROP TABLE IF EXISTS $tableName")

      // Create table, 6 dimensions, 2 measure
      cc.sql(s"""
             CREATE TABLE IF NOT EXISTS $tableName
             (ID Int, date Date, country String,
             name String, phonetype String, serialname char(10), salary Int)
             STORED BY 'carbondata'
             TBLPROPERTIES ('BITMAP'='country')
             """)
      // Load data
      cc.sql(s"""
             LOAD DATA LOCAL INPATH '$testData' into table $tableName
             """)
*/

    //    cc.sql(s"""
    //           SELECT count(*)
    //           FROM $tableName
    //           """).show(10)

    // scalastyle:off println
    var maxTestTimes = 3
    var timeCostSeq = Seq[LinkedHashMap[String, Long]]()
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      var start = System.currentTimeMillis()
      cc.sql(s"""
             SELECT country, serialname, phonetype, salary, name, id
             FROM $tableName
             WHERE country <> 'korea'
             order by country limit 10
             """).show(10)

      timeCostMap += ("country <> 'korea': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country <> 'korea': " + (System.currentTimeMillis() - start))
      start = System.currentTimeMillis()

      cc.sql(s"""
             SELECT country, serialname, phonetype, salary, name, id
             FROM $tableName
             WHERE country = 'korea'
             order by country limit 10
             """).show(10)

      timeCostMap += ("country = 'korea': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country = 'korea': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql(s"""
             SELECT country, serialname, phonetype, salary, name, id
             FROM $tableName
             WHERE country in ('korea', 'china', 'usa', 'uk')
             order by country limit 10
             """).show(10)

      timeCostMap += ("country in ('korea', 'china', 'usa', 'uk'): "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country in ('korea', 'china', 'usa', 'uk'): " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql(s"""
             SELECT country, serialname, phonetype, salary, name, id
             FROM $tableName
             WHERE country not in ('korea', 'china', 'usa', 'uk')
             order by country limit 10
             """).show(10)

      timeCostMap += ("country not in ('korea', 'china', 'usa', 'uk'): "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country not in ('korea', 'china', 'usa', 'uk'): "
          + (System.currentTimeMillis() - start))
      timeCostSeq = timeCostSeq :+ timeCostMap
    }
    // Drop table
    // cc.sql("DROP TABLE IF EXISTS $tableName")

    // use to get statistical information
    for (timeCostMap <- timeCostSeq) {
      for (timeCost <- timeCostMap) {
        print(timeCost._2 + "\t")
      }
      println()
    }
  }
}
