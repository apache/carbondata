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

object CarbonSelectBitMapColumnWithGroupByFilterQuery_Old {
  def main(args: Array[String]) {

    CarbonSelectBitMapColumnWithGroupByFilterQuery_New.extracted("t3")
  }
  def extracted(tableName: String) = {
    val cc = ExampleUtils.createCarbonContext("CarbonBitMapFilterQueryExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    /*    cc.sql("DROP TABLE IF EXISTS '$tableName'")

      // Create table, 6 dimensions, 2 measure
      cc.sql("""
             CREATE TABLE IF NOT EXISTS '$tableName'
             (ID Int, date Date, country2 String,
             name String, phonetype String, serialname char(10), salary Int)
             STORED BY 'carbondata'
             TBLPROPERTIES ('BITMAP'='country2')
             """)
      // Load data
      cc.sql(s"""
             LOAD DATA LOCAL INPATH '$testData' into table '$tableName'
             """)
*/
    cc.sql("""
             SELECT country2, count(*)
             FROM '$tableName'
             group by country2
             """).show(10)
    //    cc.sql("""
    //           SELECT count(*)
    //           FROM '$tableName'
    //           """).show(10)

    // scalastyle:off println
    var maxTestTimes = 5
    var timeCostSeq = Seq[LinkedHashMap[String, Long]]()
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      var start = System.currentTimeMillis()
      cc.sql("""
             SELECT country2, count(*)
             FROM '$tableName'
             group by country2
             """).show(10)

      timeCostMap += ("all group by: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("all group by: " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql("""
             SELECT country2, count(*)
             FROM '$tableName'
             WHERE country2 in ('china', 'korea', 'poland')
             group by country2
             """).show(10)

      timeCostMap += ("country2 in ('china', 'korea', 'poland') group by: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 in ('china', 'korea', 'poland') group by: "
        + (System.currentTimeMillis() - start))

      cc.sql("""
             SELECT country2, count(*)
             FROM '$tableName'
             WHERE country2 not in ('china', 'korea', 'poland')
             group by country2
             """).show(10)

      timeCostMap += ("country2 not in ('china', 'korea', 'poland') group by: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 not in ('china', 'korea', 'poland') group by: "
        + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql("""
             SELECT country2, phonetype, sum(salary)
             FROM '$tableName'
             WHERE country2 in ('china', 'korea', 'poland', 'france')
             order by country2, phonetype
             """).show(10)

      timeCostMap += ("country2 in ('china', 'korea', 'poland', 'france') group by: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 in ('china', 'korea', 'poland', 'france') group by: "
        + (System.currentTimeMillis() - start))
      timeCostSeq = timeCostSeq :+ timeCostMap

    }
    // Drop table
    // cc.sql("DROP TABLE IF EXISTS '$tableName'")

    // use to get statistical information
    for (timeCostMap <- timeCostSeq) {
      for (timeCost <- timeCostMap) {
        print(timeCost._2 + "	 ")
      }
      println()
    }
  }
}
