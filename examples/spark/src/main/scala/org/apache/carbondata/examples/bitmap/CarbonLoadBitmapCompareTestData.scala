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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonLoadBitmapCompareTestData {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonLoadBitmapCompareTestData")
    val testData = ExampleUtils.currentPath + "/src/main/resources/bitmaptest1.csv"

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    // Create BITMAP table, 6 dimensions, 2 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, country0 String, date Date, country String,
           name String, phonetype String, country1 String, serialname char(10),
           salary Int, country2 String)
           STORED BY 'carbondata'
           """)
    cc.sql("""
           CREATE TABLE IF NOT EXISTS b3
           (ID Int, country0 String, date Date, country String,
           name String, phonetype String, country1 String, serialname char(10),
           salary Int, country2 String)
           STORED BY 'carbondata'
           TBLPROPERTIES ('BITMAP'='country')
           """)
    // Load data
    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table b3
           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table b3
//           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table b3
//           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table b3
//           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table t3
//           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table t3
//           """)
//    cc.sql(s"""
//           LOAD DATA LOCAL INPATH '$testData' into table t3
//           """)
    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)
    cc.sql("""
     SELECT count(*)
     FROM b3
     """).show(10)
    cc.sql("""
     SELECT count(*)
     FROM t3
     """).show(10)
  }

}
