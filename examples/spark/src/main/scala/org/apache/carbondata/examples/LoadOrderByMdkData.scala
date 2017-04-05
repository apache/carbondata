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

import java.util.Date

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object LoadOrderByMdkData {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data_sort_by_mdk.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS sortbymdk")

    cc.sql("""
           CREATE TABLE IF NOT EXISTS sortbymdk
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int
           )
           STORED BY 'carbondata'
           """)

    var start = System.currentTimeMillis()

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table sortbymdk
           """)

    var end = System.currentTimeMillis()

    print("load time: " + (end - start))

    cc.sql("""
           SELECT count(country) AS country_cnt
           FROM sortbymdk
           """).show()
  }
}
