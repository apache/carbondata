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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object OrderByMdkFilterQuery {
  def main(args: Array[String]) {
   val cc = ExampleUtils.createCarbonContext("CarbonExample")
   val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS t3")

    // Create table, 6 dimensions, 1 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Date, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    // Load data
    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)

    cc.sql("""select * from t3 order by 2""").show()
    var start = System.currentTimeMillis()
    cc.sql("""
           SELECT date,country,name,salary FROM t3 WHERE
           country = 'china' ORDER BY date limit 1000
           """).show(100000)
    print("levarage optimization query time: " + (System.currentTimeMillis() - start))
    start = System.currentTimeMillis()
    cc.sql("""
           SELECT date,country,name,salary FROM t3 WHERE
           country = 'china'  ORDER BY date, country limit 1000
           """).show(100000)
    print("levarage optimization query time: " + (System.currentTimeMillis() - start))
    start = System.currentTimeMillis()
    cc.sql("""
           SELECT date,country,name,salary FROM t3 WHERE country = 'china'
            ORDER BY date desc, country desc, name desc limit 1000
           """).show(100000)
    print("levarage optimization query time: " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    cc.sql("""
           SELECT date,country,name,salary FROM t3 WHERE
           country = 'china'  ORDER BY date, country desc limit 1000
           """).show(100000)
    print("not levarage optimization query time: " + (System.currentTimeMillis() - start))
    start = System.currentTimeMillis()
    cc.sql("""
           SELECT date,country,name,salary FROM t3 WHERE country = 'china'
            ORDER BY date, country desc, name desc limit 1000
           """).show(100000)
    print("not levarage optimization query time: " + (System.currentTimeMillis() - start))

    cc.sql("DROP TABLE IF EXISTS t3")
  }
}
