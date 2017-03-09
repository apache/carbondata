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

object DataUpdateDeleteExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("DataUpdateDeleteExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"
    val testData1 = ExampleUtils.currentPath + "/src/main/resources/data_update.csv"

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS update_table")

    // Create table, 6 dimensions, 1 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Date, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' INTO TABLE t3
           """)

    // 1.Update data with simple SET
    cc.sql("""
           SELECT * FROM t3 ORDER BY ID
           """).show()

    // Update data where salary < 15003
    cc.sql("""
           UPDATE t3 SET (t3.country) = ('india') WHERE t3.salary < 15003
           """).show()
    cc.sql("""
           UPDATE t3 SET (t3.salary) = (t3.salary + 9) WHERE t3.name = 'aaa1'
           """).show()

    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY ID
           """).show()

    // 2.Update data with subquery result SET
    cc.sql("""
           CREATE TABLE IF NOT EXISTS update_table
           (ID Int, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData1' INTO TABLE update_table
           """)

    cc.sql("""
         UPDATE t3
         SET (t3.country, t3.name) = (SELECT u.country, u.name FROM update_table u WHERE u.id = 5)
         WHERE t3.id < 5""").show()

    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY ID
           """).show()

    // 3.Update data with join query result SET
    cc.sql("""
         UPDATE t3
         SET (t3.country, t3.salary) =
         (SELECT u.country, f.salary FROM update_table u FULL JOIN update_table f
         WHERE u.id = 8 and f.id=6) WHERE t3.id >6""").show()

    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY ID
           """).show()

    // 4.Delete data where salary > 15005
    cc.sql("""
           DELETE FROM t3 WHERE salary > 15005
           """).show()

    // Query data again after delete data
    cc.sql("""
           SELECT * FROM t3 ORDER BY ID
           """).show()

    // Drop table
    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS update_table")
  }

}
