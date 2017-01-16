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

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS t3")

    // Create table, 6 dimensions, 1 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)

    // Query data before update and deletion
    cc.sql("""
           SELECT * FROM t3
           """).show()

    // Delete data where salary > 15005
    cc.sql("""
           delete FROM t3 where salary > 15005
           """).show()

    // Query data again after delete data where salary > 15005
    cc.sql("""
           SELECT * FROM t3
           """).show()

    // Update data where salary < 15003
    cc.sql("""
           update t3 set (t3.country) = ('india') where t3.salary < 15003
           """).show()

    cc.sql("""
           update t3 set (t3.salary) = (t3.salary + 9) where t3.name = 'aaa1'
           """).show()

    // Query data again after update data
    cc.sql("""
           SELECT * FROM t3
           """).show()

    // Drop table
    cc.sql("DROP TABLE IF EXISTS t3")
  }

}
