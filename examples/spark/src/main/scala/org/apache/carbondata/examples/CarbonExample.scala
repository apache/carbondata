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

object CarbonExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // Drop table

    cc.sql("DROP TABLE IF EXISTS uniq_shared_dictionary ")

    // Create table, with shared columns

    cc.sql(
      """ CREATE TABLE uniq_shared_dictionary (CUST_ID int,CUST_NAME String,
      ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,
      BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,
      10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY
    'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,
    Double_COLUMN2,DECIMAL_COLUMN2','columnproperties.CUST_ID.shared_column'='shared.CUST_ID',
    'columnproperties.decimal_column2.shared_column'='shared.decimal_column2')"""
        .stripMargin).show

    cc.sql("select * from uniq_shared_dictionary").show()

    // Create table, 6 dimensions, 1 measure
    cc.sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
      """)

    // Currently there are two data loading flows in CarbonData, one uses Kettle as ETL tool
    // in each node to do data loading, another uses a multi-thread framework without Kettle (See
    // AbstractDataLoadProcessorStep)
    // Load data with Kettle
    cc.sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)

    // Perform a query
    cc.sql(
      """
           SELECT country, count(salary) AS amount
           FROM t3
           WHERE country IN ('china','france')
           GROUP BY country
      """).show()

    // Load data without kettle
    cc.sql(
      s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           OPTIONS('USE_KETTLE'='false')
           """)

    // Perform a query
    cc.sql(
      """
           SELECT country, count(salary) AS amount
           FROM t3
           WHERE country IN ('china','france')
           GROUP BY country
      """).show()

    // Drop table
    cc.sql("DROP TABLE IF EXISTS t3")
  }

}
