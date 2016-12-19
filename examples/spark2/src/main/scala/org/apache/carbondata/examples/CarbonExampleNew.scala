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

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonExampleNew {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"
    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonExampleNew")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastoredb;create=true")
      .getOrCreateCarbon()
    spark.sparkContext.setLogLevel("WARN")
    CarbonProperties.getInstance()
      .addProperty("carbon.kettle.home", s"$rootPath/processing/carbonplugins")
      .addProperty("carbon.storelocation", storeLocation)

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

    spark.sql("DROP TABLE IF EXISTS t3")

    // Create table, 6 dimensions, 1 measure
    spark.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    // Currently there are two data loading flows in CarbonData, one uses Kettle as ETL tool
    // in each node to do data loading, another uses a multi-thread framework without Kettle (See
    // AbstractDataLoadProcessorStep)
    // Load data with Kettle
    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$path' into table t3
           """)

    // Perform a query
    spark.sql("""
           SELECT country, count(salary) AS amount
           FROM t3
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

    // Load data without kettle
    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$path' into table t3
           OPTIONS('USE_KETTLE'='false')
           """)

    // Perform a query
    spark.sql("""
           SELECT country, count(salary) AS amount
           FROM t3
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS t3")
  }

}
