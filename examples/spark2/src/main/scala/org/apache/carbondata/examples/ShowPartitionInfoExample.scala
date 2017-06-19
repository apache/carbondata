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

object ShowPartitionInfoExample {
  def main(args: Array[String]) {
    ShowPartitionInfoExample.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val testData = s"$rootPath/examples/spark2/src/main/resources/bitmaptest2.csv"
    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonDataLoad")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    // range partition
    spark.sql("DROP TABLE IF EXISTS t1")
    // hash partition
    spark.sql("DROP TABLE IF EXISTS t3")
    // list partition
    spark.sql("DROP TABLE IF EXISTS t5")

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t1
                | (
                | vin STRING,
                | phonenumber LONG,
                | country STRING,
                | area STRING
                | )
                | PARTITIONED BY (logdate TIMESTAMP)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
                | 'RANGE_INFO'='2014/01/01,2015/01/01,2016/01/01')
              """.stripMargin)

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t3
                | (
                | logdate TIMESTAMP,
                | phonenumber LONG,
                | country STRING,
                | area STRING
                | )
                | PARTITIONED BY (vin STRING)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)

    spark.sql("""
       | CREATE TABLE IF NOT EXISTS t5
       | (
       | vin STRING,
       | logdate TIMESTAMP,
       | phonenumber LONG,
       | area STRING
       |)
       | PARTITIONED BY (country STRING)
       | STORED BY 'carbondata'
       | TBLPROPERTIES('PARTITION_TYPE'='LIST',
       | 'LIST_INFO'='(China,United States),UK ,japan,(Canada,Russia), South Korea ')
       """.stripMargin)

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("""SHOW PARTITIONS t1""").show()
    spark.sql("""SHOW PARTITIONS t3""").show()
    spark.sql("""SHOW PARTITIONS t5""").show()

    // range partition
    spark.sql("DROP TABLE IF EXISTS t1")
    // hash partition
    spark.sql("DROP TABLE IF EXISTS t3")
    // list partition
    spark.sql("DROP TABLE IF EXISTS t5")

    // close spark session
    spark.close()

  }
}
