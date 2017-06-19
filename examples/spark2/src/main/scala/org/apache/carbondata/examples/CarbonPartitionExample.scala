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

object CarbonPartitionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val testData = s"$rootPath/examples/spark2/src/main/resources/partition_data.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonPartitionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    // none partition table
    spark.sql("DROP TABLE IF EXISTS t0")

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t0
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String
                | )
                | STORED BY 'carbondata'
              """.stripMargin)

    // range partition
    spark.sql("DROP TABLE IF EXISTS t1")

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t1
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String
                | )
                | PARTITIONED BY (logdate Timestamp)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
                | 'RANGE_INFO'='20140101, 2015/01/01 ,2016-01-01, ')
              """.stripMargin)

    // hash partition
    spark.sql("DROP TABLE IF EXISTS t3")

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t3
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)

    // list partition
    spark.sql("DROP TABLE IF EXISTS t5")

    spark.sql("""
       | CREATE TABLE IF NOT EXISTS t5
       | (
       | vin String,
       | logdate Timestamp,
       | phonenumber Long,
       | country String,
       | area String
       |)
       | PARTITIONED BY (country string)
       | STORED BY 'carbondata'
       | TBLPROPERTIES('PARTITION_TYPE'='LIST',
       | 'LIST_INFO'='(China,United States),UK ,japan,(Canada,Russia), South Korea ')
       """.stripMargin)

    // spark.sql(s"""
    //   LOAD DATA LOCAL INPATH '$testData' into table t3
    // options('BAD_RECORDS_ACTION'='FORCE')
    //   """)

    // spark.sql("select vin, count(*) from t3 group by vin
    // order by count(*) desc").show(50)

    // Drop table
    // spark.sql("DROP TABLE IF EXISTS t3")
  }

}
