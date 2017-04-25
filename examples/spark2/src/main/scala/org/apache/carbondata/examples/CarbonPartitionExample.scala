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

    spark.sql("DROP TABLE IF EXISTS non_partition_table")

    spark.sql("""
                | CREATE TABLE IF NOT EXISTS non_partition_table
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String
                | )
                | STORED BY 'carbondata'
              """.stripMargin)

    spark.sql("DROP TABLE IF EXISTS partition_table")

    spark.sql("""
       | CREATE TABLE IF NOT EXISTS partition_table
       | (
       | vin String,
       | logdate Timestamp,
       | phonenumber Long,
       | country String,
       | area String
       | )
       | PARTITIONED BY (vin String)
       | STORED BY 'carbondata'
       | TBLPROPERTIES('PARTITIONING'='HASH','PARTITIONCOUNT'='5')
       """.stripMargin)

    //spark.sql(s"""
    //   LOAD DATA LOCAL INPATH '$testData' into table rx5_tbox_parquet_all options('BAD_RECORDS_ACTION'='FORCE')
    //   """)

    //spark.sql("select vin, count(*) from rx5_tbox_parquet_all group by vin order by count(*) desc").show(50)

    // Drop table
    //spark.sql("DROP TABLE IF EXISTS carbon_table")
  }

}
