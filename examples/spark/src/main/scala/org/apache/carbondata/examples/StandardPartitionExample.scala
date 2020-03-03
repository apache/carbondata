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

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example is for standard partition, same as hive and spark partition
 */

object StandardPartitionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("StandardPartitionExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark/src/test/resources/" +
                   s"partition_data_example.csv"
    /**
     * 1. Partition basic usages
     */

    spark.sql("drop table if exists partitiontable0")
    spark.sql(
      """
        | CREATE TABLE partitiontable0
        | (id Int)
        | PARTITIONED BY (lonng Long)
        | STORED AS carbondata
      """.stripMargin)
//
//    // load data and build partition with logdate value
//
    spark.sql(
      s"""
       LOAD DATA LOCAL INPATH '$testData' into table partitiontable0
       """)

    spark.sql(
      s"""
         | SELECT *
         | FROM partitiontable0 where lonng = 1234
      """.stripMargin).show(100, false)

    spark.close()
  }
}
