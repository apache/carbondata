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

import org.apache.spark.sql.{CarbonEnv, SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example is for showing how to create external table with location.
 */

object ExternalTableExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("ExternalTableExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    // Create origin_table
    spark.sql("DROP TABLE IF EXISTS origin_table")
    spark.sql(
      s"""
         | CREATE TABLE origin_table(
         |   shortField SHORT,
         |   intField INT,
         |   bigintField LONG,
         |   doubleField DOUBLE,
         |   stringField STRING,
         |   timestampField TIMESTAMP,
         |   decimalField DECIMAL(18,2),
         |   dateField DATE,
         |   charField CHAR(5),
         |   floatField FLOAT
         | )
         | STORED AS carbondata
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

    // load 4 times, each load has 10 rows data
    // scalastyle:off
    (1 to 4).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE origin_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin))
    // scalastyle:on

    // 40 rows
    spark.sql("SELECT count(*) FROM origin_table").show()

    val origin_table_path = CarbonEnv.getTablePath(Some("default"), "origin_table")(spark)

    // Create external_table
    spark.sql("DROP TABLE IF EXISTS external_table")
    spark.sql("CREATE EXTERNAL TABLE external_table STORED AS carbondata" +
              s" LOCATION '$origin_table_path'")
    spark.sql("SELECT count(*) FROM external_table").show()

    // Load 2 times again
    (1 to 2).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE origin_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin))

    spark.sql("SELECT count(*) FROM external_table").show()

    // Drop tables
    spark.sql("DROP TABLE IF EXISTS origin_table")
    spark.sql("DROP TABLE IF EXISTS external_table")
  }
}
