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
import org.apache.carbondata.examples.util.ExampleUtils


/**
 * This example introduces how to query data with specified segments
 */

object QuerySegmentExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("QuerySegmentExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    spark.sql("DROP TABLE IF EXISTS querysegment_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE querysegment_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED AS carbondata
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

    // load 4 segments, each load has 10 rows data
    // scalastyle:off
    (1 to 4).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE querysegment_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin))
    // scalastyle:on

    // 1.Query data with specified segments without compaction

    spark.sql("SHOW SEGMENTS FOR TABLE querysegment_table").show()
    // 40 rows
    spark.sql(
      s"""
         | SELECT count(*)
         | FROM querysegment_table
       """.stripMargin).show()

    // specify segments to query
    spark.sql("SET carbon.input.segments.default.querysegment_table = 1,3")
    // 20 rows from segment1 and segment3
    spark.sql(
      s"""
         | SELECT count(*)
         | FROM querysegment_table
       """.stripMargin).show()

    // 2.Query data with specified segments after compaction

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "3,2")

    spark.sql("ALTER TABLE querysegment_table COMPACT 'MINOR'")
    spark.sql("SHOW SEGMENTS FOR TABLE querysegment_table").show()

    // Reset to query all segments data
    spark.sql("SET carbon.input.segments.default.querysegment_table = *")
    // 40 rows from all segments
    spark.sql(
      s"""
         | SELECT count(*)
         | FROM querysegment_table
       """.stripMargin).show()
    // After MINOR compaction, 0.1 has 30 rows data(compact 3 segments)
    spark.sql("SET carbon.input.segments.default.querysegment_table = 0.1")
    spark.sql(
      s"""
         | SELECT count(*)
         | FROM querysegment_table
       """.stripMargin).show()

    spark.sql("ALTER TABLE querysegment_table COMPACT 'MAJOR'")
    spark.sql("CLEAN FILES FOR TABLE querysegment_table")
    spark.sql("SHOW SEGMENTS FOR TABLE querysegment_table").show()

    // Load 2 new segments
    (1 to 2).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE querysegment_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin))

    spark.sql("SHOW SEGMENTS FOR TABLE querysegment_table").show()
    // 50 rows: segment0.2 has 40 rows after major compaction, plus segment5 with 10 rows
    spark.sql("SET carbon.input.segments.default.querysegment_table = 0.2,5")
    spark.sql(
      s"""
         | SELECT count(*)
         | FROM querysegment_table
       """.stripMargin).show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    spark.sql("DROP TABLE IF EXISTS querysegment_table")
  }

}