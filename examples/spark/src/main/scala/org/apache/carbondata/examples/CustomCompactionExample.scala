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
 * This example is for showing how to use custom compaction to merge specified segments.
 */

object CustomCompactionExample {

  def main(args: Array[String]): Unit = {
    val spark = ExampleUtils.createSparkSession("CustomCompactionExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    spark.sql("DROP TABLE IF EXISTS custom_compaction_table")

    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS custom_compaction_table(
         |   ID Int,
         |   date Date,
         |   country String,
         |   name String,
         |   phonetype String,
         |   serialname String,
         |   salary Int,
         |   floatField float
         | )
         | STORED AS carbondata
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark/src/main/resources/dataSample.csv"

    // load 4 segments
    // scalastyle:off
    (1 to 4).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE custom_compaction_table
         | OPTIONS('HEADER'='true')
       """.stripMargin))
    // scalastyle:on

    // show all segment ids: 0,1,2,3
    spark.sql("SHOW SEGMENTS FOR TABLE custom_compaction_table").show()

    // query data
    spark.sql("SELECT count(*) FROM custom_compaction_table").show()
    spark.sql("SELECT * FROM custom_compaction_table WHERE ID=5").show()

    // do custom compaction, segments specified will be merged
    spark.sql("ALTER TABLE custom_compaction_table COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2)")

    // show all segment ids after custom compaction
    spark.sql("SHOW SEGMENTS FOR TABLE custom_compaction_table").show()

    // query data again, results should be same
    spark.sql("SELECT count(*) FROM custom_compaction_table").show()
    spark.sql("SELECT * FROM custom_compaction_table WHERE ID=5").show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    spark.sql("DROP TABLE IF EXISTS custom_compaction_table")
  }

}
