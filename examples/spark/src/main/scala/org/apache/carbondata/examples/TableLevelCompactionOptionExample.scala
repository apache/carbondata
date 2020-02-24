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


object TableLevelCompactionOptionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("TableLevelCompactionOptionExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    spark.sql("DROP TABLE IF EXISTS tablecompaction_table")

    // Create table with table level compaction options
    // while loading and compacting, table level compaction options will be used instead of
    // options specified in carbon.properties
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS tablecompaction_table(
         | ID Int,
         | date Date,
         | country String,
         | name String,
         | phonetype String,
         | serialname String,
         | salary Int,
         | floatField float
         | )
         | STORED AS carbondata
         | TBLPROPERTIES (
         | 'MAJOR_COMPACTION_SIZE'='1024',
         | 'AUTO_LOAD_MERGE'='true',
         | 'COMPACTION_LEVEL_THRESHOLD'='3,2',
         | 'COMPACTION_PRESERVE_SEGMENTS'='2',
         | 'ALLOWED_COMPACTION_DAYS'='1')
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark/src/main/resources/dataSample.csv"

    // load 6 segments
    // scalastyle:off
    (1 to 6).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE tablecompaction_table
         | OPTIONS('HEADER'='true')
       """.stripMargin))
    // scalastyle:on

    // show all segments, existing segments are 0.1,3,4,5, compacted segments are 0,1,2
    // because of 2 segments are preserved, only one level-1 minor compaction is triggered
    spark.sql("SHOW SEGMENTS FOR TABLE tablecompaction_table").show()

    // load another 2 segments
    // scalastyle:off
    (1 to 2).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE tablecompaction_table
         | OPTIONS('HEADER'='true')
       """.stripMargin))
    // scalastyle:on

    // show all segments, existing segments will be 0.2,6,7,
    // compacted segments are 0,1,2,3,4,5,0.1,3.1
    spark.sql("SHOW SEGMENTS FOR TABLE tablecompaction_table").show()

    // load another 2 segments
    // scalastyle:off
    (1 to 2).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE tablecompaction_table
         | OPTIONS('HEADER'='true')
       """.stripMargin))
    // scalastyle:on

    // do major compaction, there will be 3 segment left(2 preserved segments)
    spark.sql("ALTER TABLE tablecompaction_table COMPACT 'MAJOR'")
    spark.sql("CLEAN FILES FOR TABLE tablecompaction_table")
    spark.sql("SHOW SEGMENTS FOR TABLE tablecompaction_table").show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    spark.sql("DROP TABLE IF EXISTS tablecompaction_table")
  }

}
