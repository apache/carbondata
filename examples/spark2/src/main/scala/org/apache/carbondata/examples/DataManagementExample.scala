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

object DataManagementExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("DataManagementExample")
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table(
         | ID Int,
         | date Date,
         | country String,
         | name String,
         | phonetype String,
         | serialname String,
         | salary Int,
         | floatField float
         | ) STORED BY 'carbondata'
       """.stripMargin)

    // data.csv has 10 lines
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark2/src/main/resources/dataSample.csv"

    // load data 5 times, each load of data is called a segment in CarbonData
    // scalastyle:off
    (1 to 5).foreach(_ => spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin))
    // scalastyle:on

    // show all segments, there will be 5 segments
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()

    // 50 rows loaded
    spark.sql("SELECT count(*) FROM carbon_table").show()

    // delete the first segment
    spark.sql("DELETE FROM TABLE carbon_table WHERE SEGMENT.ID IN (0)")
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()

    // this query will be executed on last 4 segments, it should return 40 rows
    spark.sql("SELECT count(*) FROM carbon_table").show()

    // force a major compaction to compact all segments into one
    spark.sql("ALTER TABLE carbon_table COMPACT 'MAJOR'")
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()

    // load again, add another 10 rows
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()

    // this query will be executed on 2 segments, it should return 5000 rows
    spark.sql("SELECT count(*) FROM carbon_table").show()

    // delete all segments whose loading time is before '2099-01-01 01:00:00'
    spark.sql("DELETE FROM TABLE carbon_table WHERE SEGMENT.STARTTIME BEFORE '2099-01-01 01:00:00'")
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table ").show()

    // this query will be executed on 0 segments, it should return 0 rows
    spark.sql("SELECT count(*) FROM carbon_table").show()

    // force clean up all 'MARKED_FOR_DELETE' and 'COMPACTED' segments immediately
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()
    spark.sql("CLEAN FILES FOR TABLE carbon_table")
    spark.sql("SHOW SEGMENTS FOR TABLE carbon_table").show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")

    spark.stop()
  }

}
