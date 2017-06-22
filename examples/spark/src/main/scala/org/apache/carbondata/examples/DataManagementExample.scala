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

import org.apache.carbondata.examples.util.ExampleUtils

object DataManagementExample {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("DataManagementExample")

    cc.sql("DROP TABLE IF EXISTS t3")

    // create a table using CarbonData
    cc.sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
      """
    )

    // data.csv has 1000 lines
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // load data 5 times, each load of data is called a segment in CarbonData
    (1 to 5).map { i =>
      cc.sql(s"LOAD DATA LOCAL INPATH '$testData' into table t3")
    }
    cc.sql("SHOW SEGMENTS FOR TABLE t3 ").show

    // delete the first segment
    cc.sql("DELETE FROM TABLE T3 WHERE SEGMENT.ID IN (0)")
    cc.sql("SHOW SEGMENTS FOR TABLE t3 LIMIT 10").show

    // this query will be executed on last 4 segments, it should return 4000 rows
    cc.sql("SELECT count(*) AS amount FROM t3").show

    // force a major compaction to compact all segments into one
    cc.sql("ALTER TABLE t3 COMPACT 'MAJOR' ")
    cc.sql("SHOW SEGMENTS FOR TABLE t3 LIMIT 10").show

    // load again, add another 1000 rows
    cc.sql(s"LOAD DATA LOCAL INPATH '$testData' into table t3")
    cc.sql("SHOW SEGMENTS FOR TABLE t3 LIMIT 10").show

    // this query will be executed on 2 segments, it should return 5000 rows
    cc.sql("SELECT count(*) AS amount FROM t3").show

    // delete all segments whose loading time is before '2099-01-01 01:00:00'
    cc.sql("DELETE FROM TABLE T3 WHERE SEGMENT.STARTTIME BEFORE '2099-01-01 01:00:00'")
    cc.sql("SHOW SEGMENTS FOR TABLE t3 ").show

    // this query will be executed on 0 segments, it should return 0 rows
    cc.sql("SELECT count(*) AS amount FROM t3").show

    // force clean up all 'MARKED_FOR_DELETE' and 'COMPACTED' segments immediately
    cc.sql("CLEAN FILES FOR TABLE t3")
    cc.sql("SHOW SEGMENTS FOR TABLE t3").show

    cc.sql("DROP TABLE IF EXISTS t3")
  }
}
