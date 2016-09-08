/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.spark.testsuite.datacompaction

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.lcm.status.SegmentStatusManager

/**
  * FT for data compaction scenario.
  */
class DataCompactionBlockletBoundryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists  blocklettest")
    sql("drop table if exists  Carbon_automation_hive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "mm/dd/yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        "55")
    sql(
      "CREATE TABLE IF NOT EXISTS blocklettest (country String, ID String, date Timestamp, name " +
        "String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata" +
        ".format'"
    )


    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath1 = currentDirectory + "/src/test/resources/compaction/compaction1.csv"

    // loading the rows greater than 256. so that the column cardinality crosses byte boundary.
    val csvFilePath2 = currentDirectory + "/src/test/resources/compaction/compactioncard2.csv"


    sql("LOAD DATA LOCAL INPATH '" + csvFilePath1 + "' INTO TABLE blocklettest OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("LOAD DATA LOCAL INPATH '" + csvFilePath2 + "' INTO TABLE blocklettest  OPTIONS" +
      "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    // compaction will happen here.
    sql("alter table blocklettest compact 'major'"
    )

    sql(
      "create table Carbon_automation_hive (ID String, date " +
      "Timestamp,country String, name String, phonetype String, serialname String, salary Int ) row format " +
      "delimited fields terminated by ',' TBLPROPERTIES ('skip.header.line.count'='1') "
    )

    sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
        "/src/test/resources/compaction/compaction1_forhive.csv" + "' INTO " +
        "table Carbon_automation_hive ")
    sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
        "/src/test/resources/compaction/compactioncard2_forhive.csv" + "' INTO " +
        "table Carbon_automation_hive ")

  }

  test("select country,count(*) as a from blocklettest")({
    sql("select country,count(*) as a from Carbon_automation_hive group by country").show()
    sql("select *  from Carbon_automation_hive").show
    checkAnswer(
      sql("select country,count(*) as a from blocklettest group by country"),
      sql("select country,count(*) as a from Carbon_automation_hive group by country")
    )
  }
  )

  override def afterAll {
    /* sql("drop table blocklettest") */
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        "" + CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
  }

}
