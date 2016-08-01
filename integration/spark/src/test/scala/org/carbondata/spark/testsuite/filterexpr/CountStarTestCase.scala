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

package org.carbondata.spark.testsuite.filterexpr

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for filter expression query on String datatypes
  *
  * @author N00902756
  *
  */
class CountStarTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists filtertestTables")


    sql("CREATE TABLE filterTimestampDataType (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath = currentDirectory + "/src/test/resources/datanullmeasurecol.csv";
      sql(
      s"LOAD DATA LOCAL INPATH '" + csvFilePath + "' INTO TABLE " +
        s"filterTimestampDataType " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
  }

  test("select count ") {
    checkAnswer(
      sql("select count(*) from filterTimestampDataType where country='china'"),
      Seq(Row(2))
    )
  }

  override def afterAll {
    sql("drop table noloadtable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}