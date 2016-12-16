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

package org.apache.carbondata.spark.testsuite.directdictionary

import java.io.File
import java.sql.{Date, Timestamp}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.BeforeAndAfterAll


/**
  * Test Class for detailed query on timestamp datatypes
  *
  *
  */
class DateDataTypeNullDataTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    try {
      sql(
        """CREATE TABLE IF NOT EXISTS timestampTyeNullData
                     (ID Int, dateField date, country String,
                     name String, phonetype String, serialname String, salary Int)
                    STORED BY 'org.apache.carbondata.format'"""
      )

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
      val csvFilePath = currentDirectory + "/src/test/resources/datasamplenull.csv"
      sql("LOAD DATA LOCAL INPATH '" + csvFilePath + "' INTO TABLE timestampTyeNullData").collect();

    } catch {
      case x: Throwable =>
        x.printStackTrace()
        CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    }
  }

  test("SELECT max(dateField) FROM timestampTyeNullData where dateField is not null") {
    checkAnswer(
      sql("SELECT max(dateField) FROM timestampTyeNullData where dateField is not null"),
      Seq(Row(Date.valueOf("2015-07-23"))
      )
    )
  }
  test("SELECT * FROM timestampTyeNullData where dateField is null") {
    checkAnswer(
      sql("SELECT dateField FROM timestampTyeNullData where dateField is null"),
      Seq(Row(null)
      ))
  }

  override def afterAll {
    sql("drop table timestampTyeNullData")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
  }

}