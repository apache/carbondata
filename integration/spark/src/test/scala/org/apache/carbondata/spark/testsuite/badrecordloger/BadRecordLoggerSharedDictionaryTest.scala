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

package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.BeforeAndAfterAll


/**
 * Test Class for detailed query on timestamp datatypes
 *
 *
 */
class BadRecordLoggerSharedDictionaryTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _
  var csvFilePath : String = null
  var timestamp_format: String = null

  override def beforeAll {
      sql("drop table IF EXISTS testdrive")
    sql(
      """create table testdrive (ID int,CUST_ID int,cust_name string)
          STORED BY 'org.apache.carbondata.format'
            TBLPROPERTIES("columnproperties.cust_name.shared_column"="shared.cust_name",
            "columnproperties.ID.shared_column"="shared.ID",
            "columnproperties.CUST_ID.shared_column"="shared.CUST_ID",
            'DICTIONARY_INCLUDE'='ID,CUST_ID')"""
    )

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          new File("./target/test/badRecords")
            .getCanonicalPath
        )

    val carbonProp = CarbonProperties.getInstance()
    timestamp_format = carbonProp.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    carbonProp.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    csvFilePath = currentDirectory + "/src/test/resources/badrecords/test2.csv"

  }
  test("dataload with bad record test") {
    try {
      sql(
        s"""LOAD DATA INPATH '$csvFilePath' INTO TABLE testdrive OPTIONS('DELIMITER'=',',
            |'QUOTECHAR'= '"', 'BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL',
            |'FILEHEADER'= 'ID,CUST_ID,cust_name')""".stripMargin)
    } catch {
      case e: Throwable =>
        assert(e.getMessage.contains("Data load failed due to bad record"))
    }
  }

  override def afterAll {
    sql("drop table IF EXISTS testdrive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestamp_format)
  }
}
