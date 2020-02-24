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

package org.apache.spark.carbondata

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp datatypes
 *
 *
 */
class BadRecordPathLoadOptionTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    sql("drop table IF EXISTS salestest")
  }

  test("data load log file and csv file written at the configured location") {
    sql(
      s"""CREATE TABLE IF NOT EXISTS salestest(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata TBLPROPERTIES('BAD_RECORD_PATH'='$warehouse')""")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE salestest OPTIONS" +
        "('bad_records_logger_enable'='true','bad_records_action'='redirect', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    val location: Boolean = isFilesWrittenAtBadStoreLocation
    assert(location)
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table salestest")
  }

  def isFilesWrittenAtBadStoreLocation: Boolean = {
    val badStorePath =
      CarbonEnv.getCarbonTable(Some("default"), "salestest")(sqlContext.sparkSession).getTableInfo
        .getFactTable.getTableProperties.get("bad_record_path") + "/0/0"
    val carbonFile: CarbonFile = FileFactory.getCarbonFile(badStorePath)
    var exists: Boolean = carbonFile.exists()
    if (exists) {
      val listFiles: Array[CarbonFile] = carbonFile.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          if (file.getName.endsWith(".log") || file.getName.endsWith(".csv")) {
            return true;
          }
          return false;
        }
      })
      exists = listFiles.size > 0
    }
    return exists;
  }

}