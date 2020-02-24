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

package org.apache.carbondata.spark.testsuite.directdictionary

import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test case copied from `DateDataTypeDirectDictionaryTest` to verify CARBONDATA-1878
 */
class DateDataTypeDirectDictionaryWithOffHeapSortDisabledTest
  extends QueryTest with BeforeAndAfterAll {
  private val originOffHeapSortStatus: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)

  override def beforeAll {
    try {
      CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")

      sql("drop table if exists directDictionaryTable ")
      sql("CREATE TABLE if not exists directDictionaryTable (empno int,doj date, salary int) " +
        "STORED AS carbondata")

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      val csvFilePath = s"$resourcesPath/datasamplefordate.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable OPTIONS" +
          "('DELIMITER'= ',', 'QUOTECHAR'= '\"')" )
    } catch {
      case x: Throwable =>
        x.printStackTrace()
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    }
  }

  test("test direct dictionary for not null condition") {
    checkAnswer(sql("select doj from directDictionaryTable where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")), Row(Date.valueOf("2016-04-14"))))
  }

  override def afterAll {
    sql("drop table directDictionaryTable")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      originOffHeapSortStatus)
  }
}
