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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test Class for detailed query on timestamp datatypes
 */
class TimestampNoDictionaryColumnTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")

    sql("drop table if exists timestamp_nodictionary")
    sql(
      """
         CREATE TABLE IF NOT EXISTS timestamp_nodictionary
        (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        workgroupcategoryname String,
         projectcode int, projectjoindate Timestamp, projectenddate Timestamp, attendance int,
         utilization int, salary Int) STORED AS carbondata"""
    )

    val csvFilePath = s"$resourcesPath/data_beyond68yrs.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE timestamp_nodictionary OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
  }

  test("select projectjoindate, projectenddate from timestamp_nodictionary") {
    checkAnswer(
      sql("select projectjoindate, projectenddate from timestamp_nodictionary"),
      Seq(Row(Timestamp.valueOf("2000-01-29 00:00:00.0"), Timestamp.valueOf("2016-06-29 00:00:00.0")),
        Row(Timestamp.valueOf("1800-02-17 00:00:00.0"), Timestamp.valueOf("1900-11-29 00:00:00.0")),
        Row(null, Timestamp.valueOf("2016-05-29 00:00:00.0")),
        Row(null, Timestamp.valueOf("2016-11-30 00:00:00.0")),
        Row(Timestamp.valueOf("3000-10-22 00:00:00.0"), Timestamp.valueOf("3002-11-15 00:00:00.0")),
        Row(Timestamp.valueOf("1802-06-29 00:00:00.0"), Timestamp.valueOf("1902-12-30 00:00:00.0")),
        Row(null, Timestamp.valueOf("2016-12-30 00:00:00.0")),
        Row(Timestamp.valueOf("2038-11-14 00:00:00.0"), Timestamp.valueOf("2041-12-29 00:00:00.0")),
        Row(null, null),
        Row(Timestamp.valueOf("2014-09-15 00:00:00.0"), Timestamp.valueOf("2016-05-29 00:00:00.0"))
      )
    )
  }


  test("select projectjoindate, projectenddate from timestamp_nodictionary where in filter") {
    checkAnswer(
      sql("select projectjoindate, projectenddate from timestamp_nodictionary where projectjoindate in" +
          "('1800-02-17 00:00:00','3000-10-22 00:00:00') or projectenddate in ('1900-11-29 00:00:00'," +
          "'3002-11-15 00:00:00','2041-12-29 00:00:00')"),
      Seq(Row(Timestamp.valueOf("1800-02-17 00:00:00.0"), Timestamp.valueOf("1900-11-29 00:00:00.0")),
        Row(Timestamp.valueOf("3000-10-22 00:00:00.0"), Timestamp.valueOf("3002-11-15 00:00:00.0")),
        Row(Timestamp.valueOf("2038-11-14 00:00:00.0"), Timestamp.valueOf("2041-12-29 00:00:00.0")))
    )

  }


  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table timestamp_nodictionary")
  }
}