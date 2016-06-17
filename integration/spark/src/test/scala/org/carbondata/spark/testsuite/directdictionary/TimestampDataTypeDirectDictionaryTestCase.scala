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

package org.carbondata.spark.testsuite.directdictionary

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, Row}
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants
import org.scalatest.BeforeAndAfterAll


/**
  * Test Class for detailed query on timestamp datatypes
  *
  *
  */
class TimestampDataTypeDirectDictionaryTest extends QueryTest with BeforeAndAfterAll {
  var oc: HiveContext = _

  override def beforeAll {
    try {
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10.00.0")
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY,
          TimeStampGranularityConstants.TIME_GRAN_SEC.toString
        )
      CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
      sql(
        "CREATE CUBE directDictionaryCube DIMENSIONS (empno Integer,doj Timestamp) MEASURES " +
          "(salary Integer) " +
          "OPTIONS (PARTITIONER [PARTITION_COUNT=1])"
      )

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
      var csvFilePath = currentDirectory + "/src/test/resources/datasample.csv"
      sql("LOAD DATA fact from '" + csvFilePath + "' INTO CUBE directDictionaryCube PARTITIONDATA" +
        "(DELIMITER ',', QUOTECHAR '\"')");

    } catch {
      case x: Throwable => CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    }
  }

  test("select doj from directDictionaryCube") {
    checkAnswer(
      sql("select doj from directDictionaryCube"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-04-14 15:00:09.0"))
      )
    )
  }


  test("select doj from directDictionaryCube with equals filter") {
    checkAnswer(
      sql("select doj from directDictionaryCube where doj='2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09")))
    )

  }
  
    test("select doj from directDictionaryCube with greater than filter") {
    checkAnswer(
      sql("select doj from directDictionaryCube where doj>'2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-04-14 15:00:09")))
    )

  }

  test("select count(doj) from directDictionaryCube") {
    checkAnswer(
      sql("select count(doj) from directDictionaryCube"),
      Seq(Row(2))
    )
  }

  override def afterAll {
    sql("drop cube directDictionaryCube")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
  }
}