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

package org.carbondata.integration.spark.testsuite.detailquery

import java.io.File
import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, Row}
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext

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
    val file: File = new File("")
    val rootPath: String = file.getAbsolutePath
    val hdfsCarbonBasePath = rootPath + "/../integration/spark/target/metastore"
    val sc = new SparkContext(new SparkConf()
      .setAppName("CarbonSpark")
      .setMaster("local[2]"))
    oc = new CarbonContext(sc, hdfsCarbonBasePath)
    oc.setConf("carbon.kettle.home", rootPath + "/../processing/carbonplugins/")

    CarbonProperties.getInstance().addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10.00.0")
    CarbonProperties.getInstance().addProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY, TimeStampGranularityConstants.TIME_GRAN_SEC.toString)


    oc.sql("CREATE CUBE directDictionaryCube DIMENSIONS (empno Integer,doj Timestamp) MEASURES (salary Integer) " +
      "OPTIONS (PARTITIONER [PARTITION_COUNT=1])")

    val dataFile :String = rootPath + "/../integration/spark/src/test/resources/datasample.csv"
    oc.sql("LOAD DATA fact from "+"'"+ dataFile +"'"+" INTO CUBE directDictionaryCube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }

  test("select doj from directDictionaryCube") {
    checkAnswer(
      oc.sql("select doj from directDictionaryCube"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-04-14 15:00:09.0"))
        ))

  }

  override def afterAll {
    oc.sql("drop cube directDictionaryCube")
  }
}