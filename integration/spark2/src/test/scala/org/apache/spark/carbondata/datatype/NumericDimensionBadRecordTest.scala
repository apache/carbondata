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

package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp dataDataTypes
 *
 *
 */
class NumericDimensionBadRecordTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    try {
      sql("drop table IF EXISTS intDataType")
      sql("drop table IF EXISTS longDataType")
      sql("drop table IF EXISTS doubleDataType")
      sql("drop table IF EXISTS floatDataType")
      sql("drop table IF EXISTS bigDecimalDataType")
      sql("drop table IF EXISTS stringDataType")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          new File("./target/test/badRecords")
            .getCanonicalPath)
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      var csvFilePath = ""

      // 1. bad record int DataType dimension
      sql("create table intDataType(name String, dob timestamp, weight int)" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table intDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");
      // 2. bad record long DataType dimension
      sql("create table longDataType(name String, dob timestamp, weight long)" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table longDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");
      // 3. bad record double DataType dimension
      sql("create table doubleDataType(name String, dob timestamp, weight double)" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table doubleDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");

      // 4. bad record float DataType dimension
      sql("create table floatDataType(name String, dob timestamp, weight float)" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table floatDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");
      // 5. bad record decimal DataType dimension
      sql("create table bigDecimalDataType(name String, dob timestamp, weight decimal(3,1))" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table bigDecimalDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");

      // 6. bad record string DataType dimension
      sql("create table stringDataType(name String, dob timestamp, weight String)" +
          " STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='weight')")
      csvFilePath = s"$resourcesPath/badrecords/dummy.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table stringDataType options " +
          "('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORDS_ACTION'='IGNORE')");

    } catch {
      case x: Throwable => {
        System.out.println(x.getMessage)
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
  }

   test("select count(*) from intDataType") {
    checkAnswer(
      sql("select count(*) from intDataType"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from longDataType") {
    checkAnswer(
      sql("select count(*) from longDataType"),
      Seq(Row(2)
      )
    )
  }


  test("select count(*) from doubleDataType") {
    checkAnswer(
      sql("select count(*) from doubleDataType"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from floatDataType") {
    checkAnswer(
      sql("select count(*) from floatDataType"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from bigDecimalDataType") {
    checkAnswer(
      sql("select count(*) from bigDecimalDataType"),
      Seq(Row(2)
      )
    )
  }

  test("select count(*) from stringDataType") {
    checkAnswer(
      sql("select count(*) from stringDataType"),
      Seq(Row(3)
      )
    )
  }

  override def afterAll {
    sql("drop table IF EXISTS intDataType")
    sql("drop table IF EXISTS longDataType")
    sql("drop table IF EXISTS doubleDataType")
    sql("drop table IF EXISTS floatDataType")
    sql("drop table IF EXISTS bigDecimalDataType")
    sql("drop table IF EXISTS stringDataType")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}