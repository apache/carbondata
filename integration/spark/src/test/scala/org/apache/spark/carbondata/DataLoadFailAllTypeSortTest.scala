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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for detailed query on timestamp datatypes
 */
class DataLoadFailAllTypeSortTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll: Unit = {
    sql("drop table IF EXISTS data_pm")
    sql("drop table IF EXISTS data_um")
    sql("drop table IF EXISTS data_bm")
    sql("drop table IF EXISTS data_bmf")
    sql("drop table IF EXISTS data_tbm")
    sql("drop table IF EXISTS data_bm_no_good_data")
  }

  test("dataload with parallel merge with bad_records_action='FAIL'") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL");
      sql("create table data_pm(name String, dob long, weight int) " +
          "STORED AS carbondata")
      val testData = s"$resourcesPath/badrecords/dummy.csv"
      sql(s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_pm""")


    } catch {
      case x: Throwable => {
        assert(x.getMessage.contains("Data load failed due to bad record"))
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);
    }
  }

  test("dataload with ENABLE_UNSAFE_SORT='true' with bad_records_action='FAIL'") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true");
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL");
      sql("create table data_um(name String, dob long, weight int) " +
          "STORED AS carbondata")
      val testData = s"$resourcesPath/badrecords/dummy.csv"
      sql(s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_um""")


    } catch {
      case x: Throwable => {
        assert(x.getMessage.contains("Data load failed due to bad record"))
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false");
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);
    }
  }

  test("dataload with bad_records_action='FAIL'") {
    try {
        CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "local_sort")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
      sql("create table data_bm(name String, dob long, weight int) " +
          "STORED AS carbondata")
      val testData = s"$resourcesPath/badrecords/dummy.csv"
      sql(s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_bm""")
    } catch {
      case x: Throwable => {
        assert(x.getMessage.contains("Data load failed due to bad record"))
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT);
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);
    }
  }

  test("dataload with bad_records_action='FORCE'") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "local_sort")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")
      sql("create table data_bmf(name String, dob long, weight int) " +
          "STORED AS carbondata")
      val testData = s"$resourcesPath/badrecords/dummy.csv"
      sql(s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_bmf""")


    } catch {
      case x: Throwable => {
        assert(false)
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    }
  }

  test("dataload with bad_records_action='REDIRECT'") {
    try {
        CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "local_sort")
      sql("create table data_bm_no_good_data(name String, dob long, weight int) " +
          "STORED AS carbondata")
      val testData = s"$resourcesPath/badrecords/dummy2.csv"
      sql(
        s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_bm_no_good_data options
           ('IS_EMPTY_DATA_BAD_RECORD'='true','BAD_RECORDS_ACTION'='REDIRECT')""")
    } catch {
      case x: Throwable => {
        assert(x.getMessage.contains("No Data to load"))
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    }
  }

  test("dataload with table bucketing with bad_records_action='FAIL'") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
      sql("create table data_tbm(name String, dob long, weight int) " +
          "STORED AS carbondata tblproperties('bucketnumber'='4', " +
          "'bucketcolumns'='name', 'tableName'='data_tbm')")
      val testData = s"$resourcesPath/badrecords/dummy.csv"
      sql(s"""LOAD DATA LOCAL INPATH '$testData' INTO table data_tbm""")
    } catch {
      case x: Throwable => {
        assert(x.getMessage.contains("Data load failed due to bad record"))
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      }
    }
    finally {

    }
  }
  //
  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        LoggerAction.FORCE.name())
    sql("drop table IF EXISTS data_pm")
    sql("drop table IF EXISTS data_um")
    sql("drop table IF EXISTS data_bm")
    sql("drop table IF EXISTS data_bmf")
    sql("drop table IF EXISTS data_tbm")
    sql("drop table IF EXISTS data_bm_no_good_data")
  }
}