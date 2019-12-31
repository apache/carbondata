
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
  * Test Class for globalsort1TestCase to verify all scenerios
  */

class GlobalSortTestCase extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach{

  override def beforeAll {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
  }

  override def beforeEach(): Unit = {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
  }

  //Carbon-Loading-Optimizations-Global-Sort-01-01-01
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-01", Include) {
    sql(s"""drop table if exists uniqdata11""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,
         |ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int) STORED BY 'carbondata'""".stripMargin.replaceAll(System
        .lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv'
         | into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"',
         | 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,
         | DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,
         | Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-02
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-02", Include) {
    sql(
      s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
         |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
         |Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table
         | uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#',
         | 'MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='FORCE',
         | 'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect


    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-03
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-03", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/folder1/folder2' into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-04
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-04", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/folder1' into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-05
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-05", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','maxcolumns'='13','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-06
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-06", Include) {
    sql(s"""CREATE TABLE uniqdata17 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata17 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata17""").collect
    sql(s"""drop table if exists uniqdata17""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-07
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-07", Include) {
    sql(s"""CREATE TABLE uniqdata19b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19b OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19b""").collect
    sql(s"""drop table if exists uniqdata19b""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-08
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-08", Include) {
    sql(s"""CREATE TABLE uniqdata19c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19c OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19c""").collect
    sql(s"""drop table if exists uniqdata19c""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-09
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-09", Include) {
    sql(s"""CREATE TABLE uniqdata19d (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19d OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19d""").collect
    sql(s"""drop table if exists uniqdata19d""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-10
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-10", Include) {
    sql(s"""CREATE TABLE uniqdata19e (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19e OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19e""").collect
    sql(s"""drop table if exists uniqdata19e""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-11
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-11", Include) {
    sql(s"""CREATE TABLE uniqdata19f (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19f OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin).collect

    sql(s"""select * from uniqdata19f""").collect
    sql(s"""drop table if exists uniqdata19f""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-14
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-14", Include) {
    sql(
      s"""CREATE TABLE uniqdata20c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20c OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata20c""").collect
    sql(s"""drop table if exists uniqdata20c""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-15
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-15", Include) {
    sql(s"""drop table if exists t3""").collect
    sql(s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(
    s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv' into table t3 options('GLOBAL_SORT_PARTITIONS'='2')""".stripMargin).collect

    sql(s"""select * from t3""").collect
    sql(s"""drop table if exists t3""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-16
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-16", Include) {
    sql(s"""drop table if exists t3""").collect
    sql(s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv' into table t3 options('GLOBAL_SORT_PARTITIONS'='2')""").collect

    sql(s"""select * from t3""").collect
    sql(s"""drop table if exists t3""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-19
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-19", Include) {
    sql(s"""drop table if exists uniqdata20b""").collect
    sql(s"""drop table if exists uniqdata20c""").collect
    sql(s"""CREATE TABLE uniqdata20b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20b OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""CREATE TABLE uniqdata20c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""insert into uniqdata20c select * from uniqdata20b""").collect

    sql(s"""select * from uniqdata20b""").collect
    sql(s"""drop table if exists uniqdata20b""").collect
    sql(s"""drop table if exists uniqdata20c""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-20
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-20", Include) {
    sql(s"""drop table if exists uniqdata_h""").collect
    sql(s"""drop table if exists uniqdata_c""").collect
    sql(s"""CREATE TABLE uniqdata_h (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect
    sql(s"""load data inpath '$resourcesPath/Data/uniqdata/2000_UniqData_hive2.csv' into table uniqdata_h""").collect
    sql(s"""CREATE TABLE uniqdata_c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""insert into uniqdata_c select * from uniqdata_h""").collect

    sql(s"""select * from uniqdata_c""").collect
    sql(s"""drop table if exists uniqdata_h""").collect
    sql(s"""drop table if exists uniqdata_c""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-21
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-21", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect
  }

  //Carbon-Loading-Optimizations-Global-Sort-01-01-22
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-22", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata11 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-23
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-23", Include) {
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata11 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect


    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-24
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-24", Include) {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/folder1/folder2' into table uniqdata11 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-25
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-25", Include) {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/folder1' into table uniqdata11 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-26
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-26", Include) {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata11 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','maxcolumns'='13','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata11""").collect
    sql(s"""drop table if exists uniqdata11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-27
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-27", Include) {
    sql(s"""drop table if exists uniqdata17""").collect
    sql(s"""CREATE TABLE uniqdata17 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata17 OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata17""").collect
    sql(s"""drop table if exists uniqdata17""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-28
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-28", Include) {
    sql(s"""drop table if exists uniqdata19b""").collect
    sql(s"""CREATE TABLE uniqdata19b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19b OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19b""").collect
    sql(s"""drop table if exists uniqdata19b""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-29
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-29", Include) {
    sql(s"""drop table if exists uniqdata19c""").collect
    sql(s"""CREATE TABLE uniqdata19c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19c OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19c""").collect
    sql(s"""drop table if exists uniqdata19c""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-30
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-30", Include) {
    sql(s"""drop table if exists uniqdata19d""").collect
    sql(s"""CREATE TABLE uniqdata19d (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19d OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19d""").collect
    sql(s"""drop table if exists uniqdata19d""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-31
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-31", Include) {
    sql(s"""drop table if exists uniqdata19e""").collect
    sql(s"""CREATE TABLE uniqdata19e (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19e OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19e""").collect
    sql(s"""drop table if exists uniqdata19e""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-32
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-32", Include) {
    sql(s"""drop table if exists uniqdata19f""").collect
    sql(s"""CREATE TABLE uniqdata19f (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19f OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata19f""").collect
    sql(s"""drop table if exists uniqdata19f""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-36
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-36", Include) {
    sql(s"""drop TABLE if exists uniqdata_c""").collect
    sql(s"""CREATE TABLE uniqdata_c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_c OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""delete from uniqdata_c where CUST_NAME='CUST_NAME_20000'""").collect

    sql(s"""select * from uniqdata_c""").collect
    sql(s"""drop TABLE if exists uniqdata_c""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-38
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-38", Include) {
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('GLOBAL_SORT_PARTITIONS'='2','DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata""").collect
    sql(s"""drop TABLE if exists uniqdata""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-39
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-39", Include) {
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select count(*) from uniqdataquery1 where cust_name="CUST_NAME_00000" group by cust_name""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-40
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-40", Include) {
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select count(*) from uniqdataquery1 where cust_name IN(1,2,3) group by cust_name""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-41
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-41", Include) {
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdataquery1 where cust_id between 9002 and 9030""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-42
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-42", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    intercept[Exception] {
      sql(s"""select * from uniqdataquery1 where Is NulL""").collect
    }
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-43
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-43", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdataquery1 where cust_id IS NOT NULL""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-44
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-44", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from (select cust_id from uniqdataquery1 where cust_id IN (10987,10988)) uniqdataquery1 where cust_id IN (10987, 10988)""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-45
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-45", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdataquery11 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 join uniqdataquery11 where uniqdataquery1.CUST_ID > 10700 and uniqdataquery11.CUST_ID > 10500""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-46
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-46", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdataquery11 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 LEFT join uniqdataquery11 where uniqdataquery1.CUST_ID > 10000""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-47
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-47", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdataquery11 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 FULL JOIN uniqdataquery11 where uniqdataquery1.CUST_ID=uniqdataquery11.CUST_ID""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-48
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-48", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
    sql(s"""CREATE TABLE uniqdataquery11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdataquery11 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select broadcast.cust_id from uniqdataquery1 broadcast join uniqdataquery11 where broadcast.cust_id > 10900""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""drop table if exists uniqdataquery11""").collect
  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-49
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-49", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_id asc""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-50
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-50", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_name desc""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-51
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-51", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_name desc, cust_id asc""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-52
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-52", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select cust_id,avg(cust_id) from uniqdataquery1 where cust_id IN (select cust_id from uniqdataquery1) group by cust_id""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }


  //Carbon-Loading-Optimizations-Global-Sort-01-01-54
  test("Carbon-Loading-Optimizations-Global-Sort-01-01-54", Include) {
    sql(s"""drop table if exists uniqdataquery1""").collect
    sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' tblproperties('sort_columns'='')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select cust_id,avg(cust_id) from uniqdataquery1 where cust_id IN (select cust_id from uniqdataquery1) group by cust_id""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect

  }

  override def afterAll: Unit = {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
  }

  override def afterEach: Unit = {
    sql(s"""drop table if exists uniqdata11""").collect
    sql(s"""drop table if exists uniqdataquery1""").collect
  }
}