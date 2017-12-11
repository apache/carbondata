
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
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.api.CarbonProperties

/**
 * Test Class for BatchSortLoad2TestCase to verify all scenerios
 */

class BatchSortLoad2TestCase extends QueryTest with BeforeAndAfterAll {
         

  //To load data after setting only sort scope in carbon property file
  test("Batch_sort_Loading_001-01-01-01_001-TC_027", Include) {
     sql(s"""drop table if exists uniqdata11""").collect
   sql(s"""CREATE TABLE uniqdata11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata11""").collect

     sql(s"""drop table uniqdata11""").collect
  }


  //To load 1 lac data load after setting only sort scope in carbon property file
  test("Batch_sort_Loading_001-01-01-01_001-TC_028", Include) {
    sql(s"""drop table if exists uniqdata12""").collect
     sql(s"""CREATE TABLE uniqdata12 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/1lac_UniqData.csv' into table uniqdata12 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata12""").collect

     sql(s"""drop table uniqdata12""").collect
  }


  //To load data after setting only sort scope in carbon property file with option file header in load
  test("Batch_sort_Loading_001-01-01-01_001-TC_029", Include) {
    sql(s"""drop table if exists uniqdata12a""").collect
     sql(s"""CREATE TABLE uniqdata12a(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata12a OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata12a""").collect

     sql(s"""drop table uniqdata12a""").collect
  }


  //To load data after setting only sort scope in carbon property file without folder path in load
  test("Batch_sort_Loading_001-01-01-01_001-TC_030", Include) {
    try {
     sql(s"""CREATE TABLE uniqdata13 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
      sql(s"""LOAD DATA  into table uniqdata13 OPTIONS('DELIMITER'=',' , 'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table uniqdata13""").collect
  }


  //To load data after setting only sort scope in carbon property file without table_name in load
  test("Batch_sort_Loading_001-01-01-01_001-TC_031", Include) {
    try {
     sql(s"""CREATE TABLE uniqdata14 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
      sql(s"""LOAD DATA  INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table OPTIONS('DELIMITER'=',' , 'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table uniqdata14""").collect
  }


  //To load data after setting only sort scope in carbon property file with option QUOTECHAR'='"'
  test("Batch_sort_Loading_001-01-01-01_001-TC_032", Include) {
     sql(s"""CREATE TABLE uniqdata15 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata15 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata15""").collect

     sql(s"""drop table uniqdata15""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS('COMMENTCHAR'='#')

  test("Batch_sort_Loading_001-01-01-01_001-TC_033", Include) {
     sql(s"""CREATE TABLE uniqdata16 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata16 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata16""").collect

     sql(s"""drop table uniqdata16""").collect
  }


  //To load data after setting only sort scope in carbon property file with option 'MULTILINE'='true'
  test("Batch_sort_Loading_001-01-01-01_001-TC_034", Include) {
     sql(s"""CREATE TABLE uniqdata17 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata17 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata17""").collect

     sql(s"""drop table uniqdata17""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS('ESCAPECHAR'='\')
  test("Batch_sort_Loading_001-01-01-01_001-TC_035", Include) {
     sql(s"""CREATE TABLE uniqdata18 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata18 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata18""").collect

     sql(s"""drop table uniqdata18""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS 'BAD_RECORDS_ACTION'='FORCE'
  test("Batch_sort_Loading_001-01-01-01_001-TC_036", Include) {
     sql(s"""CREATE TABLE uniqdata19b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19b OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata19b""").collect

     sql(s"""drop table uniqdata19b""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS 'BAD_RECORDS_ACTION'='IGNORE'
  test("Batch_sort_Loading_001-01-01-01_001-TC_037", Include) {
     sql(s"""CREATE TABLE uniqdata19c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19c OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata19c""").collect

     sql(s"""drop table uniqdata19c""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS 'BAD_RECORDS_ACTION'='REDIRECT'
  test("Batch_sort_Loading_001-01-01-01_001-TC_038", Include) {
     sql(s"""CREATE TABLE uniqdata19d (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19d OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata19d""").collect

     sql(s"""drop table uniqdata19d""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS 'BAD_RECORDS_LOGGER_ENABLE'='FALSE'
  test("Batch_sort_Loading_001-01-01-01_001-TC_039", Include) {
     sql(s"""CREATE TABLE uniqdata19e (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19e OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata19e""").collect

     sql(s"""drop table uniqdata19e""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS 'BAD_RECORDS_LOGGER_ENABLE'='TRUE'
  test("Batch_sort_Loading_001-01-01-01_001-TC_040", Include) {
     sql(s"""CREATE TABLE uniqdata19f (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata19f OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata19f""").collect

     sql(s"""drop table uniqdata19f""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS ‘SINGLE_PASS’=’true’
  test("Batch_sort_Loading_001-01-01-01_001-TC_041", Include) {
     sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',','QUOTECHAR'='"','SINGLE_PASS'='TRUE','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata20a""").collect

     sql(s"""drop table uniqdata20a""").collect
  }


  //To load data after setting only sort scope in carbon property file with OPTIONS ‘SINGLE_PASS’=’false’
  test("Batch_sort_Loading_001-01-01-01_001-TC_042", Include) {
     sql(s"""CREATE TABLE uniqdata20b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20b OPTIONS('DELIMITER'=',','QUOTECHAR'='"','SINGLE_PASS'='FALSE','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata20b""").collect

     sql(s"""drop table uniqdata20b""").collect
  }


  //To load data after setting only sort scope in carbon property file with NO_INVERTED_INDEX
  test("Batch_sort_Loading_001-01-01-01_001-TC_043", Include) {
     sql(s"""CREATE TABLE uniqdata20c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20c OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata20c""").collect

     sql(s"""drop table uniqdata20c""").collect
  }


  //To load data after setting only sort scope in carbon property file with COLUMNDICT
  test("Batch_sort_Loading_001-01-01-01_001-TC_044", Include) {
     sql(s"""drop table if exists t3""").collect
   sql(s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table t3""").collect
  }


  //To load data after setting only sort scope in carbon property file with ALL_DICTIONARY_PATH
  test("Batch_sort_Loading_001-01-01-01_001-TC_045", Include) {
    sql(s"""drop table if exists t3""").collect
     sql(s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table t3""").collect
  }


  //To check incremental load one with batch_sort
  test("Batch_sort_Loading_001-01-01-01_001-TC_047", Include) {
     sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata' TBLPROPERTIES ('SORT_SCOPE'='BATCH_SORT')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""select * from uniqdata20a""").collect
   sql(s"""drop table uniqdata20a""").collect
  }


  //To check sort_scope option with a wrong value
  test("Batch_sort_Loading_001-01-01-01_001-TC_049", Include) {
    try {
     sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' , 'SORT_SCOPE'='ABCXYZ',‘SINGLE_PASS’=’true’,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table uniqdata20a""").collect
  }


  //To check sort_scope option with null value
  test("Batch_sort_Loading_001-01-01-01_001-TC_050", Include) {
    try {
     sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' , 'SORT_SCOPE'='null',‘SINGLE_PASS’=’true’,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table uniqdata20a""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.load.sort.scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.load.sort.scope", "batch_sort")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.load.sort.scope", p1)
  }
       
}