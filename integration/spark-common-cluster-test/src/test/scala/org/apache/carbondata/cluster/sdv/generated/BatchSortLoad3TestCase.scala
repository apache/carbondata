
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

/**
 * Test Class for BatchSortLoad3TestCase to verify all scenerios
 */

class BatchSortLoad3TestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql(s"""drop table if exists uniqdata20c""").collect
    sql(s"""drop table if exists uniqdata19c""").collect
  }
//PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_020
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_020", Include) {
    sql(
      s"""CREATE TABLE uniqdata20c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
         |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
         |Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'
         | TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""".stripMargin.replaceAll(System
        .lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into
         | table uniqdata20c OPTIONS('DELIMITER'=',' ,'QUOTECHAR'='"','COMMENTCHAR'='#',
         | 'MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT',
         | 'BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,
         | DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,
         | Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin.replaceAll(System
        .lineSeparator, "")).collect

    sql(s"""select * from uniqdata20c""").collect
    sql(s"""drop table  if exists uniqdata20c""").collect

  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_046
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_046", Include) {
    sql(
      s"""CREATE TABLE uniqdata19c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
        DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
        DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
        Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table
         | uniqdata19c OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#',
         | 'MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='IGNORE',
         | 'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,
         | Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(s"""select * from uniqdata19c""").collect
    sql(s"""drop table if exists uniqdata19c""").collect

  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_053
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_053", Include) {
    sql(
      s"""drop table if exists t3""").collect
    sql(
      s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String,
         |serialname String,salary Int,floatField float)
         | STORED BY 'carbondata'""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv' into table t3 options(
         |'COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv','single_pass'='true')"""
        .stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(s"""drop table if exists t3""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_054
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_054", Include) {
    sql(s"""drop table if exists t3""").collect
    sql(
      s"""CREATE TABLE t3 (ID Int, country String, name String, phonetype String,
         |serialname String, salary Int,floatField float)
         | STORED BY 'carbondata'""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/batchsort/data.csv'
         | into table t3 options('ALL_DICTIONARY_PATH'=
         | '$resourcesPath/Data/columndict/data.dictionary','single_pass'='true')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(s"""drop table if exists t3""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_056
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_056", Include) {
    sql(s"""drop table if exists uniqdata20a""").collect
    sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='LOCAL_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='NO_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='NO_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='LOCAL_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdata20a""").collect
    sql(s"""drop table if exists  uniqdata20a""").collect

  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_057
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_057", Include) {
    sql(s"""drop table if exists uniqdata20a""").collect
    sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/batchsort/1000_UniqData.csv' into table  uniqdata20a OPTIONS('DELIMITER'=',' ,'SORT_SCOPE'='BATCH_SORT','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin).collect

    sql(s"""alter table uniqdata20a compact 'minor'""").collect
    sql(s"""drop table if exists  uniqdata20a""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_058
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_058", Include) {
    sql(s"""drop table if exists uniqdata20a""").collect
    sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' , 'SORT_SCOPE'='ABCXYZ',‘SINGLE_PASS’=’true’,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a_hive """).collect
    }
    sql(s"""drop table if exists uniqdata20a""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_059
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_059", Include) {
    sql(s"""drop table if exists uniqdata20a""").collect
    intercept[Exception] {
      sql(s"""CREATE TABLE uniqdata20a (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect

      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a OPTIONS('DELIMITER'=',' , 'SORT_SCOPE'='null',‘SINGLE_PASS’=’true’,'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20a_hive """).collect
    }
    sql(s"""drop table if exists  uniqdata20a""").collect
  }

  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_060
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_060", Include) {
    sql(s"""drop table if exists uniqdata20b""").collect
    sql(s"""drop table if exists uniqdata20c""").collect
    sql(s"""CREATE TABLE uniqdata20b (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata20b OPTIONS('DELIMITER'=',','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""CREATE TABLE uniqdata20c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""insert into uniqdata20c select * from uniqdata20b""")
    sql(s"""drop table if exists  uniqdata20b""").collect
    sql(s"""drop table if exists  uniqdata20c""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_061
  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_061", Include) {
    sql(s"""drop TABLE if exists uniqdata_h""").collect
    sql(s"""drop TABLE if exists uniqdata_c""").collect
    sql(s"""CREATE TABLE uniqdata_h (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect
    sql(s"""load data inpath '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_h""").collect
    sql(s"""CREATE TABLE uniqdata_c (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""insert into uniqdata_c select * from uniqdata_h""")
    sql(s"""drop table if exists  uniqdata_h""").collect
    sql(s"""drop table if exists  uniqdata_c""").collect
  }


  //PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_064
//  test("PTS-AR-Batch_sort_Loading_001-01-01-01_001-TC_064", Include) {
//    sql(s"""drop table if exists uniqdata""").collect
//    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='20160302,20150302')""").collect
//    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
//    sql(s"""drop table if exists uniqdata""").collect
//  }

  override def afterAll {
  }
}