
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.TestQueryExecutor

/**
 * Test Class for singlepassTestCase to verify all scenerios
 */

class SinglepassTestCase extends QueryTest with BeforeAndAfterAll {
         

  //To check data loading with OPTIONS ‘SINGLE_PASS’=’true’
  test("Loading-004-01-01-01_001-TC_001", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test1""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_001")
     sql(s"""drop table test1""").collect
  }


  //To check data loading with OPTIONS ‘SINGLE_PASS’=’false’
  test("Loading-004-01-01-01_001-TC_002", Include) {
     sql(s"""create table test1(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test1""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_002")

  }


  //To check data loading from CSV with incomplete data
  test("Loading-004-01-01-01_001-TC_003", Include) {
    intercept[Exception] {
     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_incomplete.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading from CSV with bad records
  test("Loading-004-01-01-01_001-TC_004", Include) {
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_badrec.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading from CSV with no data
  test("Loading-004-01-01-01_001-TC_005", Include) {
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_nodata.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading from CSV with incomplete data
  test("Loading-004-01-01-01_001-TC_006", Include) {
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_incomplete.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading from CSV with wrong data
  test("Loading-004-01-01-01_001-TC_007", Include) {
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_incomplete.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading from CSV with no data and 'SINGLEPASS' = 'FALSE'
  test("Loading-004-01-01-01_001-TC_008", Include) {
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_nodata.csv.csv' INTO TABLE uniqdata OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    }
  }


  //To check data loading using  'SINGLE_PASS'='NULL/any invalid string'
  test("Loading-004-01-01-01_001-TC_009", Include) {
     sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='NULL', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test1""",
      Seq(Row(198)), "singlepassTestCase_Loading-004-01-01-01_001-TC_009")
     sql(s"""drop table test1""").collect
  }


  //To check data load using multiple CSV from folder into table with single_pass=true
  test("Loading-004-01-01-01_001-TC_010", Include) {
     sql(s"""drop table if exists emp_record12""").collect
   sql(s"""create table emp_record12 (ID int,Name string,DOJ timestamp,Designation string,Salary double,Dept string,DOB timestamp,Addr string,Gender string,Mob bigint) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA inpath '$resourcesPath/Data/singlepass/data' into table emp_record12 options('DELIMITER'=',', 'QUOTECHAR'='"','SINGLE_PASS'='TRUE','FILEHEADER'='ID,Name,DOJ,Designation,Salary,Dept,DOB,Addr,Gender,Mob','BAD_RECORDS_ACTION'='FORCE')""").collect
    sql(s"""select count(*) from emp_record12""").collect

     sql(s"""drop table emp_record12""").collect
  }


  //To check data load using CSV from multiple level of folders into table
  test("Loading-004-01-01-01_001-TC_011", Include) {
     sql(s"""create table emp_record12 (ID int,Name string,DOJ timestamp,Designation string,Salary double,Dept string,DOB timestamp,Addr string,Gender string,Mob bigint) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA inpath '$resourcesPath/Data/singlepass/data' into table emp_record12 options('DELIMITER'=',', 'QUOTECHAR'='"','SINGLE_PASS'='TRUE','FILEHEADER'='ID,Name,DOJ,Designation,Salary,Dept,DOB,Addr,Gender,Mob','BAD_RECORDS_ACTION'='FORCE')""").collect
    sql(s"""select count(*) from emp_record12""").collect

     sql(s"""drop table emp_record12""").collect
  }


  //To check data load using multiple CSV from folder into table with single_pass=false
  test("Loading-004-01-01-01_001-TC_012", Include) {
     sql(s"""create table emp_record12 (ID int,Name string,DOJ timestamp,Designation string,Salary double,Dept string,DOB timestamp,Addr string,Gender string,Mob bigint) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA inpath '$resourcesPath/Data/singlepass/data' into table emp_record12 options('DELIMITER'=',', 'QUOTECHAR'='"','SINGLE_PASS'='FALSE','FILEHEADER'='ID,Name,DOJ,Designation,Salary,Dept,DOB,Addr,Gender,Mob','BAD_RECORDS_ACTION'='FORCE')""").collect
    sql(s"""select count(*) from emp_record12""").collect

     sql(s"""drop table emp_record12""").collect
  }


  //To check data load using CSV from multiple level of folders into table
  test("Loading-004-01-01-01_001-TC_013", Include) {
     sql(s"""create table emp_record12 (ID int,Name string,DOJ timestamp,Designation string,Salary double,Dept string,DOB timestamp,Addr string,Gender string,Mob bigint) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA inpath '$resourcesPath/Data/singlepass/data' into table emp_record12 options('DELIMITER'=',', 'QUOTECHAR'='"','SINGLE_PASS'='FALSE','FILEHEADER'='ID,Name,DOJ,Designation,Salary,Dept,DOB,Addr,Gender,Mob','BAD_RECORDS_ACTION'='FORCE')""").collect
    sql(s"""select count(*) from emp_record12""").collect

     sql(s"""drop table emp_record12""").collect
  }


  //To check Data loading in proper CSV format with .dat
  test("Loading-004-01-01-01_001-TC_014", Include) {
     sql(s"""drop table if exists uniqdata_file_extn""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.dat' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_014")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format with .xls
  test("Loading-004-01-01-01_001-TC_015", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.xls' into table uniqdata_file_extn OPTIONS('DELIMITER'='\001' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_015")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format  with .doc
  test("Loading-004-01-01-01_001-TC_016", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.dat' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_016")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format  with .txt
  test("Loading-004-01-01-01_001-TC_017", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.txt' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_017")
     sql(s"""drop table uniqdata_file_extn""").collect
  }



  //To check Data loading in proper CSV format  wiithout any extension
  test("Loading-004-01-01-01_001-TC_020", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_020")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format with .dat with single_pass=false
  test("Loading-004-01-01-01_001-TC_021", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.dat' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_021")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format with .xls with single_pass=false
  test("Loading-004-01-01-01_001-TC_022", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.xls' into table uniqdata_file_extn OPTIONS('DELIMITER'='\001' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_022")
     sql(s"""drop table uniqdata_file_extn""").collect
  }



  //To check Data loading in proper CSV format  with .txt with single_pass=false
  test("Loading-004-01-01-01_001-TC_024", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData.txt' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_024")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading in proper CSV format  wiithout any extension with single_pass=false
  test("Loading-004-01-01-01_001-TC_027", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_file_extn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA  inpath '$resourcesPath/Data/singlepass/2000_UniqData' into table uniqdata_file_extn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_file_extn""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_027")
     sql(s"""drop table uniqdata_file_extn""").collect
  }


  //To check Data loading with delimiters  as / [slash]
  test("Loading-004-01-01-01_001-TC_028", Include) {
     sql(s"""drop table if exists uniqdata_slash""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_slash(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_slash.csv' into table uniqdata_slash OPTIONS('DELIMITER'='/' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_slash""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_028")
     sql(s"""drop table uniqdata_slash""").collect
  }


  //To check Data loading with delimiters  as " [double quote]
  test("Loading-004-01-01-01_001-TC_029", Include) {
     sql(s"""drop table if exists uniqdata_doublequote""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_doublequote (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_quote.csv' into table uniqdata_doublequote OPTIONS('DELIMITER'='"' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_doublequote""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_029")
     sql(s"""drop table uniqdata_doublequote""").collect
  }


  //To check Data loading with delimiters  as  ! [exclamation]
  test("Loading-004-01-01-01_001-TC_030", Include) {
     sql(s"""drop table if exists uniqdata_exclamation""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_exclamation (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_exclamation.csv' into table uniqdata_exclamation OPTIONS('DELIMITER'='!' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_exclamation""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_030")
     sql(s"""drop table uniqdata_exclamation""").collect
  }


  //To check Data loading with delimiters  as  | [pipeline]
  test("Loading-004-01-01-01_001-TC_031", Include) {
     sql(s"""drop table if exists uniqdata_pipe""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_pipe (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_pipe.csv' into table uniqdata_pipe OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_pipe""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_031")
     sql(s"""drop table uniqdata_pipe""").collect
  }


  //To check Data loading with delimiters  as ' [single quota]
  test("Loading-004-01-01-01_001-TC_032", Include) {
     sql(s"""drop table if exists uniqdata_singleQuote""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_singleQuote (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_singlequote.csv' into table uniqdata_singleQuote OPTIONS('DELIMITER'="'" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_singleQuote""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_032")
     sql(s"""drop table uniqdata_singleQuote""").collect
  }


  //To check Data loading with delimiters  as \017
  test("Loading-004-01-01-01_001-TC_033", Include) {
     sql(s"""drop table if exists uniqdata_017""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_017 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_017.csv' into table uniqdata_017 OPTIONS('DELIMITER'="\017" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_017""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_033")
     sql(s"""drop table uniqdata_017""").collect
  }


  //To check Data loading with delimiters  as \001
  test("Loading-004-01-01-01_001-TC_034", Include) {
     sql(s"""drop table if exists uniqdata_001""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_001.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
    checkAnswer(s"""select count(*) from uniqdata_001""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_034")
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Data loading with delimiters  as / [slash]  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_035", Include) {
     sql(s"""drop table if exists uniqdata_slash""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_slash(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_slash.csv' into table uniqdata_slash OPTIONS('DELIMITER'='/' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_slash""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_035")
     sql(s"""drop table uniqdata_slash""").collect
  }


  //To check Data loading with delimiters  as " [double quote]  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_036", Include) {
     sql(s"""drop table if exists uniqdata_doublequote""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_doublequote (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_quote.csv' into table uniqdata_doublequote OPTIONS('DELIMITER'='"' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_doublequote""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_036")
     sql(s"""drop table uniqdata_doublequote""").collect
  }


  //To check Data loading with delimiters  as  ! [exclamation]  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_037", Include) {
     sql(s"""drop table if exists uniqdata_exclamation""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_exclamation (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_exclamation.csv' into table uniqdata_exclamation OPTIONS('DELIMITER'='!' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_exclamation""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_037")
     sql(s"""drop table uniqdata_exclamation""").collect
  }


  //To check Data loading with delimiters  as  | [pipeline]  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_038", Include) {
     sql(s"""drop table if exists uniqdata_pipe""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_pipe (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_pipe.csv' into table uniqdata_pipe OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_pipe""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_038")
     sql(s"""drop table uniqdata_pipe""").collect
  }


  //To check Data loading with delimiters  as ' [single quota]  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_039", Include) {
     sql(s"""drop table if exists uniqdata_singleQuote""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_singleQuote (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_singlequote.csv' into table uniqdata_singleQuote OPTIONS('DELIMITER'="'" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_singleQuote""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_039")
     sql(s"""drop table uniqdata_singleQuote""").collect
  }


  //To check Data loading with delimiters  as \017  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_040", Include) {
     sql(s"""drop table if exists uniqdata_017""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_017 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_017.csv' into table uniqdata_017 OPTIONS('DELIMITER'="\017" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata_017""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_040")
     sql(s"""drop table uniqdata_017""").collect
  }


  //To check Data loading with delimiters  as \001  and SINGLE_PASS= FALSE
  test("Loading-004-01-01-01_001-TC_041", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData_001.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
    checkAnswer(s"""select count(*) from uniqdata_001""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_041")
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Auto compaction is successful with carbon.enable.auto.load.merge= True & SINGLE_PASS=TRUE
  test("Loading-004-01-01-01_001-TC_043", Include) {
     sql(s"""drop table if exists uniqdata_001""").collect
   sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""alter table uniqdata_001 compact 'minor'""").collect
    sql(s"""show segments for table uniqdata_001""").collect
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Auto compaction is successful with carbon.enable.auto.load.merge= True & SINGLE_PASS=FALSE
  test("Loading-004-01-01-01_001-TC_044", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect
   sql(s"""alter table uniqdata_001 compact 'minor'""").collect
    sql(s"""show segments for table uniqdata_001""").collect
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Auto compaction is successful with carbon.enable.auto.load.merge= false & SINGLE_PASS=TRUE
  test("Loading-004-01-01-01_001-TC_045", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""alter table uniqdata_001 compact 'major'""").collect
    sql(s"""show segments for table uniqdata_001""").collect
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Auto compaction is successful with carbon.enable.auto.load.merge= false & SINGLE_PASS=FALSE
  test("Loading-004-01-01-01_001-TC_046", Include) {
     sql(s"""CREATE TABLE if not exists uniqdata_001 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/2000_UniqData.csv' into table uniqdata_001 OPTIONS('DELIMITER'="\001" , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'=':')""").collect

   sql(s"""alter table uniqdata_001 compact 'major'""").collect
    sql(s"""show segments for table uniqdata_001""").collect
     sql(s"""drop table uniqdata_001""").collect
  }


  //To check Data loading is success with 'SINGLE_PASS'='TRUE' with already created table with Include dictionary
  test("Loading-004-01-01-01_001-TC_051", Include) {
     sql(s"""create database includeexclude""").collect
   sql(s"""use includeexclude""").collect
   sql(s"""create table test2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_051")
     sql(s"""drop table includeexclude.test2""").collect
   sql(s"""drop database includeexclude cascade""").collect
  }


  //To check Data loading is success with 'SINGLE_PASS'='FALSE' with already created table with Include dictionary
  test("Loading-004-01-01-01_001-TC_052", Include) {
     sql(s"""create database includeexclude""").collect
   sql(s"""use includeexclude""").collect
   sql(s"""create table test2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_052")
     sql(s"""drop table includeexclude.test2""").collect
   sql(s"""use default""").collect
   sql(s"""drop database includeexclude cascade""").collect
  }


  //To check Data loading is success with 'SINGLE_PASS'='TRUE' with already created table with Exclude dictionary
  test("Loading-004-01-01-01_001-TC_053", Include) {
     sql(s"""drop table if exists test2""").collect
   sql(s"""create table test2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_053")
     sql(s"""drop table test2""").collect
  }


  //To check Data loading is success with 'SINGLE_PASS'='FALSE' with already created table with Exclude dictionary
  test("Loading-004-01-01-01_001-TC_054", Include) {
     sql(s"""create table test2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId int,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='FALSE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_054")
     sql(s"""drop table test2""").collect
  }


  //To check data loading is success when loading from Carbon Table using ‘SINGLE_PASS’=TRUE
  test("Loading-004-01-01-01_001-TC_061", Include) {
     sql(s"""create table test1(imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/vardhandaterestruct.csv' INTO TABLE test1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','SINGLE_PASS'='TRUE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from test1""",
      Seq(Row(99)), "singlepassTestCase_Loading-004-01-01-01_001-TC_061")
     sql(s"""drop table test1""").collect
  }


  //Verifying load data with single Pass true and BAD_RECORDS_ACTION= ='FAIL
  test("Loading-004-01-01-01_001-TC_067", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    intercept[Exception] {
      sql(s"""
             | CREATE TABLE uniqdata(
             | shortField SHORT,
             | booleanField BOOLEAN,
             | intField INT,
             | bigintField LONG,
             | doubleField DOUBLE,
             | stringField STRING,
             | decimalField DECIMAL(18,2),
             | charField CHAR(5),
             | floatField FLOAT,
             | complexData ARRAY<STRING>,
             | booleanField2 BOOLEAN
             | )
             | STORED BY 'carbondata'
       """.stripMargin)

        .collect


      sql(
        s"""LOAD DATA INPATH  '${TestQueryExecutor
          .integrationPath}/spark2/src/test/resources/bool/supportBooleanBadRecords.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2','SINGLE_Pass'='true')""".stripMargin)
        .collect
      checkAnswer(
        s"""select count(*) from uniqdata""",
        Seq(Row(2013)),
        "singlepassTestCase_Loading-004-01-01-01_001-TC_067")
  }
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true and BAD_RECORDS_ACTION= ='REDIRECT'
  test("Loading-004-01-01-01_001-TC_071", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_071")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass false and BAD_RECORDS_ACTION= ='REDIRECT'
  test("Loading-004-01-01-01_001-TC_072", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_072")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true and BAD_RECORDS_ACTION= ='IGNORE'
  test("Loading-004-01-01-01_001-TC_073", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_073")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass false and BAD_RECORDS_ACTION= ='IGNORE'
  test("Loading-004-01-01-01_001-TC_074", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_074")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true and BAD_RECORDS_ACTION= ='FORCE'
  test("Loading-004-01-01-01_001-TC_075", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_075")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass false and BAD_RECORDS_ACTION= ='FORCE'
  test("Loading-004-01-01-01_001-TC_076", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_076")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass false and 'BAD_RECORDS_LOGGER_ENABLE'='TRUE',
  test("Loading-004-01-01-01_001-TC_077", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_077")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass false and 'BAD_RECORDS_LOGGER_ENABLE'='FALSE',
  test("Loading-004-01-01-01_001-TC_078", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_078")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true and 'BAD_RECORDS_LOGGER_ENABLE'='TRUE',
  test("Loading-004-01-01-01_001-TC_079", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "singlepassTestCase_Loading-004-01-01-01_001-TC_079")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true, NO_INVERTED_INDEX, and dictionary_exclude
  test("Loading-004-01-01-01_001-TC_080", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME','dictionary_exclude'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(10)), "singlepassTestCase_Loading-004-01-01-01_001-TC_080")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single Pass true, NO_INVERTED_INDEX and dictionary_include a measure
  test("Loading-004-01-01-01_001-TC_081", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_Pass'='true')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(10)), "singlepassTestCase_Loading-004-01-01-01_001-TC_081")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single pass=false and column dictionary path
  test("Loading-004-01-01-01_001-TC_084", Include) {
    dropTable("uniqdata")
    intercept[Exception] {
      sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format'""")

        .collect
      sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_PASS'='false','COLUMNDICT'='CUST_NAME:$resourcesPath/Data/singlepass/data/cust_name.txt')""")
        .collect
      checkAnswer(
        s"""select count(*) from uniqdata""",
        Seq(Row(10)),
        "singlepassTestCase_Loading-004-01-01-01_001-TC_084")
    }
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying load data with single pass=true and column dictionary path
  test("Loading-004-01-01-01_001-TC_085", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_PASS'='true','COLUMNDICT'='CUST_NAME:$resourcesPath/Data/singlepass/data/cust_name.txt')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(10)), "singlepassTestCase_Loading-004-01-01-01_001-TC_085")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying single pass false with all dimensions as dictionary_exclude and dictionary_include
  test("Loading-004-01-01-01_001-TC_088", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME','DICTIONARY_INCLUDE'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_PASS'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(10)), "singlepassTestCase_Loading-004-01-01-01_001-TC_088")
     sql(s"""drop table uniqdata""").collect
  }


  //Verifying single pass true with all dimensions as dictionary_exclude and dictionary_include
  test("Loading-004-01-01-01_001-TC_089", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE if not exists uniqdata (CUST_ID int,CUST_NAME String, DOB timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME','DICTIONARY_INCLUDE'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/singlepass/data/10_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,DOB','SINGLE_PASS'='false')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(10)), "singlepassTestCase_Loading-004-01-01-01_001-TC_089")
     sql(s"""drop table uniqdata""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.enable.auto.load.merge", CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.enable.auto.load.merge", "true")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.enable.auto.load.merge", p1)
  }
       
}