
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

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for DataLoadingTestCase to verify all scenerios
 */

class DataLoadingTestCase extends QueryTest with BeforeAndAfterAll {



  //Data load--->Action--->Redirect--->Logger-->True
  test("BadRecord_Dataload_001", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "DataLoadingTestCase-BadRecord_Dataload_001")
     sql(s"""drop table uniqdata""").collect
  }


  //Data load--->Action--->FORCE--->Logger-->True
  test("BadRecord_Dataload_002", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "DataLoadingTestCase-BadRecord_Dataload_002")
     sql(s"""drop table uniqdata""").collect
  }


  //Data load--->Action--->IGNORE--->Logger-->True
  test("BadRecord_Dataload_003", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2010)), "DataLoadingTestCase-BadRecord_Dataload_003")
     sql(s"""drop table uniqdata""").collect
  }


  //Data load--->Action--->Ignore--->Logger-->False
  test("BadRecord_Dataload_004", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s""" CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='IGNORE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2010)), "DataLoadingTestCase-BadRecord_Dataload_004")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Data load--->Action--->FORCE--->Logger-->False
  test("BadRecord_Dataload_005", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s""" CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2013)), "DataLoadingTestCase-BadRecord_Dataload_005")
     sql(s"""drop table uniqdata""").collect
  }


  //Data load--->Action--->Redirect--->Logger-->False
  test("BadRecord_Dataload_006", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s""" CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(2010)), "DataLoadingTestCase-BadRecord_Dataload_006")
     sql(s"""drop table uniqdata""").collect
  }


  //Data load-->Dictionary_Exclude
  test("BadRecord_Dataload_007", Include) {
     sql(s"""CREATE TABLE uniq_exclude (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME,ACTIVE_EMUI_VERSION','DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniq_exclude OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniq_exclude""",
      Seq(Row(2010)), "DataLoadingTestCase-BadRecord_Dataload_007")
     sql(s"""drop table uniq_exclude""").collect
  }


  //Data load-->Extra_Column_in table
  test("BadRecord_Dataload_010", Include) {
     sql(s"""CREATE TABLE exceed_column_in_table (cust_id int ,CUST_NAME String,date timestamp,date2 timestamp) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/extra_column.csv' into table exceed_column_in_table OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='cust_id,CUST_NAME,date,date2')""").collect
    checkAnswer(s"""select count(*) from exceed_column_in_table""",
      Seq(Row(2)), "DataLoadingTestCase-BadRecord_Dataload_010")
     sql(s"""drop table exceed_column_in_table""").collect
  }


  //Data load-->Empty BadRecords Parameters
  test("BadRecord_Dataload_011", Include) {
    intercept[Exception] {
      sql(s"""CREATE TABLE badrecords_test1 (ID int,CUST_ID int,sal int,cust_name string) STORED BY 'org.apache.carbondata.format'""")

        .collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/bad_records1.csv' into table badrecords_test1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='', 'BAD_RECORDS_ACTION'='','FILEHEADER'='ID,CUST_ID,sal,cust_name')""")
        .collect
      checkAnswer(
        s"""select count(*) from badrecords_test1""",
        Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_011")
    }
    sql(s"""drop table badrecords_test1""").collect
  }


  //Data load-->Range Exceed
  test("BadRecord_Dataload_012", Include) {
     sql(s"""CREATE TABLE all_data_types_range (integer_column int,string_column string,double_Column double,decimal_column decimal,bigint_Column bigint) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/all_data_types_range.csv' into table all_data_types_range OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='integer_column,string_column,double_Column,decimal_column,bigint_Column')""").collect
    checkAnswer(s"""select count(*) from all_data_types_range""",
      Seq(Row(2)), "DataLoadingTestCase-BadRecord_Dataload_012")
     sql(s"""drop table all_data_types_range""").collect
  }


  //Data load-->Escape_Character
  test("BadRecord_Dataload_013", Include) {
     sql(s"""CREATE TABLE Escape_test(integer_col int,String_col String,Integer_column2 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/EScape_Test.csv' into table Escape_test OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='integer_col,String_col,Integer_column2')""").collect
    checkAnswer(s"""select count(*) from Escape_test""",
      Seq(Row(3)), "DataLoadingTestCase-BadRecord_Dataload_013")
     sql(s"""drop table Escape_test""").collect
  }


  //Data load-->All_Bad_Records_IN CSV
  test("BadRecord_Dataload_014", Include) {
     sql(s"""CREATE TABLE test25(integer_col int,integer_col2 int,String_col String,decimal_col decimal,double_col double,date timestamp) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_test6.csv' into table test25 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='integer_col,integer_col2,String_col,decimal_col,double_col,date')""").collect
    checkAnswer(s"""select count(*) from test25""",
      Seq(Row(1)), "DataLoadingTestCase-BadRecord_Dataload_014")
     sql(s"""drop table test25""").collect
  }


  //Data load-->CSV_Contain_Single_Space
  test("BadRecord_Dataload_015", Include) {
     sql(s"""CREATE TABLE test3 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect



   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/test3.csv' into table test3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='ID,CUST_ID,Cust_name')""").collect
    checkAnswer(s"""select count(*) from test3""",
      Seq(Row(4)), "DataLoadingTestCase-BadRecord_Dataload_015")
     sql(s"""drop table test3""").collect
  }


  //Data load-->Multiple_Csv
  test("BadRecord_Dataload_016", Include) {
     sql(s"""CREATE TABLE multicsv_check(integer_col int,integer_col2 int,String_col String,decimal_col decimal,double_col double,date timestamp) STORED BY 'org.apache.carbondata.format'""").collect


   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Test' into table multicsv_check OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='integer_col,integer_col2,String_col,decimal_col,double_col,date')""").collect
    checkAnswer(s"""select count(*) from multicsv_check""",
      Seq(Row(2)), "DataLoadingTestCase-BadRecord_Dataload_016")
     sql(s"""drop table multicsv_check""").collect
  }


  //Data load-->Empty csv
  test("BadRecord_Dataload_017", Include) {
    intercept[Exception] {
      sql(s"""CREATE TABLE emptycsv_check(integer_col int,integer_col2 int,String_col String,decimal_col decimal,double_col double,date timestamp) STORED BY 'org.apache.carbondata.format'""").collect

      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/empty.csv' into table emptycsv_check OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='integer_col,integer_col2,String_col,decimal_col,double_col,date')""")
        .collect
    }
    checkAnswer(s"""select count(*) from  emptycsv_check """,
      Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_017")

     sql(s"""drop table  emptycsv_check """).collect
  }


  //Data load-->Datatype contain value of Other Datatype
  test("BadRecord_Dataload_018", Include) {
    sql(s"""CREATE TABLE datatype_check(integer_col int,integer_col2 int,String_col String) STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/datatype.csv' into table datatype_check OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='integer_col,integer_col2,String_col')""").collect
    checkAnswer(
        s"""select count(*) from datatype_check""",
        Seq(Row(1)), "DataLoadingTestCase-BadRecord_Dataload_018")
    sql(s"""drop table datatype_check""").collect
  }


  //Data load-->Extra_Column_incsv
  test("BadRecord_Dataload_019", Include) {
    sql(
      s"""CREATE TABLE exceed_column_in_Csv (CUST_NAME String,date timestamp) STORED BY 'org.apache.carbondata.format'""".stripMargin).collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/extra_column.csv' into table exceed_column_in_Csv OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_NAME,date')""".stripMargin).collect
      checkAnswer(s"""select count(*) from exceed_column_in_Csv """,Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_019")
      sql(s"""drop table exceed_column_in_Csv """).collect
  }


  //Data load-->Timestamp Exceed Range
  test("BadRecord_Dataload_020", Include) {
    sql(
      s"""CREATE TABLE timestamp_range (date timestamp) STORED BY 'org.apache.carbondata.format'""".stripMargin).collect
      sql(
        s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/timetsmap.csv' into table timestamp_range OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='date')""".stripMargin).collect
    checkAnswer(s"""select count(*) from timestamp_range""",Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_020")
    sql(s"""drop table timestamp_range""").collect
  }


  //Show loads-->Delimiter_check
  test("BadRecord_Dataload_021", Include) {
    sql(
      s"""CREATE TABLE bad_records_test5 (String_col string,integer_col int,decimal_column
         |decimal,date timestamp,double_col double) STORED BY 'org.apache.carbondata.format'"""
        .stripMargin)
      .collect
      sql(
        s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_test5.csv' into table
           |bad_records_test5 OPTIONS('DELIMITER'='*' , 'QUOTECHAR'='"',
           |'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='IGNORE',
           |'FILEHEADER'='String_col,integer_col,decimal_column,date,double_col') """.stripMargin)
        .collect
    checkAnswer(
      s"""select count(*) from bad_records_test5""",
      Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_021")
    sql(s"""drop table bad_records_test5 """).collect
  }


  //Data load--->Action--->FAIL--->Logger-->True
  test("BadRecord_Dataload_022", Include) {
    dropTable("bad_records_test5")
     sql(s"""CREATE TABLE bad_records_test5 (String_col string,integer_col int,decimal_column decimal,date timestamp,double_col double) STORED BY 'org.apache.carbondata.format'""").collect
  intercept[Exception] {
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_test5.csv' into table bad_records_test5 OPTIONS('DELIMITER'='*' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='String_col,integer_col,decimal_column,date,double_col') """).collect
  }
    checkAnswer(s"""select count(*) from bad_records_test5""",
      Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_022")
     sql(s"""drop table bad_records_test5 """).collect
  }


  //Data load without any any action parameter
  test("BadRecord_Dataload_023", Include) {
    dropTable("bad_records_test5")
     sql(s"""CREATE TABLE bad_records_test5 (String_col string,integer_col int,decimal_column decimal,date timestamp,double_col double) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_test5.csv' into table bad_records_test5 OPTIONS('DELIMITER'='*' , 'QUOTECHAR'='"','FILEHEADER'='String_col,integer_col,decimal_column,date,double_col') """).collect
    checkAnswer(s"""select count(*) from bad_records_test5""",
      Seq(Row(1)), "DataLoadingTestCase-BadRecord_Dataload_023")
     sql(s"""drop table bad_records_test5 """).collect
  }


  //Check for insert into carbon table with all columns selected from Hive table where both tables having same number of columns
  test("Insert_Func_005", Include) {
     sql(s"""drop table IF EXISTS T_Hive1""").collect
   sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""create table T_Hive1(Active_status String, Item_type_cd INT, Qty_day_avg INT, Qty_total INT, Sell_price BIGINT, Sell_pricep DOUBLE, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date String)row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' overwrite into table T_Hive1""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_005")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Parquet table where both tables having same number of columns
  ignore("Insert_Func_006", Include) {
     sql(s"""drop table IF EXISTS T_Parq1""").collect
   sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Parq1(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE) stored as 'parquet'""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""Insert into T_Parq1 select * from T_hive1""").collect
   sql(s"""insert into T_Carbn01 select * from T_Parq1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_006")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Carbon  table where both tables having same number of columns
  test("Insert_Func_007", Include) {
     sql(s"""drop table IF EXISTS T_Carbn1""").collect
   sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""create table T_Carbn1(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn1 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""insert into T_Carbn01 select * from T_Carbn1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      Seq(Row("TRUE",1,450,304034400,200000343430000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",2,423,3046340,200000000003454300L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",3,453,3003445,200000000000003450L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",4,4350,3044364,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",114,4520,30000430,200000000004300000L,121.5,4.99,2.44,"RE3423ee","asfdsffdfg"),Row("FALSE",123,454,30000040,200000000000000000L,121.5,4.99,2.44,"RE3423ee","asfrewerfg"),Row("TRUE",11,4530,3000040,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffder"),Row("TRUE",14,4590,3000400,200000000000000000L,121.5,4.99,2.44,"ASD423ee","asfertfdfg"),Row("FALSE",41,4250,0,200000000000000000L,121.5,4.99,2.44,"SAD423ee","asrtsffdfg"),Row("TRUE",13,4510,30400,200000000000000000L,121.5,4.99,2.44,"DE3423ee","asfrtffdfg")), "DataLoadingTestCase-Insert_Func_007")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""drop table IF EXISTS T_Carbn1""").collect
  }


  //Check for insert into table providing values in the query
  test("Insert_Func_001", Include) {
     sql(s"""drop table IF EXISTS T_Carbn04""").collect
   sql(s"""create table T_Carbn04(Item_code STRING, Item_name STRING)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn04 values('abc',1)""").collect
    checkAnswer(s"""select * from T_Carbn04""",
      Seq(Row("abc","1")), "DataLoadingTestCase-Insert_Func_001")
     sql(s"""drop table IF EXISTS T_Carbn04""").collect
  }


  //Check for insert into carbon table with all columns selected from Hive  table where selected query is having more columns and the additional columns come after the equivalent columns
  test("Insert_Func_008", Include) {
     sql(s"""drop table IF EXISTS t_hive2""").collect
   sql(s"""create table T_Hive2(Active_status String, Item_type_cd INT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep DOUBLE, Discount_price DOUBLE , Profit DECIMAL(3,2),  Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date String,Profit_perc DECIMAL(4,3),name string)row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive2.csv' overwrite into table T_Hive2""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg SMALLINT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive2""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive2 order by update_time""", "DataLoadingTestCase-Insert_Func_008")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Parquet table where selected query is having more columns
  ignore("Insert_Func_010", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""drop table IF EXISTS T_Parq2""").collect
   sql(s"""create table T_Parq2(Active_status String, Item_type_cd INT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep DOUBLE, Discount_price DOUBLE , Profit DECIMAL(3,2),  Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date String,Profit_perc DECIMAL(4,3),name string) stored as 'parquet'""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""Insert into t_parq2 select * from T_Hive2""").collect
   sql(s"""create table if not exists T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Parq2""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive2 order by update_time""", "DataLoadingTestCase-Insert_Func_010")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Carbon table where selected query is having more columns
  test("Insert_Func_011", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""drop table IF EXISTS t_carbn2""").collect
   sql(s"""create table T_Carbn2(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String,Profit_perc DECIMAL(4,3), name string)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive2.csv' INTO table T_Carbn2 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date,Profit_perc,name')""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Carbn2""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn2 order by update_time""", "DataLoadingTestCase-Insert_Func_011")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on hive when hiveis having TINYINT, SMALLINT data types
  test("Insert_Func_015", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_015")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on Hive table where selected query is having multiple values associated with DATE and TIMESTAMP data type
  test("Insert_Func_016", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into  T_Carbn01 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_016")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Hive table where data transformations done in the selected query on DATE
  test("Insert_Func_018", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into  T_Carbn01 select Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,date_sub(Create_date, 200) from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from (select Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,date_sub(Create_date, 200) from T_Hive1) t1 order by update_time""", "DataLoadingTestCase-Insert_Func_018")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Hive table where multiple tables are joined
  ignore("Insert_Func_019", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""drop table IF EXISTS T_hive4""").collect
   sql(s"""drop table IF EXISTS T_hive5""").collect
   sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Hive4(Item_code STRING, Item_name VARCHAR(50))row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""create table T_Hive5(Item_code STRING, Profit DECIMAL(3,2))row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive4.csv' overwrite into table T_Hive4""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive5.csv' overwrite into table T_Hive5""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_carbn01 select x.Active_status,x.Item_type_cd,x.Qty_day_avg,x.Qty_total,x.Sell_price,x.Sell_pricep,x.Discount_price,z.Profit,x.Item_code,y.Item_name,x.Outlet_name,x.Update_time,x.Create_date from T_Hive1 x,T_Hive4 y, T_Hive5 z where x.Item_code = y.Item_code and x.Item_code = z.Item_code""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from (select x.Active_status,x.Item_type_cd,x.Qty_day_avg,x.Qty_total,x.Sell_price,x.Sell_pricep,x.Discount_price,z.Profit,x.Item_code,y.Item_name,x.Outlet_name,x.Update_time,x.Create_date from T_Hive1 x,T_Hive4 y, T_Hive5 z where x.Item_code = y.Item_code and x.Item_code = z.Item_code) t1 order by update_time""", "DataLoadingTestCase-Insert_Func_019")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with all columns selected from Hive table where table is having the columns in different name
  test("Insert_Func_020", Include) {
     sql(s"""drop table IF EXISTS t_hive7""").collect
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Hive7(Active_status1 BOOLEAN, Item_type_cd1 TINYINT, Qty_day_avg1 SMALLINT, Qty_total1 INT, Sell_price1 BIGINT, Sell_pricep1 FLOAT, Discount_price1 DOUBLE , Profit1 DECIMAL(3,2), Item_code1 STRING, Item_name1 VARCHAR(50), Outlet_name1 CHAR(100), Update_time TIMESTAMP, Create_date DATE)row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' overwrite into table T_Hive7""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv'  into table T_Hive7""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive7""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      Seq(Row("true",1,450,304034400,200000343430000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",1,450,304034400,200000343430000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",2,423,3046340,200000000003454300L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",2,423,3046340,200000000003454300L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",3,453,3003445,200000000000003450L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",3,453,3003445,200000000000003450L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",4,4350,3044364,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",4,4350,3044364,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("true",114,4520,30000430,200000000004300000L,121.5,4.99,2.44,"RE3423ee","asfdsffdfg"),Row("true",114,4520,30000430,200000000004300000L,121.5,4.99,2.44,"RE3423ee","asfdsffdfg"),Row("false",123,454,30000040,200000000000000000L,121.5,4.99,2.44,"RE3423ee","asfrewerfg"),Row("false",123,454,30000040,200000000000000000L,121.5,4.99,2.44,"RE3423ee","asfrewerfg"),Row("true",11,4530,3000040,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffder"),Row("true",11,4530,3000040,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffder"),Row("true",14,4590,3000400,200000000000000000L,121.5,4.99,2.44,"ASD423ee","asfertfdfg"),Row("true",14,4590,3000400,200000000000000000L,121.5,4.99,2.44,"ASD423ee","asfertfdfg"),Row("false",41,4250,0,200000000000000000L,121.5,4.99,2.44,"SAD423ee","asrtsffdfg"),Row("false",41,4250,0,200000000000000000L,121.5,4.99,2.44,"SAD423ee","asrtsffdfg"),Row("true",13,4510,30400,200000000000000000L,121.5,4.99,2.44,"DE3423ee","asfrtffdfg"),Row("true",13,4510,30400,200000000000000000L,121.5,4.99,2.44,"DE3423ee","asfrtffdfg")), "DataLoadingTestCase-Insert_Func_020")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all column from a Hive table where table has no records
  test("Insert_Func_021", Include) {
     sql(s"""drop table IF EXISTS T_Hive8""").collect
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Hive8(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE)row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive8""").collect
    checkAnswer(s"""select count(*) from T_Carbn01""",
      Seq(Row(0)), "DataLoadingTestCase-Insert_Func_021")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all column from a Carbon table where table has no records
  test("Insert_Func_023", Include) {
     sql(s"""drop table IF EXISTS T_Carbn02""").collect
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Carbn02""").collect
    checkAnswer(s"""select count(*) from T_Carbn01""",
      Seq(Row(0)), "DataLoadingTestCase-Insert_Func_023")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all column from a Hive table where already records already present in the Carbon table from Load
  test("Insert_Func_027", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""create table T_Hive_1(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String) row format delimited fields terminated by ',' collection items terminated by '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Hive_1""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive_1""").collect
    checkAnswer(s"""select count(*) from T_Carbn01""",
      Seq(Row(20)), "DataLoadingTestCase-Insert_Func_027")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all column from the same carbon table
  test("Insert_Func_028", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""insert into T_Carbn01 select * from T_Carbn01""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      Seq(Row("TRUE",1,450,304034400,200000343430000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",1,450,304034400,200000343430000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",2,423,3046340,200000000003454300L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",2,423,3046340,200000000003454300L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",3,453,3003445,200000000000003450L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",3,453,3003445,200000000000003450L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",4,4350,3044364,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",4,4350,3044364,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffdfg"),Row("TRUE",114,4520,30000430,200000000004300000L,121.5,4.99,2.44,"RE3423ee","asfdsffdfg"),Row("TRUE",114,4520,30000430,200000000004300000L,121.5,4.99,2.44,"RE3423ee","asfdsffdfg"),Row("FALSE",123,454,30000040,200000000000000000L,121.5,4.99,2.44,"RE3423ee","asfrewerfg"),Row("FALSE",123,454,30000040,200000000000000000L,121.5,4.99,2.44,"RE3423ee","asfrewerfg"),Row("TRUE",11,4530,3000040,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffder"),Row("TRUE",11,4530,3000040,200000000000000000L,121.5,4.99,2.44,"SE3423ee","asfdsffder"),Row("TRUE",14,4590,3000400,200000000000000000L,121.5,4.99,2.44,"ASD423ee","asfertfdfg"),Row("TRUE",14,4590,3000400,200000000000000000L,121.5,4.99,2.44,"ASD423ee","asfertfdfg"),Row("FALSE",41,4250,0,200000000000000000L,121.5,4.99,2.44,"SAD423ee","asrtsffdfg"),Row("FALSE",41,4250,0,200000000000000000L,121.5,4.99,2.44,"SAD423ee","asrtsffdfg"),Row("TRUE",13,4510,30400,200000000000000000L,121.5,4.99,2.44,"DE3423ee","asfrtffdfg"),Row("TRUE",13,4510,30400,200000000000000000L,121.5,4.99,2.44,"DE3423ee","asfrtffdfg")), "DataLoadingTestCase-Insert_Func_028")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all column from  a hive table limiting the records selected
  test("Insert_Func_038", Include) {
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
     sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive1 limit 10""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from (select  * from T_Hive1 limit 10) order by update_time""", "DataLoadingTestCase-Insert_Func_038")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select statement having subquery and join
  test("Insert_Func_039", Include) {
     sql(s"""drop table IF EXISTS t_hive5""").collect
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Hive5(Item_code STRING, Profit DECIMAL(3,2))row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive5.csv' overwrite into table T_Hive5""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive1 x where exists (select * from T_Hive5 y where x.Item_code= y.Item_code) """).collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from (select * from T_Hive1 x where exists (select * from T_Hive5 y where x.Item_code= y.Item_code)) t1 order by update_time""", "DataLoadingTestCase-Insert_Func_039")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select statement having filter
  test("Insert_Func_044", Include) {
     sql(s"""drop table if exists t_hive4""").collect
    sql(s"""drop table IF EXISTS T_Carbn01""").collect
   sql(s"""create table T_Hive4(Item_code STRING, Item_name VARCHAR(50))row format delimited fields terminated by ','""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive4.csv' overwrite into table T_Hive4""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_Hive1 a where a.Item_code in (select b.item_code from T_Hive4 b)""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from (select * from T_Hive1 a where a.Item_code in (select b.item_code from T_Hive4 b)) t1 order by update_time""", "DataLoadingTestCase-Insert_Func_044")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check for insert into carbon table with select on all columns from Hive table where Carbon table is created with block size of 1 mb
  test("Insert_Func_045", Include) {
     sql(s"""drop table IF EXISTS T_Carbn011""").collect
   sql(s"""create table T_Carbn011(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""insert into T_Carbn011 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn011 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_045")
     sql(s"""drop table IF EXISTS T_Carbn011""").collect
  }


  //Check for insert into carbon table with select on all columns from Hive table where Carbon table is created with block size of 100 mb
  test("Insert_Func_046", Include) {
     sql(s"""drop table IF EXISTS t_carbn011""").collect
   sql(s"""create table T_Carbn011(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='100')""").collect
   sql(s"""insert into T_Carbn011 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn011 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_046")
     sql(s"""drop table IF EXISTS T_Carbn011""").collect
  }


  //Check for insert into carbon table with select on all columns from Hive table where Carbon table is created with block size of 500 mb
  test("Insert_Func_047", Include) {
     sql(s"""drop table IF EXISTS t_carbn011""").collect
   sql(s"""create table T_Carbn011(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1024')""").collect
   sql(s"""insert into T_Carbn011 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn011 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_047")
     sql(s"""drop table IF EXISTS T_Carbn011""").collect
  }


  //Check for insert into carbon table with select on all columns from Hive table where Carbon table is created with block size of 2gb mb
  test("Insert_Func_048", Include) {
     sql(s"""drop table IF EXISTS t_carbn011""").collect
   sql(s"""create table T_Carbn011(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2048')""").collect
   sql(s"""insert into T_Carbn011 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn011 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_048")
     sql(s"""drop table IF EXISTS T_Carbn011""").collect
  }


  //Check for insert into carbon table with select on Hive and applying cast on the selected columns to suite the target table data type before inserting
  test("Insert_Func_050", Include) {
     sql(s"""drop table IF EXISTS t_carbn04""").collect
   sql(s"""create table T_Carbn04(Item_code STRING, Item_name STRING)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn04 select Item_code, cast(Profit as STRING) from T_Hive5""").collect
    checkAnswer(s"""select * from T_Carbn04""",
      Seq(Row("BE3423ee","4.99"),Row("BE3423ee","4.99"),Row("BE3423ee","4.99"),Row("BE3423ee","4.99"),Row("RE3423ee","4.99"),Row("RE3423ee","4.99"),Row("SE3423ee","4.99"),Row("SE3423ee","4.99"),Row("SE3423ee","4.99"),Row("SE3423ee","4.99"),Row("ASD423ee","4.99"),Row("DE3423ee","4.99"),Row("DE3423ee","4.99"),Row("FE3423ee","4.99"),Row("FE3423ee","4.99"),Row("FE3423ee","4.99"),Row("RE3423ee","4.99"),Row("RE3423ee","4.99"),Row("SAD423ee","4.99"),Row("SE3423ee","4.99")), "DataLoadingTestCase-Insert_Func_050")
     sql(s"""drop table IF EXISTS T_Carbn04""").collect
  }


  //Check for insert into carbon table with select on Hive table and inserted carbon table created with one dimension excluded from dictionary.
  test("Insert_Func_060", Include) {
     sql(s"""drop table IF EXISTS t_carbn020""").collect
   sql(s"""create table T_Carbn020(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='Item_code')""").collect
   sql(s"""Insert into T_Carbn020 select * from T_Hive1""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn020 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_060")
     sql(s"""drop table IF EXISTS T_Carbn020""").collect
  }


  //Check for insert into carbon table with select on a Carbon table and inserted carbon table created with one dimension excluded from dictionary.
  test("Insert_Func_061", Include) {
     sql(s"""drop table IF EXISTS t_carbn020""").collect
    dropTable("T_Carbn01")
   sql(s"""create table T_Carbn020(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='Item_code')""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""Insert into T_Carbn020 select * from T_Carbn01""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn020 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""", "DataLoadingTestCase-Insert_Func_061")
     sql(s"""drop table IF EXISTS T_Carbn020""").collect
  }


  //Check that Segment deletion for the inserted data in to Carbon table clears all the data
  test("Insert_Func_074", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""Insert into T_Carbn01 select * from T_Hive1""").collect
   sql(s"""delete from table T_Carbn01 where segment.id in (0)""").collect
   sql(s"""select count(*)  from T_Carbn01""").collect
    checkAnswer(s"""select count(*)  from T_Carbn01""",
      Seq(Row(0)), "DataLoadingTestCase-Insert_Func_074")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check that when load and insert is made, deletion of segments associated with load should not delete inserted records
  test("Insert_Func_075", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""Insert into T_Carbn01 select * from T_Hive1""").collect
   sql(s"""delete from table T_Carbn01 where segment.id in (0)""").collect
   sql(s"""select count(*)  from T_Carbn01""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",
      s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_075")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check that when load and insert is made, deletion of segments associated with insert operation should not delete loaded records
  test("Insert_Func_076", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""drop table if exists T_Hive1""").collect
   sql(s"""create table T_Hive1(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String) row format delimited fields terminated by ',' collection items terminated by '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Hive1""").collect
   sql(s"""Insert into T_Carbn01 select * from T_Hive1""").collect
   sql(s"""delete from table T_Carbn01 where segment.id in (0)""").collect
   sql(s"""select count(*)  from T_Carbn01""").collect
    checkAnswer(s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Carbn01 order by update_time""",s"""select active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price, profit,item_code,item_name from T_Hive1 order by update_time""", "DataLoadingTestCase-Insert_Func_076")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check insert into Carbon table with select from Hive , repeat this query multiple times in the same terminal
  test("Insert_Func_082", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""Insert into T_Carbn01 select * from T_Hive1""").collect
   sql(s"""Insert into T_Carbn01 select * from T_Hive1""").collect
    checkAnswer(s"""select count(*) from T_Carbn01""",
      Seq(Row(20)), "DataLoadingTestCase-Insert_Func_082")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check insert into Carbon table with select from Hive , and Load table done sequentially
  test("Insert_Func_083", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""insert into T_Carbn01 select * from T_hive1""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from T_Carbn01""",
      Seq(Row(20)), "DataLoadingTestCase-Insert_Func_083")
     sql(s"""drop table IF EXISTS T_Carbn01""").collect
  }


  //Check insert into Carbon table with select done on a Hive partitioned table
  test("Insert_Func_109", Include) {
     sql(s"""drop table IF EXISTS t_hive14""").collect
   sql(s"""create table T_Hive14(Item_code STRING,  Profit DECIMAL(3,2)) partitioned by (Qty_total INT, Item_type_cd TINYINT) row format delimited fields terminated by ',' collection items terminated by '$DOLLAR'""").collect
   sql(s"""drop table IF EXISTS T_Carbn014""").collect
   sql(s"""create table T_Carbn014(Item_code STRING, Profit DECIMAL(3,2), Qty_total INT, Item_type_cd INT) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""load data INPATH '$resourcesPath/Data/InsertData/T_Hive14.csv' overwrite into table T_Hive14 partition(Qty_total=100, Item_type_cd=2)""").collect
   sql(s"""insert into T_carbn014 select * from T_Hive14 where Qty_total =100""").collect
    checkAnswer(s"""select item_code, profit from T_Carbn014 order by item_code, profit""",
      Seq(Row("BE3423ee",4.99),Row("BE3423ee",4.99),Row("SE3423ee",4.99),Row("SE3423ee",4.99),Row("SE3423ee",4.99),Row("SE3423ee",4.99)), "DataLoadingTestCase-Insert_Func_109")
     sql(s"""drop table IF EXISTS T_Carbn014""").collect
  }


  //Check for select column from 2 tables joined using alias names for columns of both tables.
  test("Insert_Func_110", Include) {
     sql(s"""create table employees(name string, empid string, mgrid string, mobileno bigint) stored by 'carbondata'""").collect
   sql(s"""create table managers(name string, empid string, mgrid string, mobileno bigint) stored by 'carbondata'""").collect
   sql(s"""insert into managers select 'harry','h2399','v788232',99823230205""").collect
   sql(s"""insert into employees select 'tom','t23717','h2399',99780207526""").collect
    checkAnswer(s"""select e.empid from employees e join managers m on e.mgrid=m.empid""",
      Seq(Row("t23717")), "DataLoadingTestCase-Insert_Func_110")
     sql(s"""drop table employees""").collect
   sql(s"""drop table managers""").collect
  }


  //Show loads--->Action=Fail--->Logger=True
  test("BadRecord_Dataload_024", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    }
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_024")
     sql(s"""drop table uniqdata""").collect
  }

  //Show loads--->Action=Fail--->Logger=False
  test("BadRecord_Dataload_025", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FAIL','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      checkAnswer(
        s"""select count(*) from uniqdata""",
        Seq(Row(0)), "DataLoadingTestCase-BadRecord_Dataload_025")
    }
     sql(s"""drop table uniqdata""").collect
  }

  //when insert into null data,query table output NullPointerException
  test("HQ_DEFECT_2016111509706", Include) {
     sql(s"""drop table IF EXISTS t_carbn01""").collect
   sql(s"""drop table IF EXISTS t_carbn02""").collect
   sql(s"""create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""create table T_Carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1_Bad.csv' INTO table T_Carbn01 options ('BAD_RECORDS_ACTION'='FORCE','DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""insert into t_carbn02 select Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date from t_carbn01""").collect
    checkAnswer(s"""select count(*) from t_carbn02""",
      Seq(Row(10)), "DataLoadingTestCase_HQ_DEFECT_2016111509706")
     sql(s"""drop table t_carbn01""").collect
   sql(s"""drop table t_carbn02""").collect
  }


  //Check insert into T_Carbn01 with select from T_Carbn02 from diff database
  test("Insert_Func_112", Include) {
     sql(s"""drop database if exists Insert1 cascade""").collect
   sql(s"""create database Insert1""").collect
   sql(s"""create table Insert1.Carbon_Insert_Func_1 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Insert1.Carbon_Insert_Func_1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""drop database if exists Insert2""").collect
   sql(s"""create database Insert2""").collect
   sql(s"""create table Insert2.Carbon_Insert_Func_2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""insert into Insert2.Carbon_Insert_Func_2 select * from Insert1.Carbon_Insert_Func_1""").collect
    checkAnswer(s"""select count(*) from Insert2.Carbon_Insert_Func_2""",
      Seq(Row(99)), "DataLoadingTestCase-Insert_Func_112")
     sql(s"""drop database Insert1 cascade""").collect
   sql(s"""drop database Insert2 cascade""").collect
  }


  //Check for Data insert into select for table with blocksize configured.
  test("TableBlockSize-05-09-01", Include) {
     sql(s"""CREATE TABLE BlockSize_Dataload_1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table BlockSize_Dataload_1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""CREATE TABLE BlockSize_Dataload_2 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2')""").collect
   sql(s"""insert into BlockSize_Dataload_2 select * from BlockSize_Dataload_1""").collect
    checkAnswer(s"""select count(*) from BlockSize_Dataload_2""",
      Seq(Row(16)), "DataLoadingTestCase_TableBlockSize-05-09-01")
     sql(s"""drop table BlockSize_Dataload_1""").collect
   sql(s"""drop table BlockSize_Dataload_2""").collect
  }


  //Check for insert into carbon table with select from Hive table where only Measures columns are present.
  test("Insert_Func_066", Include) {
     sql(s"""create table Measures_Dataload_H (Item_code STRING, Qty int)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Measures_Dataload_H""").collect
   sql(s"""create table Measures_Dataload_C (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
   sql(s"""insert into Measures_Dataload_C select * from Measures_Dataload_H""").collect
    checkAnswer(s"""select count(*) from Measures_Dataload_C""",
      Seq(Row(99)), "DataLoadingTestCase-Insert_Func_066")
     sql(s"""drop table Measures_Dataload_H""").collect
   sql(s"""drop table Measures_Dataload_C""").collect
  }


  //Check insert into carbon table with select when mulitple tables are joined through union.
  ignore("Insert_Func_097", Include) {
     sql(s"""CREATE TABLE Table_Union_1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table Table_Union_1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME, ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""CREATE TABLE Table_Union_2 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table Table_Union_2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME, ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""CREATE TABLE Table_Union_3 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""insert into Table_Union_3 select * from Table_Union_1 union select * from Table_Union_2""").collect
    checkAnswer(s"""select count(*) from Table_Union_3""",
      Seq(Row(16)), "DataLoadingTestCase-Insert_Func_097")
     sql(s"""drop table Table_Union_1""").collect
   sql(s"""drop table Table_Union_2""").collect
   sql(s"""drop table Table_Union_3""").collect
  }


  //Check for insert into carbon table with select statement having logical operators
  test("Insert_Func_043", Include) {
     sql(s"""create table Logical_Dataload_H (Item_code STRING, Qty int)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Logical_Dataload_H""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Logical_Dataload_H""").collect
   sql(s"""create table Logical_Dataload_C (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
   sql(s"""insert into Logical_Dataload_C select * from Logical_Dataload_H where Item_Code != 'D' and Qty < 40""").collect
    checkAnswer(s"""select count(*) from Logical_Dataload_C""",
      Seq(Row(6)), "DataLoadingTestCase-Insert_Func_043")
     sql(s"""drop table Logical_Dataload_H""").collect
   sql(s"""drop table Logical_Dataload_C""").collect
  }


  //Check that select query fetches the correct data after doing insert and load .
  test("Insert_Func_073", Include) {
     sql(s"""create table Dataload_H (Item_code STRING, Qty int)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Dataload_H""").collect
   sql(s"""create table Dataload_C (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect


   sql(s"""insert into Dataload_C select * from Dataload_H""").collect
    checkAnswer(s"""select count(*) from Dataload_C""",
      Seq(Row(6)), "DataLoadingTestCase-Insert_Func_073")
     sql(s"""drop table Dataload_H""").collect
   sql(s"""drop table Dataload_C""").collect
  }


  //Check insert into T_Carbn01 with select from T_Hive1 from diff database
  test("Insert_Func_111", Include) {
     sql(s"""create database insert1""").collect
   sql(s"""create table insert1.DiffDB_Dataload_H(Item_code STRING, Qty int)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE insert1.DiffDB_Dataload_H""").collect
   sql(s"""create database insert2""").collect
   sql(s"""create table insert2.DiffDB_Dataload_C(Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
   sql(s"""insert into insert2.DiffDB_Dataload_C select * from insert1.DiffDB_Dataload_H""").collect
    checkAnswer(s"""select count(*) from insert2.DiffDB_Dataload_C""",
      Seq(Row(6)), "DataLoadingTestCase-Insert_Func_111")
     sql(s"""drop database insert1 cascade""").collect
   sql(s"""drop database insert2 cascade""").collect
  }


  //Check for insert into carbon table with select from Hive table where only Dimension columns are present.
  ignore("Insert_Func_065", Include) {
     sql(s"""create table Dimension_Dataload_H (Item_code STRING)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Dimension_Dataload_H""").collect
   sql(s"""create table Dimension_Dataload_C (Item_code STRING)stored by 'org.apache.carbondata.format'""").collect
   sql(s"""insert into Dimension_Dataload_C select * from Dimension_Dataload_H""").collect
    checkAnswer(s"""select count(*) from Dimension_Dataload_C""",
      Seq(Row(6)), "DataLoadingTestCase-Insert_Func_065")
     sql(s"""drop table Dimension_Dataload_H""").collect
   sql(s"""drop table Dimension_Dataload_C""").collect
  }


  //Check data load after retension.
  ignore("LCM_002_001-001-TC-006_827", Include) {
     sql(s"""create table DL_RETENCTION (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_RETENCTION options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    val dateFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    val date = dateFormat.format(new Date(System.currentTimeMillis()))
    println(date)
   sql(s"""delete from table DL_RETENCTION where segment.STARTTIME BEFORE '${date}'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_RETENCTION options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_RETENCTION""",
      Seq(Row(10)), "DataLoadingTestCase_LCM_002_001-001-TC-006_827")
     sql(s"""drop table DL_RETENCTION""").collect
  }


  //Check for the incremental load data DML without "DELIMITER" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-09_840", Include) {
     sql(s"""create table DL_WithOutDELIMITER(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Update_time')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_WithOutDELIMITER options ('QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_WithOutDELIMITER options ('QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_WithOutDELIMITER""",
      Seq(Row(20)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-09_840")
     sql(s"""drop table DL_WithOutDELIMITER""").collect
  }


  //Check for correct result set displayed for query execution after historical data loading.
  test("History_Data_Load_001_001-002-TC-01_749", Include) {
     sql(s"""CREATE TABLE  DL_HistoricalData( CUST_ID String,CUST_COUNTRY String,CUST_STATE String,CUST_CITY String,CUST_JOB_TITLE String,CUST_BUY_POTENTIAL String,PROD_UNQ_MDL_ID String,PROD_BRAND_NAME String,PRODUCT_NAME String,PRODUCT_MODEL String,PROD_MODEL_ID String,PROD_COLOR String,ITM_ID String,ITM_NAME String,PRMTION_ID String,PRMTION_NAME String,SHP_MODE_ID String,SHP_MODE String,DELIVERY_COUNTRY String,DELIVERY_STATE String,DELIVERY_CITY String,DELIVERY_DISTRICT String,TRACKING_NO String,ACTIVE_EMUI_VERSION String,WH_NAME String,STR_ORDER_NO String,STR_ORDER_DATE String,OL_ORDER_NO String,OL_ORDER_DATE String,OL_SITE String,CUST_NICK_NAME String,CUST_FIRST_NAME String,CUST_LAST_NAME String,CUST_PRFRD_FLG String,CUST_BIRTH_DY String,CUST_BIRTH_MM String,CUST_BIRTH_YR String,CUST_BIRTH_COUNTRY String,CUST_LOGIN String,CUST_EMAIL_ADDR String,CUST_LAST_RVW_DATE String,CUST_SEX String,CUST_ADDRESS_ID String,CUST_STREET_NO String,CUST_STREET_NAME String,CUST_AGE String,CUST_SUITE_NO String,CUST_ZIP String,CUST_COUNTY String,PRODUCT_ID String,PROD_UNQ_DEVICE_ADDR String,PROD_UQ_UUID String,PROD_SHELL_COLOR String,DEVICE_NAME String,PROD_SHORT_DESC String,PROD_LONG_DESC String,PROD_THUMB String,PROD_IMAGE String,PROD_UPDATE_DATE String,PROD_BAR_CODE String,PROD_LIVE String,PROD_LOC String,PROD_RAM String,PROD_ROM String,PROD_CPU_CLOCK String,PROD_SERIES String,ITM_REC_START_DATE String,ITM_REC_END_DATE String,ITM_BRAND_ID String,ITM_BRAND String,ITM_CLASS_ID String,ITM_CLASS String,ITM_CATEGORY_ID String,ITM_CATEGORY String,ITM_MANUFACT_ID String,ITM_MANUFACT String,ITM_FORMULATION String,ITM_COLOR String,ITM_CONTAINER String,ITM_MANAGER_ID String,PRM_START_DATE String,PRM_END_DATE String,PRM_CHANNEL_DMAIL String,PRM_CHANNEL_EMAIL String,PRM_CHANNEL_CAT String,PRM_CHANNEL_TV String,PRM_CHANNEL_RADIO String,PRM_CHANNEL_PRESS String,PRM_CHANNEL_EVENT String,PRM_CHANNEL_DEMO String,PRM_CHANNEL_DETAILS String,PRM_PURPOSE String,PRM_DSCNT_ACTIVE String,SHP_CODE String,SHP_CARRIER String,SHP_CONTRACT String,CHECK_DATE String,CHECK_YR String,CHECK_MM String,CHECK_DY String,CHECK_HOUR String,BOM String,INSIDE_NAME String,PACKING_DATE String,PACKING_YR String,PACKING_MM String,PACKING_DY String,PACKING_HOUR String,DELIVERY_PROVINCE String,PACKING_LIST_NO String,ACTIVE_CHECK_TIME String,ACTIVE_CHECK_YR String,ACTIVE_CHECK_MM String,ACTIVE_CHECK_DY String,ACTIVE_CHECK_HOUR String,ACTIVE_AREA_ID String,ACTIVE_COUNTRY String,ACTIVE_PROVINCE String,ACTIVE_CITY String,ACTIVE_DISTRICT String,ACTIVE_NETWORK String,ACTIVE_FIRMWARE_VER String,ACTIVE_OS_VERSION String,LATEST_CHECK_TIME String,LATEST_CHECK_YR String,LATEST_CHECK_MM String,LATEST_CHECK_DY String,LATEST_CHECK_HOUR String,LATEST_AREAID String,LATEST_COUNTRY String,LATEST_PROVINCE String,LATEST_CITY String,LATEST_DISTRICT String,LATEST_FIRMWARE_VER String,LATEST_EMUI_VERSION String,LATEST_OS_VERSION String,LATEST_NETWORK String,WH_ID String,WH_STREET_NO String,WH_STREET_NAME String,WH_STREET_TYPE String,WH_SUITE_NO String,WH_CITY String,WH_COUNTY String,WH_STATE String,WH_ZIP String,WH_COUNTRY String,OL_SITE_DESC String,OL_RET_ORDER_NO String,OL_RET_DATE String,CUST_DEP_COUNT double,CUST_VEHICLE_COUNT double,CUST_ADDRESS_CNT double,CUST_CRNT_CDEMO_CNT double,CUST_CRNT_HDEMO_CNT double,CUST_CRNT_ADDR_DM double,CUST_FIRST_SHIPTO_CNT double,CUST_FIRST_SALES_CNT double,CUST_GMT_OFFSET double,CUST_DEMO_CNT double,CUST_INCOME double,PROD_UNLIMITED double,PROD_OFF_PRICE double,PROD_UNITS double,TOTAL_PRD_COST double,TOTAL_PRD_DISC double,PROD_WEIGHT double,REG_UNIT_PRICE double,EXTENDED_AMT double,UNIT_PRICE_DSCNT_PCT double,DSCNT_AMT double,PROD_STD_CST double,TOTAL_TX_AMT double,FREIGHT_CHRG double,WAITING_PERIOD double,DELIVERY_PERIOD double,ITM_CRNT_PRICE double,ITM_UNITS double,ITM_WSLE_CST double,ITM_SIZE double,PRM_CST double,PRM_RESPONSE_TARGET double,PRM_ITM_DM double,SHP_MODE_CNT double,WH_GMT_OFFSET double,WH_SQ_FT double,STR_ORD_QTY double,STR_WSLE_CST double,STR_LIST_PRICE double,STR_SALES_PRICE double,STR_EXT_DSCNT_AMT double,STR_EXT_SALES_PRICE double,STR_EXT_WSLE_CST double,STR_EXT_LIST_PRICE double,STR_EXT_TX double,STR_COUPON_AMT double,STR_NET_PAID double,STR_NET_PAID_INC_TX double,STR_NET_PRFT double,STR_SOLD_YR_CNT double,STR_SOLD_MM_CNT double,STR_SOLD_ITM_CNT double,STR_TOTAL_CUST_CNT double,STR_AREA_CNT double,STR_DEMO_CNT double,STR_OFFER_CNT double,STR_PRM_CNT double,STR_TICKET_CNT double,STR_NET_PRFT_DM_A double,STR_NET_PRFT_DM_B double,STR_NET_PRFT_DM_C double,STR_NET_PRFT_DM_D double,STR_NET_PRFT_DM_E double,STR_RET_STR_ID double,STR_RET_REASON_CNT double,STR_RET_TICKET_NO double,STR_RTRN_QTY double,STR_RTRN_AMT double,STR_RTRN_TX double,STR_RTRN_AMT_INC_TX double,STR_RET_FEE double,STR_RTRN_SHIP_CST double,STR_RFNDD_CSH double,STR_REVERSED_CHRG double,STR_STR_CREDIT double,STR_RET_NET_LOSS double,STR_RTRNED_YR_CNT double,STR_RTRN_MM_CNT double,STR_RET_ITM_CNT double,STR_RET_CUST_CNT double,STR_RET_AREA_CNT double,STR_RET_OFFER_CNT double,STR_RET_PRM_CNT double,STR_RET_NET_LOSS_DM_A double,STR_RET_NET_LOSS_DM_B double,STR_RET_NET_LOSS_DM_C double,STR_RET_NET_LOSS_DM_D double,OL_ORD_QTY double,OL_WSLE_CST double,OL_LIST_PRICE double,OL_SALES_PRICE double,OL_EXT_DSCNT_AMT double,OL_EXT_SALES_PRICE double,OL_EXT_WSLE_CST double,OL_EXT_LIST_PRICE double,OL_EXT_TX double,OL_COUPON_AMT double,OL_EXT_SHIP_CST double,OL_NET_PAID double,OL_NET_PAID_INC_TX double,OL_NET_PAID_INC_SHIP double,OL_NET_PAID_INC_SHIP_TX double,OL_NET_PRFT double,OL_SOLD_YR_CNT double,OL_SOLD_MM_CNT double,OL_SHIP_DATE_CNT double,OL_ITM_CNT double,OL_BILL_CUST_CNT double,OL_BILL_AREA_CNT double,OL_BILL_DEMO_CNT double,OL_BILL_OFFER_CNT double,OL_SHIP_CUST_CNT double,OL_SHIP_AREA_CNT double,OL_SHIP_DEMO_CNT double,OL_SHIP_OFFER_CNT double,OL_WEB_PAGE_CNT double,OL_WEB_SITE_CNT double,OL_SHIP_MODE_CNT double,OL_WH_CNT double,OL_PRM_CNT double,OL_NET_PRFT_DM_A double,OL_NET_PRFT_DM_B double,OL_NET_PRFT_DM_C double,OL_NET_PRFT_DM_D double,OL_RET_RTRN_QTY double,OL_RTRN_AMT double,OL_RTRN_TX double,OL_RTRN_AMT_INC_TX double,OL_RET_FEE double,OL_RTRN_SHIP_CST double,OL_RFNDD_CSH double,OL_REVERSED_CHRG double,OL_ACCOUNT_CREDIT double,OL_RTRNED_YR_CNT double,OL_RTRNED_MM_CNT double,OL_RTRITM_CNT double,OL_RFNDD_CUST_CNT double,OL_RFNDD_AREA_CNT double,OL_RFNDD_DEMO_CNT double,OL_RFNDD_OFFER_CNT double,OL_RTRNING_CUST_CNT double,OL_RTRNING_AREA_CNT double,OL_RTRNING_DEMO_CNT double,OL_RTRNING_OFFER_CNT double,OL_RTRWEB_PAGE_CNT double,OL_REASON_CNT double,OL_NET_LOSS double,OL_NET_LOSS_DM_A double,OL_NET_LOSS_DM_B double,OL_NET_LOSS_DM_C double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_EXCLUDE'='CUST_STATE','DICTIONARY_INCLUDE'='LATEST_OS_VERSION')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/OSCON.csv' INTO TABLE DL_HistoricalData OPTIONS('FILEHEADER'='CUST_ID,CUST_COUNTRY,CUST_STATE,CUST_CITY,ACTIVE_AREA_ID,ACTIVE_COUNTRY,ACTIVE_PROVINCE,ACTIVE_CITY,ACTIVE_DISTRICT,LATEST_AREAID,LATEST_COUNTRY,LATEST_PROVINCE,LATEST_CITY,LATEST_DISTRICT,WH_COUNTRY,WH_STATE,WH_CITY,WH_COUNTY,CUST_JOB_TITLE,CUST_BUY_POTENTIAL,PROD_UNQ_MDL_ID,PROD_BRAND_NAME,PRODUCT_NAME,PRODUCT_MODEL,PROD_MODEL_ID,PROD_STD_CST,REG_UNIT_PRICE,TOTAL_PRD_COST,TOTAL_PRD_DISC,PROD_OFF_PRICE,TOTAL_TX_AMT,PROD_UNITS,PROD_WEIGHT,UNIT_PRICE_DSCNT_PCT,DSCNT_AMT,OL_SALES_PRICE,PROD_COLOR,ITM_ID,ITM_NAME,PRMTION_ID,PRMTION_NAME,SHP_MODE_ID,SHP_MODE,DELIVERY_COUNTRY,DELIVERY_STATE,DELIVERY_CITY,DELIVERY_DISTRICT,TRACKING_NO,ACTIVE_EMUI_VERSION,WH_NAME,STR_ORDER_NO,STR_ORDER_DATE,OL_ORDER_NO,OL_ORDER_DATE,OL_SITE,CUST_NICK_NAME,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_PRFRD_FLG,CUST_BIRTH_DY,CUST_BIRTH_MM,CUST_BIRTH_YR,CUST_BIRTH_COUNTRY,CUST_LOGIN,CUST_EMAIL_ADDR,CUST_LAST_RVW_DATE,CUST_SEX,CUST_ADDRESS_ID,CUST_STREET_NO,CUST_STREET_NAME,CUST_AGE,CUST_SUITE_NO,CUST_ZIP,CUST_COUNTY,PRODUCT_ID,PROD_UNQ_DEVICE_ADDR,PROD_UQ_UUID,PROD_SHELL_COLOR,DEVICE_NAME,PROD_SHORT_DESC,PROD_LONG_DESC,PROD_THUMB,PROD_IMAGE,PROD_UPDATE_DATE,PROD_BAR_CODE,PROD_LIVE,PROD_LOC,PROD_RAM,PROD_ROM,PROD_CPU_CLOCK,PROD_SERIES,ITM_REC_START_DATE,ITM_REC_END_DATE,ITM_BRAND_ID,ITM_BRAND,ITM_CLASS_ID,ITM_CLASS,ITM_CATEGORY_ID,ITM_CATEGORY,ITM_MANUFACT_ID,ITM_MANUFACT,ITM_FORMULATION,ITM_COLOR,ITM_CONTAINER,ITM_MANAGER_ID,PRM_START_DATE,PRM_END_DATE,PRM_CHANNEL_DMAIL,PRM_CHANNEL_EMAIL,PRM_CHANNEL_CAT,PRM_CHANNEL_TV,PRM_CHANNEL_RADIO,PRM_CHANNEL_PRESS,PRM_CHANNEL_EVENT,PRM_CHANNEL_DEMO,PRM_CHANNEL_DETAILS,PRM_PURPOSE,PRM_DSCNT_ACTIVE,SHP_CODE,SHP_CARRIER,SHP_CONTRACT,CHECK_DATE,CHECK_YR,CHECK_MM,CHECK_DY,CHECK_HOUR,BOM,INSIDE_NAME,PACKING_DATE,PACKING_YR,PACKING_MM,PACKING_DY,PACKING_HOUR,DELIVERY_PROVINCE,PACKING_LIST_NO,ACTIVE_CHECK_TIME,ACTIVE_CHECK_YR,ACTIVE_CHECK_MM,ACTIVE_CHECK_DY,ACTIVE_CHECK_HOUR,ACTIVE_NETWORK,ACTIVE_FIRMWARE_VER,ACTIVE_OS_VERSION,LATEST_CHECK_TIME,LATEST_CHECK_YR,LATEST_CHECK_MM,LATEST_CHECK_DY,LATEST_CHECK_HOUR,LATEST_FIRMWARE_VER,LATEST_EMUI_VERSION,LATEST_OS_VERSION,LATEST_NETWORK,WH_ID,WH_STREET_NO,WH_STREET_NAME,WH_STREET_TYPE,WH_SUITE_NO,WH_ZIP,OL_SITE_DESC,OL_RET_ORDER_NO,OL_RET_DATE,CUST_DEP_COUNT,CUST_VEHICLE_COUNT,CUST_ADDRESS_CNT,CUST_CRNT_CDEMO_CNT,CUST_CRNT_HDEMO_CNT,CUST_CRNT_ADDR_DM,CUST_FIRST_SHIPTO_CNT,CUST_FIRST_SALES_CNT,CUST_GMT_OFFSET,CUST_DEMO_CNT,CUST_INCOME,PROD_UNLIMITED,EXTENDED_AMT,FREIGHT_CHRG,WAITING_PERIOD,DELIVERY_PERIOD,ITM_CRNT_PRICE,ITM_UNITS,ITM_WSLE_CST,ITM_SIZE,PRM_CST,PRM_RESPONSE_TARGET,PRM_ITM_DM,SHP_MODE_CNT,WH_GMT_OFFSET,WH_SQ_FT,STR_ORD_QTY,STR_WSLE_CST,STR_LIST_PRICE,STR_SALES_PRICE,STR_EXT_DSCNT_AMT,STR_EXT_SALES_PRICE,STR_EXT_WSLE_CST,STR_EXT_LIST_PRICE,STR_EXT_TX,STR_COUPON_AMT,STR_NET_PAID,STR_NET_PAID_INC_TX,STR_NET_PRFT,STR_SOLD_YR_CNT,STR_SOLD_MM_CNT,STR_SOLD_ITM_CNT,STR_TOTAL_CUST_CNT,STR_AREA_CNT,STR_DEMO_CNT,STR_OFFER_CNT,STR_PRM_CNT,STR_TICKET_CNT,STR_NET_PRFT_DM_A,STR_NET_PRFT_DM_B,STR_NET_PRFT_DM_C,STR_NET_PRFT_DM_D,STR_NET_PRFT_DM_E,STR_RET_STR_ID,STR_RET_REASON_CNT,STR_RET_TICKET_NO,STR_RTRN_QTY,STR_RTRN_AMT,STR_RTRN_TX,STR_RTRN_AMT_INC_TX,STR_RET_FEE,STR_RTRN_SHIP_CST,STR_RFNDD_CSH,STR_REVERSED_CHRG,STR_STR_CREDIT,STR_RET_NET_LOSS,STR_RTRNED_YR_CNT,STR_RTRN_MM_CNT,STR_RET_ITM_CNT,STR_RET_CUST_CNT,STR_RET_AREA_CNT,STR_RET_OFFER_CNT,STR_RET_PRM_CNT,STR_RET_NET_LOSS_DM_A,STR_RET_NET_LOSS_DM_B,STR_RET_NET_LOSS_DM_C,STR_RET_NET_LOSS_DM_D,OL_ORD_QTY,OL_WSLE_CST,OL_LIST_PRICE,OL_EXT_DSCNT_AMT,OL_EXT_SALES_PRICE,OL_EXT_WSLE_CST,OL_EXT_LIST_PRICE,OL_EXT_TX,OL_COUPON_AMT,OL_EXT_SHIP_CST,OL_NET_PAID,OL_NET_PAID_INC_TX,OL_NET_PAID_INC_SHIP,OL_NET_PAID_INC_SHIP_TX,OL_NET_PRFT,OL_SOLD_YR_CNT,OL_SOLD_MM_CNT,OL_SHIP_DATE_CNT,OL_ITM_CNT,OL_BILL_CUST_CNT,OL_BILL_AREA_CNT,OL_BILL_DEMO_CNT,OL_BILL_OFFER_CNT,OL_SHIP_CUST_CNT,OL_SHIP_AREA_CNT,OL_SHIP_DEMO_CNT,OL_SHIP_OFFER_CNT,OL_WEB_PAGE_CNT,OL_WEB_SITE_CNT,OL_SHIP_MODE_CNT,OL_WH_CNT,OL_PRM_CNT,OL_NET_PRFT_DM_A,OL_NET_PRFT_DM_B,OL_NET_PRFT_DM_C,OL_NET_PRFT_DM_D,OL_RET_RTRN_QTY,OL_RTRN_AMT,OL_RTRN_TX,OL_RTRN_AMT_INC_TX,OL_RET_FEE,OL_RTRN_SHIP_CST,OL_RFNDD_CSH,OL_REVERSED_CHRG,OL_ACCOUNT_CREDIT,OL_RTRNED_YR_CNT,OL_RTRNED_MM_CNT,OL_RTRITM_CNT,OL_RFNDD_CUST_CNT,OL_RFNDD_AREA_CNT,OL_RFNDD_DEMO_CNT,OL_RFNDD_OFFER_CNT,OL_RTRNING_CUST_CNT,OL_RTRNING_AREA_CNT,OL_RTRNING_DEMO_CNT,OL_RTRNING_OFFER_CNT,OL_RTRWEB_PAGE_CNT,OL_REASON_CNT,OL_NET_LOSS,OL_NET_LOSS_DM_A,OL_NET_LOSS_DM_B,OL_NET_LOSS_DM_C')""").collect
    checkAnswer(s"""select count(*) from DL_HistoricalData""",
      Seq(Row(10000)), "DataLoadingTestCase_History_Data_Load_001_001-002-TC-01_749")
     sql(s"""drop table DL_HistoricalData""").collect
  }


  //Verify data laoding with Special Character
  test("Details-Loading-FileFormat-003_TC_01_126", Include) {
     sql(s"""CREATE TABLE DL_SPL_CHAR_Load(imei string,specialchar string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA inpath '$resourcesPath/Data/splchar.csv' INTO table DL_SPL_CHAR_Load options ('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'= 'imei,specialchar')""").collect
    checkAnswer(s"""select count(*) from DL_SPL_CHAR_Load""",
      Seq(Row(5)), "DataLoadingTestCase_Details-Loading-FileFormat-003_TC_01_126")
     sql(s"""drop table DL_SPL_CHAR_Load""").collect
  }


  //Verify data loading in default HDFS path.
  test("History_Data_Load_001_001-008-TC-01_749", Include) {
     sql(s"""create table DL_HDFSLoads(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_HDFSLoads options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_HDFSLoads""",
      Seq(Row(10)), "DataLoadingTestCase_History_Data_Load_001_001-008-TC-01_749")
     sql(s"""drop table DL_HDFSLoads""").collect
  }


  //Check for the incremental load data DML without "PARTITIONDATA" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-08_840", Include) {
     sql(s"""create table DL_Without_Partitiondata(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Without_Partitiondata options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_Without_Partitiondata""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-08_840")
     sql(s"""drop table DL_Without_Partitiondata""").collect
  }


  //Check for the incremental load data DML without "ESCAPERCHAR" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-14_840", Include) {
     sql(s"""create table DL_Without_ESCAPERCHAR(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Without_ESCAPERCHAR options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_Without_ESCAPERCHAR""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-14_840")
     sql(s"""drop table DL_Without_ESCAPERCHAR""").collect
  }


  //Check for the incremental load data DML with invalid ESCAPERCHAR specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-19_840", Include) {
     sql(s"""create table DL_Wrong_ESCAPERCHAR(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Wrong_ESCAPERCHAR options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date','ESCAPECHAR'='\\n')""").collect
    checkAnswer(s"""select count(*) from DL_Wrong_ESCAPERCHAR""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-19_840")
     sql(s"""drop table DL_Wrong_ESCAPERCHAR""").collect
  }


  //Check for the incremental load data DML without "MULTILINE" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-15_840", Include) {
     sql(s"""create table DL_Without_MULTILINE(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Without_MULTILINE options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Without_MULTILINE options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_Without_MULTILINE""",
      Seq(Row(20)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-15_840")
     sql(s"""drop table DL_Without_MULTILINE""").collect
  }


  //Check for the initial load data DML with invalid MULTILINE specified loading the data successfully.
  test("History_Data_Load_001_001-001-TC-20_749", Include) {
     sql(s"""create table DL_Invalid_MULTILINE(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Invalid_MULTILINE options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date','MULTILINE'='asdf')""").collect
    checkAnswer(s"""select count(*) from DL_Invalid_MULTILINE""",
      Seq(Row(10)), "DataLoadingTestCase_History_Data_Load_001_001-001-TC-20_749")
     sql(s"""drop table DL_Invalid_MULTILINE""").collect
  }


  //Check for the incremental load data DML with invalid MULTILINE specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-20_840", Include) {
     sql(s"""create table DL_With_MULTILINE(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_With_MULTILINE options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date','MULTILINE'='true')""").collect
    checkAnswer(s"""select count(*) from DL_With_MULTILINE""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-20_840")
     sql(s"""drop table DL_With_MULTILINE""").collect
  }


  //Check for the incremental load data DML loading the data successfully with correct syntax
  test("Incremental_Data_Load_001_001-001-TC-01_840", Include) {
     sql(s""" CREATE TABLE DL_IncrementalLoad (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_IncrementalLoad options ('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_IncrementalLoad options ('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*)from DL_IncrementalLoad""",
      Seq(Row(32)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-01_840")
     sql(s"""drop table DL_IncrementalLoad""").collect
  }


  //Check data loaded in same table with different block size after drop and recreation
  test("TableBlockSize-07-07-01", Include) {
     sql(s""" CREATE TABLE DL_DiffTBLPROPERTIES (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_DiffTBLPROPERTIES OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""drop table DL_DiffTBLPROPERTIES""").collect
   sql(s"""CREATE TABLE DL_DiffTBLPROPERTIES (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_DiffTBLPROPERTIES OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s""" select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from DL_DiffTBLPROPERTIES""").collect
    checkAnswer(s"""select count(*)from DL_DiffTBLPROPERTIES""",
      Seq(Row(16)), "DataLoadingTestCase_TableBlockSize-07-07-01")
     sql(s"""drop table DL_DiffTBLPROPERTIES""").collect
  }


  //Verify data loading when coloumn in table is not in order as CSV coloumn.
  test("History_Data_Load_001_001-043-TC-01_749", Include) {
     sql(s"""CREATE TABLE DL_NoOrderColumn (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOJ timestamp,DOB timestamp,BIGINT_COLUMN2 bigint,BIGINT_COLUMN1 bigint, DECIMAL_COLUMN2 decimal(36,10),DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_NoOrderColumn OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN2,BIGINT_COLUMN1,DECIMAL_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*)from DL_NoOrderColumn""",
      Seq(Row(16)), "DataLoadingTestCase_History_Data_Load_001_001-043-TC-01_749")
     sql(s"""drop table DL_NoOrderColumn""").collect
  }


  //Check data load with Column dictionary for int column
  test("Details-Loading-StreamLoad-002-002", Include) {
     sql(s"""CREATE TABLE DL_NoIntInclude (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2')""").collect

   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv'  into table DL_NoIntInclude OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN2,BIGINT_COLUMN1,DECIMAL_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*)from DL_NoIntInclude""",
      Seq(Row(16)), "DataLoadingTestCase_Details-Loading-StreamLoad-002-002")
     sql(s"""drop table DL_NoIntInclude""").collect
  }


  //Check for correct result set displayed for query execution after incremental data loading.
  test("Incremental_Data_Load_001_001-002-TC-01_840", Include) {
     sql(s"""CREATE TABLE DL_Result (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_Result OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN2,BIGINT_COLUMN1,DECIMAL_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_Result OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN2,BIGINT_COLUMN1,DECIMAL_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from DL_Result""").collect
    checkAnswer(s"""select count(*)from DL_Result""",
      Seq(Row(32)), "DataLoadingTestCase_Incremental_Data_Load_001_001-002-TC-01_840")
     sql(s"""drop table DL_Result""").collect
  }


  //Check for Decimal Datatype with maximum boundary values.
  test("Decimal_002_001-001-TC-031", Include) {
     sql(s"""CREATE TABLE DL_MaxDecimal (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(38,38), Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_MaxDecimal OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from DL_MaxDecimal""",
      Seq(Row(16)), "DataLoadingTestCase_Decimal_002_001-001-TC-031")
     sql(s"""drop table DL_MaxDecimal""").collect
  }


  //Check for Decimal Datatype with minimum boundary values.
  test("Decimal_002_001-001-TC-030", Include) {
     sql(s"""CREATE TABLE DL_MinDecimal (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(1,0), Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_MinDecimal OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from DL_MinDecimal""",
      Seq(Row(16)), "DataLoadingTestCase_Decimal_002_001-001-TC-030")
     sql(s"""drop table DL_MinDecimal""").collect
  }


  //Check for Decimal Datatype with dataload functionality
  test("Decimal_002_001-001-TC-028", Include) {
     sql(s"""CREATE TABLE DL_Decimal (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_Decimal OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1, Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from DL_Decimal""",
      Seq(Row(16)), "DataLoadingTestCase_Decimal_002_001-001-TC-028")
     sql(s"""drop table DL_Decimal""").collect
  }


  //Test empty values in Data loading
  test("Details-Query-ColStore-001-TC-016_126", Include) {
     sql(s"""create table DL_EmptyDataLoad(empid String,empname String,city String,country String,gender String,salary double) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/emptyLoad.csv' INTO table DL_EmptyDataLoad OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='empid, empname, city, country, gender, salary', 'BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from DL_EmptyDataLoad""",
      Seq(Row(6)), "DataLoadingTestCase_Details-Query-ColStore-001-TC-016_126")
     sql(s"""drop table DL_EmptyDataLoad""").collect
  }


  //Check for the incremental load data DML without "local" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-02_840", Include) {
     sql(s"""create table DL_Without_LOCAL(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table DL_Without_LOCAL options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_Without_LOCAL""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-02_840")
     sql(s"""drop table DL_Without_LOCAL""").collect
  }


  //Check for the incremental load data DML with different inputs of "DELIMITER" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-10_840", Include) {
     sql(s"""create table DL_Different_DELIMITER(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/DiffDilimiter.csv' INTO table DL_Different_DELIMITER options ('DELIMITER'='&', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from DL_Different_DELIMITER""",
      Seq(Row(10)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-10_840")
     sql(s"""drop table DL_Different_DELIMITER""").collect
  }


  //Load data fails for table name having data type
  test("History_Data_Load_001_001-001-TC-83", Include) {
     sql(s"""create table bigint(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table bigint options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from  bigint""",
      Seq(Row(10)), "DataLoadingTestCase_History_Data_Load_001_001-001-TC-83")
     sql(s"""drop table bigint""").collect
  }


  //Verify escape character behaviour while dataloading
  test("Details-Loading-FileFormat-003_TC_02_126", Include) {
     sql(s"""CREATE TABLE DL_EscapeChar_Behave_1(imei string,specialchar string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""CREATE TABLE DL_EscapeChar_Behave_2(imei string,specialchar string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA inpath '$resourcesPath/Data/splchar.csv' INTO table DL_EscapeChar_Behave_1 options ('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'= 'imei,specialchar', 'ESCAPECHAR'='\')""").collect
   sql(s"""LOAD DATA inpath '$resourcesPath/Data/splchar.csv' INTO table DL_EscapeChar_Behave_2 options ('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'= 'imei,specialchar', 'ESCAPECHAR'='@')""").collect
    checkAnswer(s"""select count(*) from DL_EscapeChar_Behave_2""",
      Seq(Row(5)), "DataLoadingTestCase_Details-Loading-FileFormat-003_TC_02_126")
     sql(s"""drop table DL_EscapeChar_Behave_1""").collect
   sql(s"""drop table DL_EscapeChar_Behave_2""").collect
  }


  //Test Double data type max value support by Carbon
  test("FN_Carbon_DecimalDataType_TC_01_126", Include) {
     sql(s"""CREATE TABLE DL_MaxDoubleValue (val double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='val')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/DoubleVal.csv' INTO TABLE DL_MaxDoubleValue OPTIONS('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'='val')""").collect
    checkAnswer(s"""select count(*) from DL_MaxDoubleValue""",
      Seq(Row(4)), "DataLoadingTestCase_FN_Carbon_DecimalDataType_TC_01_126")
     sql(s"""drop table DL_MaxDoubleValue""").collect
  }


  //Check whether the bad records are logged when user load the data through CSV having invalid data at bottom/Middle/Top of CSV
  test("Bad_Records_Logger_Implementation-001-TC-002", Include) {
     sql(s"""CREATE TABLE DL_WithBadRecords(sVal1 string, iVal int, sVal2 string, tsVal timestamp) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords.csv' INTO TABLE DL_WithBadRecords OPTIONS('DELIMITER'=',','QUOTECHAR'='\','FILEHEADER'='sVal1, iVal, sVal2, tsVal')""").collect
    checkAnswer(s"""select count(*) from DL_WithBadRecords""",
      Seq(Row(5)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-002")
     sql(s"""drop table DL_WithBadRecords""").collect
  }


  //Check for the incremental load data DML with long "table name" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-07_840", Include) {
     sql(s"""CREATE TABLE DL_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table DL_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1, Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from DL_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789""",
      Seq(Row(16)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-07_840")
     sql(s"""drop table DL_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789""").collect
  }


  //Failed to delete merged segment
  test("Details-Loading-Incremental-001-01_TC_016_851", Include) {
     sql(s"""create table DL_T_Merge (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Merge OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Merge OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Merge OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Merge OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Merge OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table DL_T_Merge compact 'minor'""").collect
   sql(s"""delete from table DL_T_Merge where segment.id in (0.1)""").collect
    checkAnswer(s"""select count(*) from DL_T_Merge""",
      Seq(Row(99)), "DataLoadingTestCase_Details-Loading-Incremental-001-01_TC_016_851")
     sql(s"""drop table DL_T_Merge""").collect
  }


  //DDL for merge
  test("Details-Loading-Incremental-001-01_TC_037_870", Include) {
     sql(s"""create table DL_T_Alter (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Alter OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Alter OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Alter OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Alter OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE DL_T_Alter OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table DL_T_Alter compact 'minor'""").collect
    checkAnswer(s"""select count(*) from DL_T_Alter""",
      Seq(Row(495)), "DataLoadingTestCase_Details-Loading-Incremental-001-01_TC_037_870")
     sql(s"""drop table DL_T_Alter""").collect
  }


  //Check for the initial load data DML without "MULTILINE" specified loading the data successfully.
  test("History_Data_Load_001_001-001-TC-15_749", Include) {
     sql(s"""create table BR_Without_MULTILINE(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table BR_Without_MULTILINE options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(*) from BR_Without_MULTILINE""",
      Seq(Row(10)), "DataLoadingTestCase_History_Data_Load_001_001-001-TC-15_749")
     sql(s"""drop table BR_Without_MULTILINE""").collect
  }


  //Validate the count and data with respect to String when data has bad records
  test("Bad_Records_Logger_Implementation-001-TC-038", Include) {
     sql(s"""create table BR_StringValidations(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_StringValidations OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from  BR_StringValidations""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-038")
     sql(s"""drop table BR_StringValidations""").collect
  }


  //Check whether the bad records are logging when user configure BAD_RECORDS_LOGGER_ENABLE as True
  test("Bad_Records_Logger_Implementation-001-TC-007", Include) {
     sql(s"""create table BR_Logger_TRUE(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_Logger_TRUE OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_TRUE""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-007")
     sql(s"""drop table BR_Logger_TRUE""").collect
  }


  //Check when csv contains all bad records and BAD_RECORDS_LOGGER_ACTION'='REDIRECT'
  test("Bad_Records_Logger_Implementation-001-TC-067", Include) {
     sql(s"""create table BR_Logger_REDIRECT(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_Logger_REDIRECT OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_ACTION'='REDIRECT')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_REDIRECT""",
      Seq(Row(2)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-067")
     sql(s"""drop table BR_Logger_REDIRECT""").collect
  }


  //Check whether bad records csv can be loaded with force.
  test("Bad_Records_Logger_Implementation-001-TC-029", Include) {
     sql(s"""create table BR_Logger_Force(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv'  INTO TABLE BR_Logger_Force OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_Force""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-029")
     sql(s"""drop table BR_Logger_Force""").collect
  }


  //Check when csv contains all bad records and BAD_RECORDS_LOGGER_ACTION'=IGNORE'
  test("Bad_Records_Logger_Implementation-001-TC-068", Include) {
     sql(s"""create table BR_Logger_IGNORE(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_Logger_IGNORE OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_IGNORE""",
      Seq(Row(2)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-068")
     sql(s"""drop table BR_Logger_IGNORE""").collect
  }


  //Check whether the bad records are logged when user load the data through carbon table having invalid data at bottom/Middle/Top of CSV
  test("Bad_Records_Logger_Implementation-001-TC-004", Include) {
     sql(s""" create table BR_Logger_Invalid_Data(val1 string,val2 string,val3 string,val4 string,val5 int,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_3.csv' INTO TABLE BR_Logger_Invalid_Data OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_Invalid_Data""",
      Seq(Row(2)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-004")
     sql(s"""drop table BR_Logger_Invalid_Data""").collect
  }


  //Check for Decimal Datatype with bad records functionality.
  test("Decimal_002_001-001-TC-038", Include) {
     sql(s"""create table BR_Logger_Invalid_Decimal(val1 string,val2 decimal(3,2),val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_4.csv' INTO TABLE BR_Logger_Invalid_Decimal OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_Invalid_Decimal""",
      Seq(Row(5)), "DataLoadingTestCase_Decimal_002_001-001-TC-038")
     sql(s"""drop table BR_Logger_Invalid_Decimal""").collect
  }


  //Check with multiple csv, different folders, check which csv file has bad records issue when BAD_RECORDS_LOGGER_ACTION as REDIRECT  and  BAD_RECORDS_LOGGER_ENABLE  as False
  test("Bad_Records_Logger_Implementation-001-TC-032", Include) {
     sql(s"""create table BR_Logger_MultiCSV(val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_Logger_MultiCSV OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='REDIRECT')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_3.csv' INTO TABLE BR_Logger_MultiCSV OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='REDIRECT')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_MultiCSV""",
      Seq(Row(5)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-032")
     sql(s"""drop table BR_Logger_MultiCSV""").collect
  }


  //Check whether the bad records are logging when user BAD_RECORDS_LOGGER_ACTION as FORCE and BAD_RECORDS_LOGGER_ENABLE as False and loading the data through csv
  test("Bad_Records_Logger_Implementation-001-TC-012", Include) {
    dropTable("BR_Logger_FORCE")
     sql(s"""create table BR_Logger_FORCE(val1 string,val2 string,val3 string,val4 string,val5 int,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_2.csv' INTO TABLE BR_Logger_FORCE OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_FORCE""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-012")
     sql(s"""drop table BR_Logger_FORCE""").collect
  }


  //Check whether the bad records are logging when user configure BAD_RECORDS_LOGGER_ENABLE as False
  test("Bad_Records_Logger_Implementation-001-TC-006", Include) {
     sql(s"""create table BR_Logger_FALSE (val1 string,val2 string,val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_2.csv' INTO TABLE BR_Logger_FALSE OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_FALSE""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-006")
     sql(s"""drop table BR_Logger_FALSE""").collect
  }


  //Check whether the bad records are logged when user load the data through CSV having invalid data in different column/rows
  test("Bad_Records_Logger_Implementation-001-TC-001", Include) {
    sql(s"""drop table if exists BR_Logger_Invalid_Data""").collect
     sql(s"""create table BR_Logger_Invalid_Data (val1 string,val2 decimal(3,2),val3 string,val4 string,val5 string,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_4.csv' INTO TABLE BR_Logger_Invalid_Data OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_Invalid_Data""",
      Seq(Row(5)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-001")
     sql(s"""drop table BR_Logger_Invalid_Data""").collect
  }


  //Check whether the bad records are logged when user load the data through carbon table having invalid data in different column/rows
  test("Bad_Records_Logger_Implementation-001-TC-003", Include) {
     sql(s"""create table BR_Logger_Invalid_Data_2 (val1 string,val2 string,val3 string,val4 string,val5 int,dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/badrecords_3.csv' INTO TABLE BR_Logger_Invalid_Data_2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3, val4, val5, dt','BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='FORCE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_Invalid_Data_2""",
      Seq(Row(5)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-003")
     sql(s"""drop table BR_Logger_Invalid_Data_2""").collect
  }


  //Validate the count and data with respect to integer,decimal and double when data has bad records
  test("Bad_Records_Logger_Implementation-001-TC-037", Include) {
     sql(s"""create table BR_Logger_COUNT_DATA(val1 string,val2 int,val3 decimal(3,2),dt timestamp) stored by 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecords_5.csv' INTO TABLE BR_Logger_COUNT_DATA OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\','FILEHEADER'='val1, val2, val3,  dt', 'BAD_RECORDS_LOGGER_ENABLE'='TRUE','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from BR_Logger_COUNT_DATA""",
      Seq(Row(3)), "DataLoadingTestCase_Bad_Records_Logger_Implementation-001-TC-037")
     sql(s"""drop table BR_Logger_COUNT_DATA""").collect
  }


  //Data validation after compaction and cleanup
  test("Details-Loading-Incremental-001-01_TC_037_869", Include) {
     sql(s"""create table Compaction_ShowSegment (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_ShowSegment OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_ShowSegment OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_ShowSegment OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_ShowSegment OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_ShowSegment OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table Compaction_ShowSegment compact 'minor'""").collect
   sql(s"""CLEAN FILES FOR TABLE Compaction_ShowSegment""").collect
   sql(s"""show segments for table Compaction_ShowSegment""").collect
    checkAnswer(s"""select count(*) from Compaction_ShowSegment""",
      Seq(Row(495)), "DataLoadingTestCase_Details-Loading-Incremental-001-01_TC_037_869")
     sql(s"""drop table Compaction_ShowSegment""").collect
  }


  //Check segment status after compaction
  test("Details-Loading-Incremental-001-01_TC_037_874", Include) {
     sql(s"""create table Compaction_T_minor (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_minor OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_minor OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_minor OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_minor OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_minor OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table Compaction_T_minor compact 'minor'""").collect
   sql(s"""show segments for table Compaction_T_minor""").collect
    checkAnswer(s"""select count(*) from Compaction_T_minor""",
      Seq(Row(495)), "DataLoadingTestCase_Details-Loading-Incremental-001-01_TC_037_874")
     sql(s"""drop table Compaction_T_minor""").collect
  }


  //Include compaction folder
  test("Details-Loading-Incremental-001-01_TC_037_871", Include) {
     sql(s"""create table Compaction_T_C (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_C OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_C OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_C OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_C OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_C OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table Compaction_T_C compact 'minor'""").collect
    checkAnswer(s"""select count(*) from Compaction_T_C""",
      Seq(Row(495)), "DataLoadingTestCase_Details-Loading-Incremental-001-01_TC_037_871")
     sql(s"""show segments for table Compaction_T_C""").collect
   sql(s"""drop table Compaction_T_C""").collect
  }


  //Check the segments after compaction
  test("LCM_002_001-001-TC-013", Include) {
    dropTable("Compaction_T_Delete")
     sql(s"""create table Compaction_T_Delete (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_Delete OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_Delete OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_Delete OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_Delete OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""alter table Compaction_T_Delete compact 'minor'""").collect
   sql(s"""CLEAN FILES FOR TABLE Compaction_T_Delete""").collect
   sql(s"""show segments for table Compaction_T_Delete""").collect
   sql(s"""delete from table Compaction_T_Delete where segment.id in (0.1)""").collect
    checkAnswer(s"""select count(*) from Compaction_T_Delete""",
      Seq(Row(0)), "DataLoadingTestCase_LCM_002_001-001-TC-013")
     sql(s"""drop table Compaction_T_Delete""").collect
  }


  //Check for the incremental load data DML without "QUOTECHAR" specified loading the data successfully.
  test("Incremental_Data_Load_001_001-001-TC-11_840", Include) {
     sql(s"""CREATE TABLE DL_without_QUOTECHAR (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='DOB,DOJ')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/2000_UniqData.csv' into table DL_without_QUOTECHAR OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='REDIRECT','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from DL_without_QUOTECHAR""",
      Seq(Row(2010)), "DataLoadingTestCase_Incremental_Data_Load_001_001-001-TC-11_840")
     sql(s"""drop table DL_without_QUOTECHAR""").collect
  }


  //Check for insert into carbon table with select on all column from a Carbon table where table has no records due to segmnet deletion
  test("Insert_Func_023_01", Include) {
    dropTable("Norecords_Dataload_C")
    dropTable("Norecords_Dataload_H")
    intercept[Exception] {
      sql(s"""create table Norecords_Dataload_H (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Norecords_Dataload_H OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='Item_code,Qty')""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/Measures.csv' INTO TABLE Norecords_Dataload_H OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='Item_code,Qty')""").collect
      sql(s"""create table Norecords_Dataload_C (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
      sql(s"""delete from table Norecords_Dataload_H where segment.id in (0,1)""").collect
      sql(s"""insert into Norecords_Dataload_C select * from Norecords_Dataload_H""").collect
      checkAnswer(s"""select count(*) from Norecords_Dataload_C""",
        Seq(Row(0)), "DataLoadingTestCase-Insert_Func_023_01")
    }
     dropTable("Norecords_Dataload_C")
     dropTable("Norecords_Dataload_H")
  }


  //Check that minor compaction done after 2 minor compaction and ensure data consistency is not impacted.
  ignore("Insert_Func_071", Include) {
     sql(s"""DROP database IF EXISTS Compact_1 cascade""").collect
   sql(s"""create database Compact_1""").collect
   sql(s"""use Compact_1""").collect
   sql(s"""create table Compaction_T_1 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compaction_T_1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
   sql(s"""create table Compaction_T_2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""insert into Compact_1.Compaction_T_2 select * from Compact_1.Compaction_T_1""").collect
   sql(s"""alter table Compact_1.Compaction_T_1 compact 'minor'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE Compact_1.Compaction_T_1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    checkAnswer(s"""select count(*) from Compact_1.Compaction_T_1""",
      Seq(Row(297)), "DataLoadingTestCase-Insert_Func_071")
     sql(s"""show segments for table Compact_1.Compaction_T_1""").collect
   sql(s"""show segments for table Compact_1.Compaction_T_2""").collect
   sql(s"""drop database if exists Compact_1 cascade""").collect
  }


  //Check insert into Carbon table with select from Hive where one of the column is having Null values
  ignore("Insert_Func_080", Include) {
     sql(s"""drop database if exists insertInto CASCADE""").collect
   sql(s"""create database insertInto""").collect
   sql(s"""create table insertInto.Norecords_Dataload_H (Item_code STRING, Qty int)row format delimited fields terminated by ',' LINES TERMINATED BY '\n'""").collect 
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE insertInto.Norecords_Dataload_H""").collect
   sql(s"""use insertInto""").collect
   sql(s"""drop table if exists Norecords_Dataload_Carbon""").collect
   sql(s"""create table Norecords_Dataload_Carbon (Item_code STRING, Qty int)stored by 'org.apache.carbondata.format'""").collect
   sql(s"""insert into insertInto.Norecords_Dataload_Carbon select * from insertInto.Norecords_Dataload_H""").collect
    checkAnswer(s"""select count(*) from insertInto.Norecords_Dataload_Carbon""",
      Seq(Row(99)), "DataLoadingTestCase-Insert_Func_080")
     sql(s"""drop database if exists insertInto cascade""").collect
  }

  override protected def beforeAll(): Unit = {
    sql(s"""drop table if exists uniqdata""").collect
     }
}