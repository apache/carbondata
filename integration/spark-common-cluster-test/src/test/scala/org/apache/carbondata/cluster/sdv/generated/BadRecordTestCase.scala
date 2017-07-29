
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

/**
 * Test Class for BadRecordTestCase to verify all scenerios
 */

class BadRecordTestCase extends QueryTest with BeforeAndAfterAll {
         
  
  //Create table and Load history data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true from CSV without header and specify headers in command
  test("AR-Develop-Feature-BadRecords-001_PTS001_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest1 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test2.csv' into table badrecordtest1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='ID,CUST_ID,cust_name')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test2.csv' into table badrecordtest1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='ID,CUST_ID,cust_name')""").collect
    checkAnswer(s"""select count(*) from badrecordTest1""",
      Seq(Row(6)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS001_TC001")
     sql(s"""drop table if exists badrecordTest1""").collect
  }


  //Create table and Load history data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true from CSV with  header and specify header in command
  test("AR-Develop-Feature-BadRecords-001_PTS002_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest2 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordtest2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='ID,CUST_ID,cust_name')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordtest2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='ID,CUST_ID,cust_name')""").collect
    checkAnswer(s"""select count(*) from badrecordtest2""",
      Seq(Row(6)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS002_TC001")
     sql(s"""drop table if exists badrecordtest2""").collect
  }


  //Create table and Load history data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true from CSV with  header and without specify header in command
  test("AR-Develop-Feature-BadRecords-001_PTS003_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest3 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordtest3 OPTIONS('FILEHEADER'='ID,CUST_ID,cust_name','DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordtest3 OPTIONS('FILEHEADER'='ID,CUST_ID,cust_name','DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordtest3""",
      Seq(Row(6)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS003_TC001")
     sql(s"""drop table if exists badrecordtest3""").collect
  }


  //Create table and load the data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true with CSV has incomplete/wrong data
  test("AR-Develop-Feature-BadRecords-001_PTS004_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest4 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test4.csv' into table badrecordtest4 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test4.csv' into table badrecordtest4 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordtest4""",
      Seq(Row(6)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS004_TC001")
     sql(s"""drop table if exists badrecordtest4""").collect
  }


  //Create table and load data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true for data types with boundary values of data type
  test("AR-Develop-Feature-BadRecords-001_PTS005_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest5 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test5.csv' into table badrecordtest5 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test5.csv' into table badrecordtest5 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordtest5""",
      Seq(Row(4)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS005_TC001")
     sql(s"""drop table if exists badrecordtest5""").collect
  }


  //create table and Load history data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true  from CSV with' Delimiters , Quote characters '
  test("AR-Develop-Feature-BadRecords-001_PTS006_TC001", Include) {
    sql(s"""drop table if exists abadrecordtest1""").collect
    sql(s"""CREATE TABLE abadrecordtest1 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test6.csv' into table abadrecordtest1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'="'",'is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
      checkAnswer(
        s"""select count(*) from abadrecordtest1""",
        Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS006_TC001")
    sql(s"""drop table if exists abadrecordtest1""").collect
  }


  //create the table and load the data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true column value with separator (/ , \ ,!,\001)
  test("AR-Develop-Feature-BadRecords-001_PTS007_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest6 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/qoute1.csv' into table badrecordtest6 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='/','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE')""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/qoute3.csv' into table badrecordtest6 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE')""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/qoute4.csv' into table badrecordtest6 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='!','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE')""").collect
      checkAnswer(
        s"""select count(*) from badrecordtest6""",
        Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS007_TC001")
    }
     sql(s"""drop table if exists badrecordtest6""").collect
  }


  //Create the table and Load from Hive table
  test("AR-Develop-Feature-BadRecords-001_PTS008_TC001", Include) {
    sql(s"""drop table if exists badrecordTest7""").collect
    sql(s"""drop table if exists hivetable7""").collect
     sql(s"""CREATE TABLE badrecordtest7 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""CREATE TABLE hivetable7 (ID int,CUST_ID int,cust_name string) row format delimited fields terminated by ','""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test2.csv' into table hivetable7""").collect
   sql(s"""insert into table badrecordtest7 select * from hivetable7""").collect
    checkAnswer(s"""select count(*) from badrecordtest7""",
      Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS008_TC001")
     sql(s"""drop table if exists badrecordTest7""").collect
   sql(s"""drop table if exists hivetable7""").collect
  }


  //Create table and Insert into Select for destination carbon table from source carbon/hive/parquet table
  test("AR-Develop-Feature-BadRecords-001_PTS015_TC001", Include) {
    sql(s"""drop table if exists badrecordTest9""").collect
    sql(s"""drop table if exists hivetable9""").collect
     sql(s"""CREATE TABLE badrecordTest9 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""CREATE TABLE hivetable9 (ID int,CUST_ID int,cust_name string) row format delimited fields terminated by ','""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test2.csv' into table hivetable9""").collect
   sql(s"""insert into table badrecordTest9 select * from hivetable9""").collect
    checkAnswer(s"""select count(*) from badrecordTest9""",
      Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS015_TC001")
     sql(s"""drop table if exists badrecordTest9""").collect
   sql(s"""drop table if exists hivetable9""").collect
  }


  //Show segments for table when data loading having parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true
  test("AR-Develop-Feature-BadRecords-001_PTS020_TC001", Include) {
     sql(s"""CREATE TABLE badrecordTest13 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordTest13 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='ID,CUST_ID,cust_name')""").collect
    sql(s"""SHOW SEGMENTS FOR TABLE badrecordTest13""").collect
     sql(s"""drop table if exists badrecordTest13""").collect
  }


  //Create table and Load data with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true  for date and char types using single pass and vectorized reader parameters
  test("AR-Develop-Feature-BadRecords-001_PTS012_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest14 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test3.csv' into table badrecordtest14 OPTIONS('FILEHEADER'='ID,CUST_ID,cust_name','DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from badrecordTest14""",
      Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS012_TC001")
     sql(s"""drop table if exists badrecordTest14""").collect
  }


  //Check the data load with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true, data having ""(empty in double quote)
  test("AR-Develop-Feature-BadRecords-001_PTS021_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest15 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/doubleqoute.csv' into table badrecordtest15 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordTest15""",
      Seq(Row(1)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS021_TC001")
     sql(s"""drop table if exists badrecordTest15""").collect
  }


  //Check the data load with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true, data having  a,  insufficient column
  test("AR-Develop-Feature-BadRecords-001_PTS022_TC001", Include) {
    sql(s"""drop table if exists badrecordTest16""").collect
     sql(s"""CREATE TABLE badrecordtest16 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/insuffcient.csv' into table badrecordtest16 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE')""").collect
    checkAnswer(s"""select count(*) from badrecordTest16""",
      Seq(Row(2)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS022_TC001")
     sql(s"""drop table if exists badrecordTest16""").collect
  }


  //Check the data load with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true, data having ‘’ (empty in single quote)
  test("AR-Develop-Feature-BadRecords-001_PTS023_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest17 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/test6.csv' into table badrecordtest17 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'="'",'is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordTest17""",
      Seq(Row(3)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS023_TC001")
     sql(s"""drop table if exists badrecordTest17""").collect
  }


  //Check the data load with parameters BAD_RECORDS_ACTION=FAIL/FORCE/REDIRECT/IGNORE,BAD_RECORD_LOGGER_ENABLE=true/false and IS_EMPTY_DATA_BAD_RECORD=false/true, data having ,(empty comma)
  test("AR-Develop-Feature-BadRecords-001_PTS024_TC001", Include) {
     sql(s"""CREATE TABLE badrecordtest18 (ID int,CUST_ID int,cust_name string) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/badrecord/emptyComma.csv' into table badrecordtest18 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','is_empty_data_bad_record'='false','BAD_RECORDS_ACTION'='IGNORE','BAD_RECORDS_LOGGER_ENABLE'='TRUE')""").collect
    checkAnswer(s"""select count(*) from badrecordTest18""",
      Seq(Row(1)), "BadRecordTestCase_AR-Develop-Feature-BadRecords-001_PTS024_TC001")
     sql(s"""drop table if exists badrecordTest18""").collect
  }

}