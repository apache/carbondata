
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
 * Test Class for invertedindexTestCase to verify all scenerios
 */

class InvertedindexTestCase extends QueryTest with BeforeAndAfterAll {
         

  //To check no_inverted_index with dimension
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC001", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC002", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC003", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC004", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC005", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC007", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC008", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC009", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC010", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC011", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC013", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC014", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC015", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC015")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC016", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC016")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC017", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC017")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC018", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC018")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC019", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC019")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC020", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC020")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC021", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC021")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC022", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dimension and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC023", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC023")
     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dimension and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC024", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC024")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC025", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(CUST_ID) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC025")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC026", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC026")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC027", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC027")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC028", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC028")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC029", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC029")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC030", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC030")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC031", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC031")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC032", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC032")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC033", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_include and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC034", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC034")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC035", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC035")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC036", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(CUST_ID) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC036")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC037", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC037")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC038", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC038")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC039", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC039")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC040", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC040")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC041", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC041")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC042", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC042")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC043", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC043")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC044", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC045", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC045")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC046", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC046")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include measure and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC047", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC047")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC048", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC048")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC049", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC049")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC050", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC050")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC051", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC051")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC052", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC052")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC053", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC053")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC054", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC054")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC055", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_exclude and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC056", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC056")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC057", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC057")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC058", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC058")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC059", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC059")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC060", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC060")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC061", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC061")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC062", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC062")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC063", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC063")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC064", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC064")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC065", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC065")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC066", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC067", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC067")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC068", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC068")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC069", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC069")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and limit
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC081", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC081")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and count()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC082", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC082")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and sum()
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC083", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC083")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and >= operator
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC084", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC084")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and !=
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC085", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC085")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and between
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC086", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC086")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and like
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC087", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC087")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and join
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC088", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and having
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC089", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC089")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and sortby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC090", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC090")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and groupby
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC091", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_AR-Develop-Feature-NoInvertedindex-001_PTS001_TC091")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with measure
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC092", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check alter drop column for no_inverted
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC097", Include) {
    try {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('COLUMN_GROUPS'='(CUST_NAME,ACTIVE_EMUI_VERSION)','DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""Alter table uniqdata drop columns(BIGINT_COLUMN1)""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      sql(s"""select BIGINT_COLUMN1 from uniqdata""").collect

      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check measure in no_inverted_index
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC101", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check dictionary exclude with no_inverted_index
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC102", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after Inserting the data
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC103", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""insert into uniqdata select '2','customerName','ACTIVE_EMUI_VERSION','2015-4-23 11:01:01','2015-4-23 11:01:01','45','56','4.5','6.5','3.2','2.5','36'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after minor Compaction
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC104", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""alter table uniqdata compact 'minor'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after major Compaction
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC105", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""alter table uniqdata compact 'major'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on High Cardinality Column
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC106", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on Low Cardinality Column
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC107", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Double_COLUMN1','NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after Inserting the data
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC108", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""insert into uniqdata select '2','customerName','ACTIVE_EMUI_VERSION','2015-4-23 11:01:01','2015-4-23 11:01:01','45','56','4.5','6.5','3.2','2.5','36'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after minor Compaction
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC109", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""alter table uniqdata compact 'minor'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after major Compaction
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC110", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""alter table uniqdata compact 'major'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on High Cardinality Column
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC111", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on Low Cardinality Column
  test("AR-Develop-Feature-NoInvertedindex-001_PTS001_TC112", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Double_COLUMN1','NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }

  override def afterAll {
    sql("drop table if exists uniqdata")
  }
}