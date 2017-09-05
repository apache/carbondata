
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
  test("NoInvertedindex-TC001", Include) {
    sql("drop table if exists uniqdata1")
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include
  test("NoInvertedindex-TC002", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure
  test("NoInvertedindex-TC003", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude
  test("NoInvertedindex-TC004", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include
  test("NoInvertedindex-TC005", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include
  test("NoInvertedindex-TC007", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension
  test("NoInvertedindex-TC008", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include
  test("NoInvertedindex-TC009", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude
  test("NoInvertedindex-TC010", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include
  test("NoInvertedindex-TC011", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include
  test("NoInvertedindex-TC013", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure
  test("NoInvertedindex-TC014", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and limit
  test("NoInvertedindex-TC015", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC015")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and count()
  test("NoInvertedindex-TC016", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC016")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and sum()
  test("NoInvertedindex-TC017", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC017")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and >= operator
  test("NoInvertedindex-TC018", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC018")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and !=
  test("NoInvertedindex-TC019", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC019")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and between
  test("NoInvertedindex-TC020", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC020")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and like
  test("NoInvertedindex-TC021", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC021")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and join
  test("NoInvertedindex-TC022", Include) {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dimension and having
  test("NoInvertedindex-TC023", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC023")
     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dimension and sortby
  test("NoInvertedindex-TC024", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC024")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dimension and groupby
  test("NoInvertedindex-TC025", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(CUST_ID) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC025")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and limit
  test("NoInvertedindex-TC026", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC026")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and count()
  test("NoInvertedindex-TC027", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC027")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and sum()
  test("NoInvertedindex-TC028", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC028")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and >= operator
  test("NoInvertedindex-TC029", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC029")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and !=
  test("NoInvertedindex-TC030", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC030")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and between
  test("NoInvertedindex-TC031", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC031")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and like
  test("NoInvertedindex-TC032", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC032")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and join
  test("NoInvertedindex-TC033", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_include and having
  test("NoInvertedindex-TC034", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC034")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and sortby
  test("NoInvertedindex-TC035", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC035")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and groupby
  test("NoInvertedindex-TC036", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(CUST_ID) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC036")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and limit
  test("NoInvertedindex-TC037", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC037")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and count()
  test("NoInvertedindex-TC038", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC038")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and sum()
  test("NoInvertedindex-TC039", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC039")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and >= operator
  test("NoInvertedindex-TC040", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC040")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and !=
  test("NoInvertedindex-TC041", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC041")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and between
  test("NoInvertedindex-TC042", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC042")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and like
  test("NoInvertedindex-TC043", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC043")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and join
  test("NoInvertedindex-TC044", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and having
  test("NoInvertedindex-TC045", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC045")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include and measure and sortby
  test("NoInvertedindex-TC046", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC046")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_include measure and groupby
  test("NoInvertedindex-TC047", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','NO_INVERTED_INDEX'='CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC047")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and limit
  test("NoInvertedindex-TC048", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC048")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and count()
  test("NoInvertedindex-TC049", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC049")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and sum()
  test("NoInvertedindex-TC050", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC050")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and >= operator
  test("NoInvertedindex-TC051", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC051")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and !=
  test("NoInvertedindex-TC052", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC052")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and between
  test("NoInvertedindex-TC053", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC053")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and like
  test("NoInvertedindex-TC054", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC054")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and join
  test("NoInvertedindex-TC055", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_exclude and having
  test("NoInvertedindex-TC056", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC056")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and sortby
  test("NoInvertedindex-TC057", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC057")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and groupby
  test("NoInvertedindex-TC058", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC058")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and limit
  test("NoInvertedindex-TC059", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC059")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and count()
  test("NoInvertedindex-TC060", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC060")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and sum()
  test("NoInvertedindex-TC061", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC061")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and >= operator
  test("NoInvertedindex-TC062", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC062")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and !=
  test("NoInvertedindex-TC063", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC063")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and between
  test("NoInvertedindex-TC064", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC064")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and like
  test("NoInvertedindex-TC065", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC065")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and join
  test("NoInvertedindex-TC066", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and having
  test("NoInvertedindex-TC067", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC067")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and sortby
  test("NoInvertedindex-TC068", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC068")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with dictionary_exclude and dictionary_include and groupby
  test("NoInvertedindex-TC069", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC069")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and limit
  test("NoInvertedindex-TC081", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata limit 100""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC081")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and count()
  test("NoInvertedindex-TC082", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(7)), "invertedindexTestCase_NoInvertedindex-TC082")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and sum()
  test("NoInvertedindex-TC083", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(INTEGER_COLUMN1) from uniqdata""",
      Seq(Row(28)), "invertedindexTestCase_NoInvertedindex-TC083")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and >= operator
  test("NoInvertedindex-TC084", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID >= 9001""",
      Seq(Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC084")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and !=
  test("NoInvertedindex-TC085", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where CUST_ID != 9001""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC085")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and between
  test("NoInvertedindex-TC086", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id between 9002 and 9030""",
      Seq(Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC086")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and like
  test("NoInvertedindex-TC087", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id Like '9%'""",
      Seq(Row(9000,"CUST_NAME_00000"),Row(9001,"CUST_NAME_00001"),Row(9002,"CUST_NAME_00002"),Row(9003,"CUST_NAME_00003"),Row(9004,"CUST_NAME_00004"),Row(9005,"CUST_NAME_00005"),Row(9006,"CUST_NAME_00006")), "invertedindexTestCase_NoInvertedindex-TC087")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and join
  test("NoInvertedindex-TC088", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata1""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdata.CUST_ID from uniqdata cross join uniqdata1 where uniqdata.CUST_ID > 9002 and uniqdata1.CUST_ID > 9003""").collect

     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""drop table if exists uniqdata1""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and having
  test("NoInvertedindex-TC089", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id from uniqdata where cust_id > 9000 group by cust_id having cust_id = 9002""",
      Seq(Row(9002)), "invertedindexTestCase_NoInvertedindex-TC089")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and sortby
  test("NoInvertedindex-TC090", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select cust_id,cust_name from uniqdata where cust_id > 9004 sort by cust_name desc""",
      Seq(Row(9006,"CUST_NAME_00006"),Row(9005,"CUST_NAME_00005")), "invertedindexTestCase_NoInvertedindex-TC090")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index for timestamp with dictionary_exclude and dictionary_include  and groupby
  test("NoInvertedindex-TC091", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID','DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION','NO_INVERTED_INDEX'='DOB')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select sum(cust_id) from uniqdata group by cust_id""",
      Seq(Row(9006),Row(9001),Row(9004),Row(9002),Row(9005),Row(9003),Row(9000)), "invertedindexTestCase_NoInvertedindex-TC091")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To check no_inverted_index with measure
  test("NoInvertedindex-TC092", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check alter drop column for no_inverted
  test("NoInvertedindex-TC097", Include) {
    sql(s"""drop table if exists uniqdata""").collect
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
  test("NoInvertedindex-TC101", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check dictionary exclude with no_inverted_index
  test("NoInvertedindex-TC102", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME','NO_INVERTED_INDEX'='CUST_NAME')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after Inserting the data
  test("NoInvertedindex-TC103", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""insert into uniqdata select '2','customerName','ACTIVE_EMUI_VERSION','2015-4-23 11:01:01','2015-4-23 11:01:01','45','56','4.5','6.5','3.2','2.5','36'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after minor Compaction
  test("NoInvertedindex-TC104", Include) {
    sql(s"""drop table if exists uniqdata""").collect
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
  test("NoInvertedindex-TC105", Include) {
    sql(s"""drop table if exists uniqdata""").collect
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
  test("NoInvertedindex-TC106", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on Low Cardinality Column
  test("NoInvertedindex-TC107", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Double_COLUMN1','NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after Inserting the data
  test("NoInvertedindex-TC108", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""insert into uniqdata select '2','customerName','ACTIVE_EMUI_VERSION','2015-4-23 11:01:01','2015-4-23 11:01:01','45','56','4.5','6.5','3.2','2.5','36'""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index after minor Compaction
  test("NoInvertedindex-TC109", Include) {
    sql(s"""drop table if exists uniqdata""").collect
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
  test("NoInvertedindex-TC110", Include) {
    sql(s"""drop table if exists uniqdata""").collect
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
  test("NoInvertedindex-TC111", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //To validate No Inverted Index on Low Cardinality Column
  test("NoInvertedindex-TC112", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Double_COLUMN1','NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }

  override def afterAll {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
  }
}