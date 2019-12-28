
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

  //To check no_inverted_index with dimension
  test("NoInvertedindex-TC008", Include) {
    sql(s"""drop table if exists uniqdata""").collect
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
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

  //To check no_inverted_index with measure
  test("NoInvertedindex-TC092", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_ID')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //to check alter drop column for no_inverted
  test("NoInvertedindex-TC097", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    intercept[Exception] {
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('COLUMN_GROUPS'='(CUST_NAME,ACTIVE_EMUI_VERSION)', 'NO_INVERTED_INDEX'='CUST_NAME')""").collect
      sql(s"""Alter table uniqdata drop columns(BIGINT_COLUMN1)""").collect
      sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
      sql(s"""select BIGINT_COLUMN1 from uniqdata""").collect
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
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='CUST_NAME')""").collect
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
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
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
     sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('NO_INVERTED_INDEX'='Double_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/noinverted.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdata""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }

  override def afterAll {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
  }
}