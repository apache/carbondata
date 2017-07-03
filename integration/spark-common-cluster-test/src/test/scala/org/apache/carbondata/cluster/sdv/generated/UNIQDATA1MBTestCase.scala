
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
 * Test Class for uniqdata1mb to verify all scenerios
 */

class UNIQDATA1MBTestCase extends QueryTest with BeforeAndAfterAll {
         

//create_table_blocksize_01_drop
test("create_table_blocksize_01_drop", Include) {
  sql(s"""drop table if exists  uniqdata_1mb""").collect

  sql(s"""drop table if exists  uniqdata_1mb_hive""").collect

}
       

//create_table_blocksize_01
test("create_table_blocksize_01", Include) {
  sql(s"""CREATE TABLE uniqdata_1mb (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect

  sql(s"""CREATE TABLE uniqdata_1mb_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//create_table_blocksize_02
test("create_table_blocksize_02", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1mb OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1mb_hive """).collect

}
       

//create_table_blocksize_03
test("create_table_blocksize_03", Include) {
  sql(s"""select max(to_date(DOB)),min(to_date(DOB)),count(to_date(DOB)) from uniqdata_1mb where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}

  //create_table_blocksize_07_drop
  test("create_table_blocksize_07_drop", Include) {
    sql(s"""drop table if exists  uniqdata_50mb""").collect

    sql(s"""drop table if exists  uniqdata_50mb_hive""").collect

  }


  //create_table_blocksize_07
  test("create_table_blocksize_07", Include) {
    sql(s"""CREATE TABLE uniqdata_50mb (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='50')""").collect

    sql(s"""CREATE TABLE uniqdata_50mb_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  }


  //create_table_blocksize_08
  test("create_table_blocksize_08", Include) {
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_50mb OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_50mb_hive """).collect

  }


  //create_table_blocksize_09
  test("create_table_blocksize_09", Include) {
    sql(s"""select sum(concat(CUST_ID)),sum(concat(CUST_NAME)),sum(concat(DOB)),sum(concat(BIGINT_COLUMN1)),sum(concat(DECIMAL_COLUMN1)),sum(concat(Double_COLUMN1)) from uniqdata_50mb where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
  }


  //create_table_blocksize_16_drop
  test("create_table_blocksize_16_drop", Include) {
    sql(s"""drop table if exists  uniqdata_1000mb""").collect

    sql(s"""drop table if exists  uniqdata_1000mb_hive""").collect

  }


  //create_table_blocksize_16
  test("create_table_blocksize_16", Include) {
    sql(s"""CREATE TABLE uniqdata_1000mb (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1000')""").collect

    sql(s"""CREATE TABLE uniqdata_1000mb_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  }


  //create_table_blocksize_17
  test("create_table_blocksize_17", Include) {
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1000mb OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1000mb_hive """).collect

  }


  //create_table_blocksize_18
  test("create_table_blocksize_18", Include) {
    sql(s"""select max(lower(CUST_NAME)),min(lower(CUST_NAME)),avg(lower(CUST_NAME)),count(lower(CUST_NAME)),sum(lower(CUST_NAME)),variance(lower(CUST_NAME)) from uniqdata_1000mb where lower(CUST_NAME)=15 or lower(CUST_NAME) is NULL or lower(CUST_NAME) is NOT NULL""").collect
  }

  //create_table_blocksize_10_drop
  test("create_table_blocksize_10_drop", Include) {
    sql(s"""drop table if exists  uniqdata_1024mb""").collect

    sql(s"""drop table if exists  uniqdata_1024mb_hive""").collect

  }


  //create_table_blocksize_10
  test("create_table_blocksize_10", Include) {
    sql(s"""CREATE TABLE uniqdata_1024mb (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1024')""").collect

    sql(s"""CREATE TABLE uniqdata_1024mb_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  }


  //create_table_blocksize_11
  test("create_table_blocksize_11", Include) {
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1024mb OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_1024mb_hive """).collect

  }


  //create_table_blocksize_12
  test("create_table_blocksize_12", Include) {
    sql(s"""select length(CUST_NAME) from uniqdata_1024mb where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL limit 10""").collect
  }


  //create_table_blocksize_13_drop
  test("create_table_blocksize_13_drop", Include) {
    sql(s"""drop table if exists  uniqdata_2000mb""").collect

    sql(s"""drop table if exists  uniqdata_2000mb_hive""").collect

  }


  //create_table_blocksize_13
  test("create_table_blocksize_13", Include) {
    sql(s"""CREATE TABLE uniqdata_2000mb (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2000')""").collect

    sql(s"""CREATE TABLE uniqdata_2000mb_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  }


  //create_table_blocksize_14
  test("create_table_blocksize_14", Include) {
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_2000mb OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_2000mb_hive """).collect

  }


  //create_table_blocksize_15
  test("create_table_blocksize_15", Include) {
    sql(s"""select max(length(CUST_NAME)),min(length(CUST_NAME)),avg(length(CUST_NAME)),count(length(CUST_NAME)),sum(length(CUST_NAME)),variance(length(CUST_NAME)) from uniqdata_2000mb where length(CUST_NAME)=15 or length(CUST_NAME) is NULL or length(CUST_NAME) is NOT NULL""").collect
  }
       
override def afterAll {
sql("drop table if exists uniqdata_1mb")
sql("drop table if exists uniqdata_1mb_hive")
  sql("drop table if exists uniqdata_50mb")
  sql("drop table if exists uniqdata_50mb_hive")
  sql("drop table if exists uniqdata_1000mb")
  sql("drop table if exists uniqdata_1000mb_hive")
  sql("drop table if exists uniqdata_1024mb")
  sql("drop table if exists uniqdata_1024mb_hive")
  sql("drop table if exists uniqdata_2000mb")
  sql("drop table if exists uniqdata_2000mb_hive")
}
}