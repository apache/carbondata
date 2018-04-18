
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
 * Test Class for partitionTestCase to verify all scenerios
 */

class PartitionTestCase extends QueryTest with BeforeAndAfterAll {
         

  //Verify exception if column in partitioned by is already specified in table schema
  test("Partition-Local-sort_TC001", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (INTEGER_COLUMN1 int)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='List','LIST_INFO'='1,3')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created with Partition
  ignore("Partition-Local-sort_TC002", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST','LIST_INFO'='3')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception partitioned by is not specified in the DDL
  test("Partition-Local-sort_TC003", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='List','NUM_PARTITIONS'='3')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if List info is not given with List type partition
  test("Partition-Local-sort_TC004", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='List')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //exception should not be thrown if Partition type is not given
  test("Partition-Local-sort_TC005", Include) {
    try {
      sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('LIST_INFO'='1,2')""").collect
      sql(s"""drop table if exists uniqdata""").collect
    } catch {
      case _ => assert(false)
    }
  }


  //Verify exception if Partition type is 'range' and LIST_INFO Is provided
  test("Partition-Local-sort_TC006", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'LIST_INFO'='1,2')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'range' and NUM_PARTITIONS Is provided
  test("Partition-Local-sort_TC007", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'NUM_PARTITIONS'='1')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created if Partition type is 'range' and RANGE_INFO Is provided
  ignore("Partition-Local-sort_TC008", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='20160302,20150302')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created if Partition type is 'LIST' and LIST_INFO Is provided
  test("Partition-Local-sort_TC009", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY (DOJ int)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,2')""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'LIST' and NUM_PARTITIONS Is provided
  test("Partition-Local-sort_TC010", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ int)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'NUM_PARTITIONS'='1')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'LIST' and RANGE_INFO Is provided
  test("Partition-Local-sort_TC011", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'RANGE_INFO'='20160302,20150302')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if datatype is not provided with partition column
  test("Partition-Local-sort_TC012", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='20160302,20150302')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if a non existent file header  is provided in partition
  test("Partition-Local-sort_TC013", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='20160302,20150302')

  LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    }
    sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition By Is empty
  test("Partition-Local-sort_TC014", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY ()STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')
  """).collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition
  test("Partition-Local-sort_TC015", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC015")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition and limit 1
  ignore("Partition-Local-sort_TC016", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select * from uniqdata limit 1""",
      Seq(Row("CUST_NAME_00002","ACTIVE_EMUI_VERSION_00002",null,null,null,12345678903.0000000000,22345678903.0000000000,1.123456749E10,-1.123456749E10,3,null,2)), "partitionTestCase_Partition-Local-sort_TC016")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition and select partition column
  test("Partition-Local-sort_TC017", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select CUST_ID from uniqdata order by CUST_ID limit 1""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC017")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if 2 partition columns are provided
  test("Partition-Local-sort_TC018", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""
  CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (CUST_ID int , DOJ timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with range partition with limit 1
  ignore("Partition-Local-sort_TC019", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='0,5,10,29')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select * from uniqdata limit 1""",
      Seq(Row("CUST_NAME_00003","ACTIVE_EMUI_VERSION_00003",null,null,null,12345678904.0000000000,22345678904.0000000000,1.123456749E10,-1.123456749E10,4,null,5)), "partitionTestCase_Partition-Local-sort_TC019")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with range partition
  ignore("Partition-Local-sort_TC020", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='0,5,10,29')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    sql(s"""select count(*) from uniqdata limit 1""").collect
    sql(s"""Seq(Row(28))""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with hash partition with limit 1
  ignore("Partition-Local-sort_TC021", Include) {
     sql(s"""drop table if exists uniqdata""").collect
   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='5')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select * from uniqdata limit 1""",
      Seq(Row("CUST_NAME_00003","ACTIVE_EMUI_VERSION_00003",null,null,null,12345678904.0000000000,22345678904.0000000000,1.123456749E10,-1.123456749E10,4,null,5)), "partitionTestCase_Partition-Local-sort_TC021")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with hash partition
  ignore("Partition-Local-sort_TC022", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='5')""").collect

   sql(s"""LOAD DATA INPATH  ''$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC022")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with List partition after compaction
  test("Partition-Local-sort_TC023", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC023")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with Range partition after compaction
  test("Partition-Local-sort_TC024", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='Range', 'RANGE_INFO'='0,5,10,30')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC024")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with Hash partition after compaction
  test("Partition-Local-sort_TC025", Include) {
    dropTable("uniqdata")
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='5')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC025")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify join operation on List partition
  test("Partition-Local-sort_TC026", Include) {
     sql(s"""drop table if exists uniqdata1""").collect
   sql(s"""drop table if exists uniqdata""").collect
   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect

   sql(s"""CREATE TABLE uniqdata1 (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    sql(s"""select a.cust_id, b.cust_id from uniqdata a, uniqdata1 b where a.cust_id > b.cust_id""").collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data when sublist is provided in LIST_INFO
  test("Partition-Local-sort_TC028", Include) {
     sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,(1,2),3')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC028")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception is thrown if partition column is dropped
  test("Partition-Local-sort_TC029", Include) {
    intercept[Exception] {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')

  alter table uniqdata drop columns(CUST_ID)

  """).collect
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify insert is successful on list partition
  ignore("Partition-Local-sort_TC030", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""").collect
   sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""").collect
   sql(s"""insert into table uniqdata values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""").collect
    checkAnswer(s"""select * from uniqdata""",
      Seq(Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",1),Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",0)), "partitionTestCase_Partition-Local-sort_TC030")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify insert is successful on range partition
  ignore("Partition-Local-sort_TC031", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='0,3,5')""").collect
   sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""").collect
   sql(s"""insert into table uniqdata values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""").collect
    checkAnswer(s"""select * from uniqdata""",
      Seq(Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",1),Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",0)), "partitionTestCase_Partition-Local-sort_TC031")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify insert is successful on HASH partition
  ignore("Partition-Local-sort_TC032", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='10')""").collect
   sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""").collect
   sql(s"""insert into table uniqdata values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""").collect
    sql(s"""select * from uniqdata""").collect
    sql(s"""Seq(Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",1),Row("a",1,"2015-07-01 00:00:00.0",5678,7654,23.4000000000,55.6000000000,7654.0,8765.0,33,"2015-07-01 00:00:00.0",0))""").collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with > filter condition and list partition
  test("Partition-Local-sort_TC033", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,0,3,4')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC033")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and list partition
  test("Partition-Local-sort_TC034", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,0,3,4')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC034")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and list partition
  test("Partition-Local-sort_TC035", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,0,3,4')""").collect

   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC035")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with > filter condition and range partition
  ignore("Partition-Local-sort_TC036", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1,0,3,4')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC036")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and list partition
  ignore("Partition-Local-sort_TC037", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1,0,3,4')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC037")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and list partition
  ignore("Partition-Local-sort_TC038", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1,0,3,4')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC038")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with > filter condition and hash partition
  ignore("Partition-Local-sort_TC039", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='1,0,3,4')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC039")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and hash partition
  ignore("Partition-Local-sort_TC040", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='1,0,3,4')""").collect
   sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC040")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and hash partition
  ignore("Partition-Local-sort_TC041", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='1,0,3,4')""").collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC041")
     sql(s"""drop table if exists uniqdata""").collect
  }

  override def afterAll {
    sql("drop table if exists uniqdata")
  }
}