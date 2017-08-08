
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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for partitionTestCase to verify all scenerios
 */

class PartitionTestCase extends QueryTest with BeforeAndAfterAll {

  val currentTimeFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  //Verify exception if column in partitioned by is already specified in table schema
  test("Partition-Local-sort_TC001", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int)
           | PARTITIONED BY (INTEGER_COLUMN1 int)STORED BY 'org.apache.carbondata.format'
           | TBLPROPERTIES('PARTITION_TYPE'='List','LIST_INFO'='1,3')""".stripMargin
          .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created with Partition
  test("Partition-Local-sort_TC002", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(
      s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         | DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int) PARTITIONED BY (DOJ int) STORED BY 'org.apache.carbondata.format'
         | TBLPROPERTIES('PARTITION_TYPE'='LIST','LIST_INFO'='3')""".stripMargin).collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception partitioned by is not specified in the DDL
  test("Partition-Local-sort_TC003", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(
      s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'
         | TBLPROPERTIES('PARTITION_TYPE'='List','NUM_PARTITIONS'='3')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if List info is not given with List type partition
  test("Partition-Local-sort_TC004", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp) STORED BY
           |'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='List')""".stripMargin
          .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is not given
  test("Partition-Local-sort_TC005", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp) STORED BY
           |'org.apache.carbondata.format' TBLPROPERTIES('LIST_INFO'='1,2')""".stripMargin
          .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'range' and LIST_INFO Is provided
  test("Partition-Local-sort_TC006", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double) PARTITIONED BY (DOJ timestamp) STORED BY 'org.apache
           |.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'LIST_INFO'='1,2')"""
          .stripMargin.replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'range' and NUM_PARTITIONS Is provided
  test("Partition-Local-sort_TC007", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp) STORED BY
           |'org.apache.carbondata.format'
           | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'NUM_PARTITIONS'='1')"""
          .stripMargin.replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created if Partition type is 'range' and RANGE_INFO Is provided
  test("Partition-Local-sort_TC008", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyyMMDD")
     sql(s"""drop table if exists uniqdata""").collect
    sql(
      s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)
         | STORED BY 'org.apache.carbondata.format'
         |  TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='20150302,20160302')"""
        .stripMargin.replaceAll(System.lineSeparator, "")).collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify table is created if Partition type is 'LIST' and LIST_INFO Is provided
  test("Partition-Local-sort_TC009", Include) {
     sql(s"""drop table if exists uniqdata""").collect
    sql(
      s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
         | DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
         | DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
         | Double_COLUMN2 double) PARTITIONED BY (DOJ int) STORED BY 'org.apache.carbondata.format'
         |  TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,2')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'LIST' and NUM_PARTITIONS Is provided
  test("Partition-Local-sort_TC010", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ int)
           | STORED BY 'org.apache.carbondata.format'
           | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'NUM_PARTITIONS'='1')""".stripMargin
          .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition type is 'LIST' and RANGE_INFO Is provided
  test("Partition-Local-sort_TC011", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)
           | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
           | 'RANGE_INFO'='20160302,20150302')""".stripMargin.replaceAll(System.lineSeparator, ""))
        .collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if datatype is not provided with partition column
  test("Partition-Local-sort_TC012", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
           |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
           |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
           |Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (DOJ)
           | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
           |  'LIST_INFO'='20160302,20150302')""".stripMargin.replaceAll(System.lineSeparator, ""))
        .collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if a non existent file header  is provided in partition
  test("Partition-Local-sort_TC013", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,
        DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        INTEGER_COLUMN1 int) PARTITIONED BY (DOJ timestamp)
         STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
          'LIST_INFO'='20160302,20150302')""".stripMargin.replaceAll(System.lineSeparator, ""))
        .collect

  sql(
    s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
       | table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,
       | ACTIVE_EMUI_VERSION,DOJ,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,
       | Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
      .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if Partition By Is empty
  test("Partition-Local-sort_TC014", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY ()
        STORED BY 'org.apache.carbondata.format'
         TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition
  test("Partition-Local-sort_TC015", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC015")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition and limit 1
  ignore("Partition-Local-sort_TC016", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select * from uniqdata limit 1""",
      Seq(Row("CUST_NAME_00002","ACTIVE_EMUI_VERSION_00002",null,null,null,12345678903.0000000000,22345678903.0000000000,1.123456749E10,-1.123456749E10,3,null,2)), "partitionTestCase_Partition-Local-sort_TC016")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify load with List Partition and select partition column
  test("Partition-Local-sort_TC017", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |   'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"',
        | 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,
        | BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,
        | Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
       .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select CUST_ID from uniqdata order by CUST_ID limit 1""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC017")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception if 2 partition columns are provided
  test("Partition-Local-sort_TC018", Include) {
    try {
       sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
           | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
           | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
           | INTEGER_COLUMN1 int) PARTITIONED BY (CUST_ID int , DOJ timestamp)
           |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
           |   'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with range partition with limit 1
  test("Partition-Local-sort_TC019", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         |  'RANGE_INFO'='0,5,10,29')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int )
         |  row format delimited fields terminated by ','""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin).collect


    checkAnswer(
      s"""select * from uniqdata order by cust_id, cust_name""",
      s"""select * from uniqdata_hive order by cust_id, cust_name""",
      "partitionTestCase_Partition-Local-sort_TC019"
    )

    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with range partition
  test("Partition-Local-sort_TC020", Include) {
    dropTable("uniqdata")
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string,
          |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,
          |DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
          |Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
          |  'RANGE_INFO'='0,5,10,29')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(28)), "SR-DataSight-Carbon-Partition-Local-sort-PTS001_TC020")
    sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with hash partition with limit 1
  test("Partition-Local-sort_TC021", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
         |  'NUM_PARTITIONS'='5')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp, cust_id int ) row format delimited fields
         | terminated by ','""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,
         |ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,
         |Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
         | table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect

    checkAnswer(s"""select * from uniqdata order by cust_id, cust_name""",
      s"""select * from uniqdata_hive order by cust_id, cust_name""",
      "partitionTestCase_SR-DataSight-Carbon-Partition-Local-sort-PTS001_TC019"
    )

    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with hash partition
  test("Partition-Local-sort_TC022", Include) {
    dropTable("uniqdata")
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
          |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
          |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
          |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
          |  'NUM_PARTITIONS'='5')"""
         .stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,
        |ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,
        |Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
       .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC022")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with List partition after compaction
  test("Partition-Local-sort_TC023", Include) {
    dropTable("uniqdata")
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
          |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
          |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
          |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
          |  'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,
        |DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,
        |DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,
        |DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC023")
     sql(s"""drop table if exists uniqdata""").collect
  }

  //Verify data load with Range partition after compaction
  test("Partition-Local-sort_TC024", Include) {
    dropTable("uniqdata")
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
          |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
          |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
          |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='Range',
          | 'RANGE_INFO'='0,5,10,30')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC024")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data load with Hash partition after compaction
  test("Partition-Local-sort_TC025", Include) {
    dropTable("uniqdata")
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
          |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
          |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
          |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
          |  'NUM_PARTITIONS'='5')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(s"""alter table uniqdata compact 'minor'""").collect
    checkAnswer(s"""select count(*) from uniqdata limit 1""",
      Seq(Row(112)), "partitionTestCase_Partition-Local-sort_TC025")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify join operation on List partition
  test("Partition-Local-sort_TC026", Include) {
     sql(s"""drop table if exists uniqdata1""").collect
   sql(s"""drop table if exists uniqdata""").collect
   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""CREATE TABLE uniqdata1 (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata1 OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(s"""select a.cust_id, b.cust_id from uniqdata a, uniqdata1 b where a.cust_id > b.cust_id""")
      .collect

     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify data when sublist is provided in LIST_INFO
  test("Partition-Local-sort_TC028", Include) {
     sql(
       s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
          |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
          |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
          |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
          | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
          |'LIST_INFO'='0,(1,2),3')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
        | table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"',
        |'FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,
        |DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,
        |CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata""",
      Seq(Row(28)), "partitionTestCase_Partition-Local-sort_TC028")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify exception is thrown if partition column is dropped
  test("Partition-Local-sort_TC029", Include) {
    try {
      sql(s"""drop table if exists uniqdata""").collect
      sql(
        s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache
        .carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='0,1')"""
          .stripMargin.replaceAll(System.lineSeparator, "")).collect

      sql(s"""alter table uniqdata drop columns(CUST_ID)""").collect
      assert(false)
    } catch {
      case _ => assert(true)
    }
    sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify insert is successful on list partition
  test("Partition-Local-sort_TC030", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
         | 'LIST_INFO'='0,1')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB
         | timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int)
         | row format delimited fields terminated by ',' """.stripMargin).collect

    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into
         | table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(s"""insert into table uniqdata select * from uniqdata_hive""")

    checkAnswer(
      s"""select * from uniqdata""",
      s"""select * from uniqdata_hive""",
      "partitionTestCase_Partition-Local-sort_TC030")

    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }


  //Verify insert is successful on range partition
  test("Partition-Local-sort_TC031", Include) {
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_par""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         |'RANGE_INFO'='0,3,5')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55
         |.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4,
         |55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""CREATE TABLE uniqdata_par (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB
         | timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp, cust_id int)
         |STORED as parquet """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata_par values ('a', '1','2015-07-01 00:00:00', 5678,7654,23
         |.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata_par values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23
         |.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    checkAnswer(
      s"""select * from uniqdata""",
      s"""select * from uniqdata_par""",
      "partitionTestCase_Partition-Local-sort_TC031"
    )

    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_par""").collect
  }


  //Verify insert is successful on HASH partition
  test("Partition-Local-sort_TC032", Include) {
     sql(s"""drop table if exists uniqdata""").collect
     sql(s"""drop table if exists uniqdata_par""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
        |  'NUM_PARTITIONS'='10')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4,
        | 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""".stripMargin
       .replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""insert into table uniqdata values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4,
        |55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""".stripMargin
       .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""CREATE TABLE uniqdata_par (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int)  STORED as parquet """.stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata_par values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4,
         |55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""insert into table uniqdata_par values ('a', '1', '2015-07-01 00:00:00', 5678,7654,23.4,
         | 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    checkAnswer(s"""select * from uniqdata""",
      s"""select * from uniqdata_par""", "SR-DataSight-Carbon-Partition-Local-sort-PTS001_TC032")

    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_par""").collect
  }


  //Verify date with > filter condition and list partition
  test("Partition-Local-sort_TC033", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='1,0,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC033")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and list partition
  test("Partition-Local-sort_TC034", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |   'LIST_INFO'='1,0,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC034")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and list partition
  test("Partition-Local-sort_TC035", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |   'LIST_INFO'='1,0,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect

   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC035")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with > filter condition and range partition
  test("Partition-Local-sort_TC036", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        |  'RANGE_INFO'='0,1,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC036")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and list partition
  test("Partition-Local-sort_TC037", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='0,1,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC037")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and list partition
  test("Partition-Local-sort_TC038", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='0,1,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC038")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with > filter condition and hash partition
  test("Partition-Local-sort_TC039", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        | INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
        |   'NUM_PARTITIONS'='10')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID>3""",
      Seq(Row(4)), "partitionTestCase_Partition-Local-sort_TC039")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = filter condition and hash partition
  test("Partition-Local-sort_TC040", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
        | 'NUM_PARTITIONS'='10')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
   sql(
     s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
        | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
        | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
        | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
        | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=3""",
      Seq(Row(8)), "partitionTestCase_Partition-Local-sort_TC040")
     sql(s"""drop table if exists uniqdata""").collect
  }


  //Verify date with = value not in list_info and hash partition
  test("Partition-Local-sort_TC041", Include) {
     sql(s"""drop table if exists uniqdata""").collect

   sql(
     s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
        |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
        |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
        |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
        | 'NUM_PARTITIONS'='10')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=10""",
      Seq(Row(0)), "partitionTestCase_Partition-Local-sort_TC041")
     sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-verify_hash_null_key", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
         | 'NUM_PARTITIONS'='10')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/2000_UniqData_partition_1.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=0""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-verify_hash_null_key")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-verify_range_null_key", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         | 'RANGE_INFO'='0,1,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/2000_UniqData_partition_1.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=0""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-verify_range_null_key")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_ID int,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_NAME string)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00001,CUST_NAME_00002,CUST_NAME_00003')"""
        .stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where CUST_ID=0""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-string_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-long_partition_column", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME STRING, CUST_ID int,ACTIVE_EMUI_VERSION string,
         |DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double,
         | Double_COLUMN2 double,DOJ timestamp) PARTITIONED BY (LONG_COLUMN1 LONG)
         |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         |  'RANGE_INFO'='1,2,3,4')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | LONG_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where LONG_COLUMN1=1""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-long_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-double_partition_column", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN2 double,INTEGER_COLUMN1 int,
         |DOJ timestamp, CUST_ID int) PARTITIONED BY (Double_COLUMN1 double)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         |  'RANGE_INFO'='11234567490')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where Double_COLUMN1=11234567490""",
      Seq(Row(28)), "Apache-CarbonData-Partition-Local-sort-double_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-decimal_partition_column", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp,
         |CUST_ID int) PARTITIONED BY (DECIMAL_COLUMN2 decimal(36,10))
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         | 'RANGE_INFO'='12345678901, 12345678902')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/2000_UniqData_partition_1.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where DECIMAL_COLUMN2=22345678901""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-decimal_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-short_partition_column", Include) {
    sql(s"""drop table if exists uniqdata""").collect

    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | Double_COLUMN1 double, Double_COLUMN2 double,DOJ timestamp, CUST_ID int,
         | DECIMAL_COLUMN2 decimal(36,10)) PARTITIONED BY (INTEGER_COLUMN1 short)
         |  STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         |  'RANGE_INFO'='1, 2')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/2000_UniqData_partition_1.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where INTEGER_COLUMN1=1""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-short_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-timestamp_partition_column", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |Double_COLUMN1 double, Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10),
         |INTEGER_COLUMN1 short) PARTITIONED BY (DOJ timestamp)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='HASH',
         | 'num_partitions'='3')""".stripMargin.replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/2000_UniqData_partition_1.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    checkAnswer(s"""select count(*) from uniqdata where DOJ='1970-01-02 01:00'""",
      Seq(Row(1)), "Apache-CarbonData-Partition-Local-sort-timestamp_partition_column")
    sql(s"""drop table if exists uniqdata""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_range_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         | bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         | Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1 short,
         | DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE' ,
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00005,CUST_NAME_00010')""".stripMargin
        .replaceAll(System
        .lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int) row format delimited fields terminated
         | by ','""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect

    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where (cust_name > 'CUST_NAME_00000' AND
         |cust_name <=
         |'CUST_NAME_00015') OR (cust_name >= 'CUST_NAME_00021' AND cust_name <=
         |'CUST_NAME_00025')""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where (cust_name > 'CUST_NAME_00000' AND
         | cust_name <=
         |'CUST_NAME_00015') OR (cust_name >= 'CUST_NAME_00021' AND cust_name <=
         |'CUST_NAME_00025')""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_range_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_range_NOT_OR_filter",
    Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         | bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         | Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1 short,
         | DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE' ,
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00005,CUST_NAME_00010')""".stripMargin
        .replaceAll(System
          .lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int) row format delimited fields terminated
         | by ','""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect

    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where (cust_name > 'CUST_NAME_00000' AND
         |cust_name <=
         |'CUST_NAME_00015') OR NOT (cust_name >= 'CUST_NAME_00021' AND cust_name <=
         |'CUST_NAME_00025')""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where (cust_name > 'CUST_NAME_00000' AND
         | cust_name <=
         |'CUST_NAME_00015') OR NOT (cust_name >= 'CUST_NAME_00021' AND cust_name <=
         |'CUST_NAME_00025')""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_range_NOT_OR_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_range_lessThan_filter",
    Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         | bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         | Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1 short,
         | DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE' ,
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00005,CUST_NAME_00010')""".stripMargin
        .replaceAll(System
          .lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp, cust_id int) row format delimited fields terminated
         | by ','""".stripMargin.replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect

    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where (cust_name <=
         |'CUST_NAME_00015' OR ACTIVE_EMUI_VERSION < 'ACTIVE_EMUI_VERSION_00010')""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where (cust_name <=
         |'CUST_NAME_00015' OR ACTIVE_EMUI_VERSION < 'ACTIVE_EMUI_VERSION_00010')""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_range_lessThan_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_equalTo_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,
         |BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1 short,
         |DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00005,CUST_NAME_00010')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         | row format delimited fields terminated by ','""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select * from uniqdata where cust_name = 'CUST_NAME_00000' AND cust_name =
         |'CUST_NAME_00020'""".stripMargin,
      s"""select * from uniqdata_hive where cust_name = 'CUST_NAME_00000' AND cust_name =
         |'CUST_NAME_00020'""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_equalTo_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_equalTo_null_filter",
    Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,
         |BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10),
         |INTEGER_COLUMN1 short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE',
         | 'RANGE_INFO'='CUST_NAME_00000,CUST_NAME_00005,CUST_NAME_00010')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         | row format delimited fields terminated by ','""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select * from uniqdata where cust_name is null""".stripMargin,
      s"""select * from uniqdata_hive where cust_name is null""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_equalTo_null_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort-string_partition_column_list_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         |bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1
         |short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST' ,
         |'LIST_INFO'='CUST_NAME_00008,CUST_NAME_00017')""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         | row format delimited fields terminated by ','""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin
        .replaceAll(System.lineSeparator, "")).collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where cust_name >= 'CUST_NAME_00000' AND
         |cust_name <= 'CUST_NAME_00020'""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where cust_name >= 'CUST_NAME_00000' AND
         |cust_name <= 'CUST_NAME_00020'""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_list_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort" +
    "-string_partition_column_list_GreaterLessThan_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         |bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1
         |short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST' ,
         |'LIST_INFO'='CUST_NAME_00008,CUST_NAME_00017')""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         | row format delimited fields terminated by ','""".stripMargin.replaceAll(System
        .lineSeparator, "")).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin).collect
    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where cust_name > 'CUST_NAME_00000' AND
         |cust_name < 'CUST_NAME_00020'""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where cust_name > 'CUST_NAME_00000' AND
         |cust_name < 'CUST_NAME_00020'""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_list_GreaterLessThan_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort" +
    "-string_partition_column_list_OR_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         |bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1
         |short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='LIST' ,
         |'LIST_INFO'='CUST_NAME_00008,CUST_NAME_00017')""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         |  row format delimited fields terminated by ','""".stripMargin).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') OR
         |ACTIVE_EMUI_VERSION NOT IN ('ACTIVE_EMUI_VERSION_00019')""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') OR
         |ACTIVE_EMUI_VERSION NOT IN ('ACTIVE_EMUI_VERSION_00019')""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_list_OR_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort" +
    "-string_partition_column_range_IN_filter", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         |bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1
         |short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE' ,
         |'RANGE_INFO'='CUST_NAME_00008,CUST_NAME_00017')""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         |  row format delimited fields terminated by ','""".stripMargin).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') AND
         |ACTIVE_EMUI_VERSION != 'ACTIVE_EMUI_VERSION_00019'""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') AND
         |ACTIVE_EMUI_VERSION != 'ACTIVE_EMUI_VERSION_00019'""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-string_partition_column_range_IN_filter")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }

  test("Apache-CarbonData-Partition-Local-sort" +
    "-major_compaction", Include) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd/MM/yyyy HH:mm")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
    sql(
      s"""CREATE TABLE uniqdata (ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1
         |bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), Double_COLUMN1 double,
         |Double_COLUMN2 double,CUST_ID int, DECIMAL_COLUMN2 decimal(36,10), INTEGER_COLUMN1
         |short,DOJ timestamp) PARTITIONED BY (CUST_NAME String)
         |STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('PARTITION_TYPE'='RANGE' ,
         |'RANGE_INFO'='CUST_NAME_00008,CUST_NAME_00017')""".stripMargin).collect
    sql(
      s"""CREATE TABLE uniqdata_hive (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         | BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         | DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         | INTEGER_COLUMN1 int, DOJ timestamp,CUST_ID int)
         |  row format delimited fields terminated by ','""".stripMargin).collect

    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,
         | BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,
         | INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin.replaceAll(System.lineSeparator, ""))
      .collect
    sql(
      s"""LOAD DATA INPATH '$resourcesPath/Data/partition/2000_UniqData_partition.csv'
         | into table uniqdata_hive """.stripMargin.replaceAll(System.lineSeparator, "")).collect
    checkAnswer(
      s"""select ACTIVE_EMUI_VERSION from uniqdata where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') AND
         |ACTIVE_EMUI_VERSION != 'ACTIVE_EMUI_VERSION_00019'""".stripMargin,
      s"""select ACTIVE_EMUI_VERSION from uniqdata_hive where cust_name IN ('CUST_NAME_00000',
         |'CUST_NAME_00022','CUST_NAME_00019') AND
         |ACTIVE_EMUI_VERSION != 'ACTIVE_EMUI_VERSION_00019'""".stripMargin,
      "Apache-CarbonData-Partition-Local-sort-major_compaction")
    sql(s"""drop table if exists uniqdata""").collect
    sql(s"""drop table if exists uniqdata_hive""").collect
  }


  override def afterAll {
    sql("drop table if exists uniqdata")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, currentTimeFormat)
  }
}