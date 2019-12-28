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

class StandardPartitionTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
  }

  //Verify exception if column in partitioned by is already specified in table schema
  test("Standard-Partition_TC001", Include) {
    sql(s"""drop table if exists uniqdata""")
    intercept[Exception] {
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (INTEGER_COLUMN1 int)STORED BY 'carbondata'""")
    }
  }

  //Verify table is created with Partition
  test("Standard-Partition_TC002", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY (INTEGER_COLUMN1 int)STORED BY 'carbondata'""")
    val df = sql(s"""DESC uniqdata""")
    assert(df.collect().reverse.head.get(0).toString.toUpperCase.contains("INTEGER_COLUMN1"))
  }

  //Verify table is created with Partition with table comment
  test("Standard-Partition_TC003",Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, decimalField DECIMAL(18,2), charField CHAR(5), floatField FLOAT ) COMMENT 'partition_table' PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    val df = sql(s"""DESC formatted partition_table""")
    checkExistence(df, true, "partition_table")
  }

  //Verify Exception while creating a partition table, with ARRAY type partitioned column
  test("Standard-Partition_TC004", Include) {
    sql(s"""drop table if exists partition_table_array""")
    intercept[Exception] {
      sql(s"""CREATE TABLE partition_table_array(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (complexData ARRAY<STRING>) STORED BY 'carbondata'""")
    }
    sql(s"""drop table if exists partition_table_array""")

  }

  //Verify exception if datatype is not provided with partition column
  test("Standard-Partition_TC007", Include) {
    sql(s"""drop table if exists uniqdata""")
    intercept[Exception] {
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY (DOJ)STORED BY 'carbondata'""")
    }
  }

  //Verify exception if non existent file header is provided in partition
  test("Standard-Partition_TC008", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY (DOJ timestamp)STORED BY 'carbondata'""")
    intercept[Exception] {
      sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata OPTIONS('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOJ,,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    }
  }

  //Verify exception if PARTITION BY is empty
  test("Standard-Partition_TC009", Include) {
    sql(s"""drop table if exists uniqdata""")
    intercept[Exception] {
      sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double) PARTITIONED BY() STORED BY 'carbondata'""")
    }
  }

  //Loading data into partitioned table with SORT_SCOPE=LOCAL_SORT
  test("Standard-Partition_TC010", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(10)))
  }

  //Loading data into partitioned table with SORT_SCOPE=GLOBAL_SORT
  test("Standard-Partition_TC011", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(10)))
  }

  //Loading data into partitioned table with SORT_SCOPE=NO_SORT
  test("Standard-Partition_TC013", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='NO_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(10)))
  }

  //Loading data into a partitioned table with Bad Records Action = FORCE
  test("Standard-Partition_TC014", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('BAD_RECORDS_ACTION'='FORCE')""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(10)))
  }

  //Verify Exception when Loading data into a partitioned table with Bad Records Action = FAIL
  test("Standard-Partition_TC015", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    intercept[Exception] {
      sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('FILEHEADER'='shortfield,intfield,bigintfield,doublefield,stringfield,timestamp,decimalfield,datefield,charfield,floatfield','BAD_RECORDS_ACTION'='FAIL')""")
    }
  }

  //Loading data into a partitioned table with Bad Records Action = IGNORE
  test("Standard-Partition_TC016", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('FILEHEADER'='shortfield,intfield,bigintfield,doublefield,stringfield,timestamp,decimalfield,datefield,charfield,floatfield','BAD_RECORDS_ACTION'='IGNORE')""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(0)))
  }

  //Loading data into a partitioned table with Bad Records Action = REDIRECT
  test("Standard-Partition_TC017", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('FILEHEADER'='shortfield,intfield,bigintfield,doublefield,stringfield,timestamp,decimalfield,datefield,charfield,floatfield','BAD_RECORDS_ACTION'='REDIRECT')""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(0)))
  }

  //Verify load with Standard Partition
  test("Standard-Partition_TC020", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select count(*) from uniqdata"""), Seq(Row(28)))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify load with Standard Partition with limit 1
  test("Standard-Partition_TC021", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select * from uniqdata limit 1"""),Seq(Row("CUST_NAME_00000","ACTIVE_EMUI_VERSION_00000",null,null,null,12345678901.0000000000,22345678901.0000000000,1.123456749E10,-1.123456749E10,1,null,1)))
  }

  //Verify load with Standard Partition with select partition column
  test("Standard-Partition_TC022", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select CUST_ID from uniqdata limit 1"""), Seq(Row(1)))
  }

  //Verify table creation if 2 partition columns are provided
  test("Standard-Partition_TC023", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (CUST_ID int,DOJ timestamp) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1',doj) OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    val df = sql(s"""DESC uniqdata""").collect()
    assert(df(df.indexWhere(_.get(0).toString.contains("# Partition")) + 3).get(0).toString.contains("doj"))
    assert(df(df.indexWhere(_.get(0).toString.contains("# Partition")) + 2).get(0).toString.contains("cust_id"))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify load with Standard Partition after compaction
  test("Standard-Partition_TC024", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""alter table uniqdata compact 'minor'""")
    checkAnswer(sql(s"""select count(*) from uniqdata"""), Seq(Row(84)))
  }

  //Verify join operation on Standard Partition
  test("Standard-Partition_TC025", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""drop table if exists uniqdata1""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""CREATE TABLE uniqdata1 (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata1 partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select a.cust_id, b.cust_id from uniqdata a, uniqdata1 b where a.cust_id >= b.cust_id limit 1"""),Seq(Row(1,1)))
    sql(s"""drop table if exists uniqdata1""")
  }

  //Verify exception if partition column is dropped
  test("Standard-Partition_TC026", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    intercept[Exception] {
      sql(s"""alter table uniqdata drop columns(CUST_ID)""")
    }
  }

  //Verify INSERT operation on standard partition
  test("Standard-Partition_TC027", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""")
    sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""")
    checkAnswer(sql(s"""select count(*) from uniqdata"""), Seq(Row(2)))
  }

  //Verify INSERT INTO SELECT operation on standard partition
  test("Standard-Partition_TC028", Include) {
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE_load""")
    sql(s"""CREATE TABLE partition_table_load (shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table_load options ('BAD_RECORDS_ACTION'='FORCE')""")
    sql(s"""INSERT into TABLE partition_table PARTITION (stringfield = 'Hello') SELECT * FROM partition_table_load au WHERE au.intfield = 25""")
    checkAnswer(sql(s"""select count(*) from partition_table"""),Seq(Row(2)))
    sql(s"""drop table if exists PARTITION_TABLE""")
    sql(s"""drop table if exists PARTITION_TABLE_load""")
  }

  //Verify INSERT overwrite operation on standard partition
  test("Standard-Partition_TC029", Include) {
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE_load""")
    sql(s"""CREATE TABLE partition_table_load (shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table_load options ('BAD_RECORDS_ACTION'='FORCE')""")
    sql(s"""INSERT OVERWRITE TABLE partition_table PARTITION (stringfield = 'Hello') SELECT * FROM partition_table_load au WHERE au.intField = 25""")
    sql(s"""INSERT OVERWRITE TABLE partition_table PARTITION (stringfield = 'Hello') SELECT * FROM partition_table_load au WHERE au.intField = 25""")
    checkAnswer(sql(s"""select count(*) from partition_table"""),Seq(Row(2)))
    sql(s"""drop table if exists PARTITION_TABLE""")
    sql(s"""drop table if exists PARTITION_TABLE_load""")
  }

  //Verify date with > filter condition and standard partition
  test("Standard-Partition_TC030", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='4') OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select count(*) from uniqdata where CUST_ID>3"""),Seq(Row(28)))
  }

  //Verify date with = filter condition and standard partition
  test("Standard-Partition_TC031", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='4')OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select count(*) from uniqdata where CUST_ID=3"""),Seq(Row(0)))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC032", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = "Hello")""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""),Seq(Row("China")))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC033", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where charfield = 'c'""").collect
    sql("""update partition_table set (stringfield)=('China123') where stringfield != 'China'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""),Seq(Row("China")))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC034", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('China123') where stringfield != 'China'""").collect
    sql("""update partition_table set (stringfield)=('Japan') where stringfield > 'China'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("China")))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC035", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('Asia') where stringfield < 'Hello'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Hello")))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC036", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('Europe') where stringfield LIKE 'C%'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Europe")))
  }

  //Verify update partition_table on standard Partition
  test("Standard-Partition_TC037", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('Europe') where stringfield LIKE 'C%'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Europe")))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("drop table if exists uniqdata")
    sql("drop table if exists partition_table")
  }
}