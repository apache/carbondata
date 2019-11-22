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
  * Test Class for partitionTestCase to verify all scenarios on Partition with Global Sort
  */

class TestPartitionWithGlobalSort extends QueryTest with BeforeAndAfterAll {

  override def beforeAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
  }

  //Loading data into Partitioned table with Global Sort
  test("Partition-Global-Sort_TC001", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('FILEHEADER'='shortfield,intfield,bigintfield,doublefield,stringfield,timestamp,decimalfield,datefield,charfield,floatfield')""")
    checkAnswer(sql(s"""select count(*) from partition_table"""), Seq(Row(11)))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify Exception when Loading data into a Partitioned table with Global Sort and Bad Records Action = FAIL
  test("Partition-Global-Sort_TC002", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    intercept[Exception] {
      sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table options('FILEHEADER'='shortfield,intfield,bigintfield,doublefield,stringfield,timestamp,decimalfield,datefield,charfield,floatfield','BAD_RECORDS_ACTION'='FAIL')""")
    }
  }
  sql(s"""drop table if exists partition_table""")

  //Verify load on Partition with Global Sort after compaction
  test("Partition-Global-Sort_TC003", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""alter table uniqdata compact 'minor'""")
    checkAnswer(sql(s"""select count(*) from uniqdata"""), Seq(Row(84)))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify join operation on Partition with Global Sort
  test("Partition-Global-Sort_TC004", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""drop table if exists uniqdata1""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""CREATE TABLE uniqdata1 (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata1 partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select a.cust_id, b.cust_id from uniqdata a, uniqdata1 b where a.cust_id >= b.cust_id limit 1"""),Seq(Row(1,1)))
    sql(s"""drop table if exists uniqdata""")
    sql(s"""drop table if exists uniqdata1""")
  }

  //Verify exception if partition column is dropped
  test("Partition-Global-Sort_TC005", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    intercept[Exception] {
      sql(s"""alter table uniqdata drop columns(CUST_ID)""")
    }
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify INSERT operation on Partition with Global Sort
  test("Partition-Global-Sort_TC006", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 1)""")
    sql(s"""insert into table uniqdata values ('a', '1','2015-07-01 00:00:00', 5678,7654,23.4, 55.6, 7654, 8765,33,'2015-07-01 00:00:00', 0)""")
    checkAnswer(sql(s"""select count(*) from uniqdata"""), Seq(Row(2)))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify INSERT overwrite operation on Partition with Global Sort
  test("Partition-Global-Sort_TC007", Include) {
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""DROP TABLE IF EXISTS PARTITION_TABLE_load""")
    sql(s"""CREATE TABLE partition_table_load (shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT ) STORED BY 'carbondata'""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table_load options ('BAD_RECORDS_ACTION'='FORCE')""")
    sql(s"""INSERT OVERWRITE TABLE partition_table PARTITION (stringfield = 'Hello') SELECT * FROM partition_table_load au WHERE au.intField = 25""")
    sql(s"""INSERT OVERWRITE TABLE partition_table PARTITION (stringfield = 'Hello') SELECT * FROM partition_table_load au WHERE au.intField = 25""")
    checkAnswer(sql(s"""select floatField,stringField from partition_table limit 1"""),Seq(Row(303.301,"Hello")))
    sql(s"""select * from partition_table""").show(truncate = false)
    sql(s"""drop table if exists PARTITION_TABLE""")
    sql(s"""drop table if exists PARTITION_TABLE_load""")
  }

  //Verify date with > filter condition and Partition with Global Sort
  test("Partition-Global-Sort_TC008", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='4') OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select count(*) from uniqdata where CUST_ID>3"""),Seq(Row(28)))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify date with = filter condition and Partition with Global Sort
  test("Partition-Global-Sort_TC009", Include) {
    sql(s"""drop table if exists uniqdata""")
    sql(s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table uniqdata partition(CUST_ID='4')OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE','QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""")
    checkAnswer(sql(s"""select count(*) from uniqdata where CUST_ID=3"""),Seq(Row(0)))
    sql(s"""drop table if exists uniqdata""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC010", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = "Hello")""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""),Seq(Row("China")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC011", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where charfield = 'c'""").collect
    sql("""update partition_table set (stringfield)=('China123') where stringfield != 'China'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""),Seq(Row("China")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC012", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('China123') where stringfield != 'China'""").collect
    sql("""update partition_table set (stringfield)=('Japan') where stringfield > 'China'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("China")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC013", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('Asia') where stringfield < 'Hello'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Hello")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC014", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('Europe') where stringfield LIKE 'C%'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Europe")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify update partition_table on Partition with Global Sort
  test("Partition-Global-Sort_TC015", Include) {
    sql(s"""drop table if exists partition_table""")
    sql(s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG, doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE, charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table partition_table PARTITION (stringField = 'Hello')""")
    sql("""update partition_table set (stringfield)=('China') where stringfield = 'Hello'""").collect
    sql("""update partition_table set (stringfield)=('Europe') where stringfield LIKE 'C%'""").collect
    checkAnswer(sql(s"""select stringfield from partition_table where charfield='c' limit 1"""), Seq(Row("Europe")))
    sql(s"""drop table if exists partition_table""")
  }

  //Verify rename table with Partition with Global sort
  test("Partition-Global-Sort_TC016", Include) {
    sql(s"""drop table if exists s""")
    sql(s"""drop table if exists partition2""")
    sql(
      s"""Create table s ( s short, f float,l long,d double,ch char(10),vch varchar(10),num int,time timestamp,
         |dt date,name string) partitioned by (dec decimal(30,15)) stored by 'carbondata'
         |TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""".stripMargin)
    sql("show partitions s")
    sql(s"""alter table s rename to partition2""")
    intercept[Exception] {
      sql(s"""select * from s""")
    }
    checkAnswer(sql(s"""select count(*) from partition2"""),Seq(Row(0)))
    sql(s"""drop table if exists s""")
    sql(s"""drop table if exists partition2""")
  }

  //Verify when a new column is added on Partition with Global Sort
  test("Partition-Global-Sort_TC017", Include) {
    sql(s"""drop table if exists a""")
    sql(s"""create table a(b int) partitioned by (c string) stored by 'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""")
    sql(s"""alter table a add columns (d int)""").collect
    sql(s"""insert into a values(13,1996,'feb')""")
    checkAnswer(sql(s"""select d from a"""),Seq(Row(1996)))
    sql(s"""drop table if exists a""")
  }

  //Verify deleting all contents of a particular Partition with Global Sort
  test("Partition-Global-Sort_TC018", Include) {
    sql(s"""drop table if exists partitiontable2""")
    sql(s"""CREATE TABLE partitiontable2(id Int,vin String,phonenumber Long,area String,salary Int,country String) PARTITIONED BY (logdate date)STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('SORT_COLUMNS'='id,vin','sort_scope'='global_sort')""")
    sql(s"""insert into partitiontable2 select 1,'A42158424831',125371341,'Asia',10000,'China','2016/02/12'""")
    sql(s"""insert into partitiontable2 select 1,'A42158424831',125371341,'Asia',10000,'China','2016/02/13'""")
    sql(s"""insert into partitiontable2 partition (logdate='2017-01-01')select 1,'A42158424831',125371341,'Asia',10000,'China'""")
    checkAnswer(sql(s"""show partitions partitiontable2"""),Seq(Row("logdate=2016-02-12"),Row("logdate=2016-02-13"),Row("logdate=2017-01-01")))
    sql("show segments for table partitiontable2")
    sql("delete from partitiontable2 where logdate='2017-01-01'")
    checkAnswer(sql(s"""show partitions partitiontable2"""),Seq(Row("logdate=2016-02-12"),Row("logdate=2016-02-13"),Row("logdate=2017-01-01")))
    sql(s"""drop table if exists partitiontable2""")
  }

  //Verify Dynamic Partition with Global Sort
  test("Partition-Global-Sort_TC019", Include) {
    sql(s"""drop table if exists partitiontable2""")
    sql(s"""CREATE TABLE partitiontable2(id Int,vin String,phonenumber Long,area String) PARTITIONED BY (salary Int,country String) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('SORT_COLUMNS'='id,vin','sort_scope'='global_sort')""")
    sql(s"""insert into partitiontable2 select 1,'A42158424831',125371341,'Asia',10000,'China'""")
    sql(s"""insert into partitiontable2 select 1,'A42158424831',125371341,'Asia',10000,'China'""")
    checkAnswer(sql(s"""select count(*) from partitiontable2"""),Seq(Row(2)))
    checkAnswer(sql(s"""show partitions partitiontable2"""),Seq(Row("salary=10000/country=China")))
    sql(s"""drop table if exists partitiontable2""")
  }

  //Verify Static Partition with Global Sort
  test("Partition-Global-Sort_TC020", Include) {
    sql(s"""drop table if exists partitiontable2""")
    sql(s"""CREATE TABLE partitiontable2(id Int,vin String,phonenumber Long,area String) PARTITIONED BY (salary Int,country String) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('SORT_COLUMNS'='id,vin','sort_scope'='global_sort')""")
    sql(s"""insert into partitiontable2 partition (salary=10101,country="India") select 1,'A42158424831',125371341,'Asia'""")
    checkAnswer(sql(s"""show partitions partitiontable2"""),Seq(Row("salary=10101/country=India")))
    sql(s"""drop table if exists partitiontable2""")
  }

  //Verify exception while trying to rename Static Partition with Global Sort
  test("Partition-Global-Sort_TC021", Include) {
    sql(s"""drop table if exists partitiontable2""")
    sql(s"""CREATE TABLE partitiontable2(id Int,vin String,phonenumber Long,area String) PARTITIONED BY (salary Int,country String) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('SORT_COLUMNS'='id,vin','sort_scope'='global_sort')""")
    sql(s"""insert into partitiontable2 select 1,'A42158424831',125371341,'Asia',10000,'China'""")
    checkAnswer(sql(s"""show partitions partitiontable2"""),Seq(Row("salary=10000/country=China")))
    intercept[Exception]
      {
        sql(s"""alter table partitiontable2 partition(salary=10000,countr y='China') rename to partition(salary=10101,country='India')""")
      }
    sql(s"""drop table if exists partitiontable2""")
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("drop table if exists uniqdata")
    sql("drop table if exists partition_table")
  }
}