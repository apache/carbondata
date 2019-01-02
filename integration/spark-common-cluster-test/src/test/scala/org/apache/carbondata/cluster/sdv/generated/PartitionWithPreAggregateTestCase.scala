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
  * Test Class for Support of Partition with PreAggregate table
  */
class PartitionWithPreAggregateTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
  }

  //Loading data into partitioned table with SORT_SCOPE=LOCAL_SORT
  test("Partition-With-PreAggregate_TC001", Include) {
    sql("drop table if exists partition_table")
    sql(
      s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG,
         |doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE,
         |charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY
         |'carbondata' TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT')""".stripMargin)
    sql(
      s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table
         |partition_table""".stripMargin)
    sql(
      "create datamap ag1 on table partition_table using 'preaggregate' as select shortField, sum" +
      "(intField) from partition_table group by shortField")
    checkAnswer(sql(
      s"""select decimalfield from partition_table where charfield='e' and
         |floatfield=307.301 group by decimalfield limit 1""".stripMargin),
      Seq(Row(159.10)))
  }

  //Loading data into partitioned table with SORT_SCOPE=GLOBAL_SORT
  test("Partition-With-PreAggregate_TC002", Include) {
    sql("drop table if exists partition_table")
    sql(
      s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG,
         |doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE,
         |charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY
         |'carbondata' TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT')""".stripMargin)
    sql(
      s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table
         |partition_table""".stripMargin)
    sql(
      "create datamap ag1 on table partition_table using 'preaggregate' as select shortField, sum" +
      "(intField) from partition_table group by shortField")
    checkAnswer(sql(
      s"""select decimalfield from partition_table where charfield='e' and
         |floatfield=307.301 group by decimalfield limit 1""".stripMargin),
      Seq(Row(159.10)))
  }

  //Loading data into partitioned table with SORT_SCOPE=BATCH_SORT
  test("Partition-With-PreAggregate_TC003", Include) {
    sql("drop table if exists partition_table")
    sql(
      s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG,
         |doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE,
         |charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY
         |'carbondata' TBLPROPERTIES('SORT_SCOPE'='BATCH_SORT')""".stripMargin)
    sql(
      s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table
         |partition_table""".stripMargin)
    sql(
      "create datamap ag1 on table partition_table using 'preaggregate' as select shortField, sum" +
      "(intField) from partition_table group by shortField")
    checkAnswer(sql(
      s"""select decimalfield from partition_table where charfield='e' and
         |floatfield=307.301 group by decimalfield limit 1""".stripMargin),
      Seq(Row(159.10)))
  }

  //Loading data into partitioned table with SORT_SCOPE=NO_SORT
  test("Partition-With-PreAggregate_TC004", Include) {
    sql("drop table if exists partition_table")
    sql(
      s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG,
         |doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE,
         |charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY
         |'carbondata' TBLPROPERTIES('SORT_SCOPE'='NO_SORT')""".stripMargin)
    sql(
      s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table
         |partition_table""".stripMargin)
    sql(
      "create datamap ag1 on table partition_table using 'preaggregate' as select shortField, sum" +
      "(intField) from partition_table group by shortField")
    checkAnswer(sql(
      s"""select decimalfield from partition_table where charfield='e' and
         |floatfield=307.301 group by decimalfield limit 1""".stripMargin),
      Seq(Row(159.10)))
  }

  //Verify Aggregation query on Loaded Partition table with Pre-Aggregate table
  test("Partition-With-PreAggregate_TC005", Include) {
    sql("drop table if exists partition_table")
    sql(
      s"""CREATE TABLE partition_table(shortField SHORT, intField INT, bigintField LONG,
         |doubleField DOUBLE, timestamp TIMESTAMP, decimalField DECIMAL(18,2),dateField DATE,
         |charField CHAR(5), floatField FLOAT ) PARTITIONED BY (stringField STRING) STORED BY
         |'carbondata'""".stripMargin)
    sql(
      "create datamap ag1 on table partition_table using 'preaggregate' as select shortField, sum" +
      "(intField) from partition_table group by shortField")
    sql(
      s"""load data inpath '$resourcesPath/Data/partition/list_partition_table.csv' into table
         |partition_table""".stripMargin)
    assert(sql("explain select shortField, sum(intField) from partition_table group by shortField")
      .collect().head.get(0).toString.contains("partition_table_ag1"))
  }

  //Verify load with Pre_Aggregate table on Partition table after compaction
  test("Partition-With-PreAggregate_TC006", Include) {
    sql("drop table if exists uniqdata")
    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY
         |'carbondata'""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql("create datamap ag1 on table uniqdata using 'preaggregate' as select cust_id, " +
        "sum(Double_COLUMN1) from uniqdata group by cust_id")
    sql(s"""alter table uniqdata compact 'minor'""")
    assert(sql("show segments for table uniqdata").collect().tail(2).get(0).toString.equals("0.1"))
  }

  //Verify load with Pre_Aggregate table on Partition table after two-level compaction
  test("Partition-With-PreAggregate_TC007", Include) {
    sql("drop table if exists uniqdata")
    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY
         |'carbondata'""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql("create datamap ag1 on table uniqdata using 'preaggregate' as select cust_id, " +
        "sum(Double_COLUMN1) from uniqdata group by cust_id")
    sql(s"""alter table uniqdata compact 'minor'""")
    sql(s"""alter table uniqdata compact 'major'""")
    assert(sql("show segments for table uniqdata").collect().tail(7).get(0).toString.equals("0.2"))
    }

  //Verify join operation on Partitonwith Pre-Aggregate table
    test("Partition-With-PreAggregate_TC008", Include) {
      sql("drop table if exists uniqdata")
      sql(s"""drop table if exists uniqdata1""")
    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'"""
        .stripMargin)
    sql(
      s"""CREATE TABLE uniqdata1 (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'"""
        .stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata1 partition(CUST_ID='1') OPTIONS('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    sql("create datamap ag1 on table uniqdata using 'preaggregate' as select cust_id, " +
        "sum(Double_COLUMN1) from uniqdata group by cust_id")
    sql("create datamap ag1 on table uniqdata1 using 'preaggregate' as select cust_id, " +
        "sum(Double_COLUMN1) from uniqdata1 group by cust_id")
    checkAnswer(sql(
      s""" select a.cust_id, b.cust_id from uniqdata a, uniqdata1 b where
         |a.cust_id >= b.cust_id limit 1""".stripMargin),Seq(Row(1,1)))
    sql(s"""drop table if exists uniqdata1""")
  }

  //Verify date with > filter condition on Partitonwith Pre-Aggregate table
  test("Partition-With-PreAggregate_TC009", Include) {
    sql("drop table if exists uniqdata")
    sql(
      s"""CREATE TABLE uniqdata (CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp,
         |BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
         |DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
         |INTEGER_COLUMN1 int, DOJ timestamp) PARTITIONED BY (CUST_ID int) STORED BY 'carbondata'"""
        .stripMargin)
    sql("create datamap ag1 on table uniqdata using 'preaggregate' as select cust_id, " +
        "sum(Double_COLUMN1) from uniqdata group by cust_id")
    sql(
      s"""LOAD DATA INPATH  '$resourcesPath/Data/partition/2000_UniqData_partition.csv' into table
         | uniqdata partition(CUST_ID='4') OPTIONS('DELIMITER'=',' , 'BAD_RECORDS_ACTION'='FORCE',
         | 'QUOTECHAR'='"','FILEHEADER'='CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DOJ,CUST_ID')""".stripMargin)
    checkAnswer(sql(
      s"""select cust_name from uniqdata where active_emui_version =
         |'ACTIVE_EMUI_VERSION_00000'""".stripMargin),Seq(Row("CUST_NAME_00000")))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("drop table if exists uniqdata")
    sql("drop table if exists partition_table")
    sql(s"""drop table if exists uniqdata1""")

  }
}