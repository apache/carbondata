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

package org.apache.carbondata.integration.spark.testsuite.primitiveTypes

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestAdaptiveEncodingForPrimitiveTypes extends QueryTest with BeforeAndAfterAll {

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath

  private val vectorReader = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER)

  private val unsafeColumnPage = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE)

  private val unsafeQueryExecution = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION)

  private val unsafeSort = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT)

  private val compactionThreshold = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD)

  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")

  override def beforeAll: Unit = {
    dropTables
    sql(
      "CREATE TABLE uniqdata_Compare (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata")

    sql(s"LOAD DATA INPATH '${resourcesPath + "/data_with_all_types.csv"}' into table" +
        " uniqdata_Compare OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")

    sql(
      "CREATE TABLE uniqdata_Compaction (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata")

    sql(s"LOAD DATA INPATH '${resourcesPath + "/data_with_all_types.csv"}' into table" +
        " uniqdata_Compaction OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${resourcesPath + "/data_with_all_types.csv"}' into table" +
        " uniqdata_Compaction OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")

    sql("alter table uniqdata_Compaction compact 'minor'")

    sql("create table negativeTable_Compare (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable_Compare options('FILEHEADER'='intColumn,stringColumn,shortColumn')")

    sql("create table negativeTable_Compaction (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable_Compaction options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable_Compaction options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    sql("alter table negativeTable_Compaction compact 'minor'")
  }

  test("test adaptive encoding on all possible data types by dictionary exclude") {
    sql("drop table if exists uniqdata")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))
  }

  test("test adaptive encoding on all possible data types by SORT_COLUMNS") {
    sql("drop table if exists uniqdata")
    sql(
      "CREATE TABLE uniqdata (CUST_ID short,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))
  }

  test("test filter queries on the adaptive encoded column") {
    sql("drop table if exists negativeTable")
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))
    checkAnswer(sql("select * from negativeTable where intColumn<0"), sql("select * from negativeTable_Compare where intColumn<0"))
    checkAnswer(sql("select * from negativeTable where intColumn<=0"), sql("select * from negativeTable_Compare where intColumn<=0"))
    checkAnswer(sql("select * from negativeTable where intColumn>0"), sql("select * from negativeTable_Compare where intColumn>0"))
    checkAnswer(sql("select * from negativeTable where intColumn>=0"), sql("select * from negativeTable_Compare where intColumn>=0"))
    checkAnswer(sql("select * from negativeTable where intColumn >= -20000"), sql("select * from negativeTable_Compare where intColumn >= -20000"))
    checkAnswer(sql("select * from negativeTable where intColumn <= 10000"), sql("select * from negativeTable_Compare where intColumn <= 10000"))
    checkAnswer(sql("select * from negativeTable where intColumn between -20000 and 10000"), sql("select * from negativeTable_Compare where intColumn between -20000 and 10000"))
    checkAnswer(sql("select * from negativeTable where intColumn is null"), sql("select * from negativeTable_Compare where intColumn is null"))
    checkAnswer(sql("select * from negativeTable where intColumn is not null"), sql("select * from negativeTable_Compare where intColumn is not null"))
    checkAnswer(sql("select * from negativeTable where shortColumn<0"), sql("select * from negativeTable_Compare where shortColumn<0"))
    checkAnswer(sql("select * from negativeTable where shortColumn<=0"), sql("select * from negativeTable_Compare where shortColumn<=0"))
    checkAnswer(sql("select * from negativeTable where shortColumn>0"), sql("select * from negativeTable_Compare where shortColumn>0"))
    checkAnswer(sql("select * from negativeTable where shortColumn>=0"), sql("select * from negativeTable_Compare where shortColumn>=0"))
    checkAnswer(sql("select * from negativeTable where shortColumn between -200 and 200"), sql("select * from negativeTable_Compare where shortColumn between -200 and 200"))
    checkAnswer(sql("select * from negativeTable where shortColumn >= -200"), sql("select * from negativeTable_Compare where shortColumn >= -200"))
    checkAnswer(sql("select * from negativeTable where shortColumn <= 100"), sql("select * from negativeTable_Compare where shortColumn <= 100"))
    checkAnswer(sql("select * from negativeTable where shortColumn is null"), sql("select * from negativeTable_Compare where shortColumn is null"))
    checkAnswer(sql("select * from negativeTable where shortColumn is not null"), sql("select * from negativeTable_Compare where shortColumn is not null"))
    sql("drop table if exists negativeTable")
  }

  test("test adaptive encoding on all possible data types by sort_columns - vector enable") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))
    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql("alter table uniqdata compact 'minor'")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compaction"))

    // negative data compaction test
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    sql("alter table negativeTable compact 'minor'")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compaction"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, vectorReader)
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
  }

  test("test adaptive encoding on all possible data types by sort_columns - vector disable") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))
    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    sql("alter table uniqdata compact 'minor'")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compaction"))

    // negative data compaction test
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    sql("alter table negativeTable compact 'minor'")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compaction"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, vectorReader)
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
  }

  test("test adaptive encoding on all possible data types by sort_columns - enable unsafe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "true")
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))

    // negative data compaction test
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
  }

  test("test adaptive encoding on all possible data types by sort_columns - disable unsafe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "false")
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select * from uniqdata"), sql("select * from uniqdata_Compare"))

    // negative data compaction test
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
  }

  test("test adaptive encoding on partition table") {
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
    sql(
      "CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) PARTITIONED BY(DOJ timestamp) STORED AS carbondata TBLPROPERTIES" +
      "('SORT_COLUMNS'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
      "BIGINT_COLUMN2,INTEGER_COLUMN1')")

    sql(s"LOAD DATA INPATH '${ resourcesPath + "/data_with_all_types.csv" }' into table" +
        " uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"'," +
        "'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION," +
        "DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2," +
        "Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')")
    checkAnswer(sql("select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,INTEGER_COLUMN1 from uniqdata"), sql("select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,INTEGER_COLUMN1 from uniqdata_Compare"))

    // negative data compaction test
    sql("create table negativeTable (intColumn int,stringColumn string) PARTITIONED BY(shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select intColumn,stringColumn,shortColumn from negativeTable"), sql("select intColumn,stringColumn,shortColumn from negativeTable_Compare"))
    sql("drop table if exists uniqdata")
    sql("drop table if exists negativeTable")
  }

  test("test IUD on the adptive encoded column") {
    sql("drop table if exists negativeTable")
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))
    sql("insert into negativeTable select 0,null,-10")
    sql("insert into negativeTable select null,'inserted',20")
    checkAnswer(sql("select * from negativeTable where intColumn=0"),
      Seq(Row(0, "ddd", 0), Row(0, null, -10)))
    checkAnswer(sql("select * from negativeTable where intColumn is null"),
      Seq(Row(null, "null", null), Row(null, "inserted", 20)))
    sql("update negativeTable set (intColumn) = (5) where intColumn=0").show()
    checkAnswer(sql("select * from negativeTable where intColumn=5"),
      Seq(Row(5, "ddd", 0), Row(5, null, -10)))
    sql("delete from negativeTable where intColumn=5").show()
    checkAnswer(sql("select * from negativeTable"),
      Seq(Row(null, "null", null),
        Row(null, "inserted", 20),
        Row(-30000, "aaa", -300),
        Row(-20000, "bbb", -200),
        Row(-10000, "ccc", -100),
        Row(10000, "eee", 100),
        Row(70000, "ggg", 700)))
    sql("drop table if exists negativeTable")
  }

  test("test filter queries on adaptive encoded column with complex column in the schema") {
    sql("drop table if exists complexTable")
    sql("CREATE TABLE complexTable( id LONG,name STRING,salary FLOAT,file struct<school:array<string>, age:int>) STORED AS carbondata TBLPROPERTIES('sort_columns'='id,name')")
    sql(s"LOAD DATA INPATH '$rootPath/examples/spark/src/main/resources/streamSample.csv' INTO TABLE complexTable OPTIONS('HEADER'='TRUE')")
    checkAnswer(sql("select id,name, salary from complexTable"),
      Seq(Row(100000001, "batch_1", 0.1),
        Row(100000002, "batch_2", 0.2),
        Row(100000003, "batch_3", 0.3),
        Row(100000004, "batch_4", 0.4),
        Row(100000005, "batch_5", 0.5)))
    sql("drop table if exists complexTable")
  }

  test("test bloom datamap on the adaptive encoded column") {
    sql("drop table if exists negativeTable")
    sql("create table negativeTable (intColumn int,stringColumn string,shortColumn short) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='intColumn,shortColumn')")
    sql(s"load data inpath '${resourcesPath + "/dataWithNegativeValues.csv"}' into table negativeTable options('FILEHEADER'='intColumn,stringColumn,shortColumn')")
    checkAnswer(sql("select * from negativeTable"), sql("select * from negativeTable_Compare"))
    // create bloom in the intColumn and shortColumn
    sql("CREATE DATAMAP negativeTable_bloom ON TABLE negativeTable USING 'bloomfilter' DMProperties('INDEX_COLUMNS'='intColumn,shortColumn', 'BLOOM_SIZE'='640000')")

    checkAnswer(sql("select * from negativeTable where intColumn<0"), sql("select * from negativeTable_Compare where intColumn<0"))
    checkAnswer(sql("select * from negativeTable where intColumn<=0"), sql("select * from negativeTable_Compare where intColumn<=0"))
    checkAnswer(sql("select * from negativeTable where intColumn>0"), sql("select * from negativeTable_Compare where intColumn>0"))
    checkAnswer(sql("select * from negativeTable where intColumn>=0"), sql("select * from negativeTable_Compare where intColumn>=0"))
    checkAnswer(sql("select * from negativeTable where intColumn >= -20000"), sql("select * from negativeTable_Compare where intColumn >= -20000"))
    checkAnswer(sql("select * from negativeTable where intColumn <= 10000"), sql("select * from negativeTable_Compare where intColumn <= 10000"))
    checkAnswer(sql("select * from negativeTable where intColumn between -20000 and 10000"), sql("select * from negativeTable_Compare where intColumn between -20000 and 10000"))
    checkAnswer(sql("select * from negativeTable where intColumn is null"), sql("select * from negativeTable_Compare where intColumn is null"))
    checkAnswer(sql("select * from negativeTable where intColumn is not null"), sql("select * from negativeTable_Compare where intColumn is not null"))
    checkAnswer(sql("select * from negativeTable where shortColumn<0"), sql("select * from negativeTable_Compare where shortColumn<0"))
    checkAnswer(sql("select * from negativeTable where shortColumn<=0"), sql("select * from negativeTable_Compare where shortColumn<=0"))
    checkAnswer(sql("select * from negativeTable where shortColumn>0"), sql("select * from negativeTable_Compare where shortColumn>0"))
    checkAnswer(sql("select * from negativeTable where shortColumn>=0"), sql("select * from negativeTable_Compare where shortColumn>=0"))
    checkAnswer(sql("select * from negativeTable where shortColumn between -200 and 200"), sql("select * from negativeTable_Compare where shortColumn between -200 and 200"))
    checkAnswer(sql("select * from negativeTable where shortColumn >= -200"), sql("select * from negativeTable_Compare where shortColumn >= -200"))
    checkAnswer(sql("select * from negativeTable where shortColumn <= 100"), sql("select * from negativeTable_Compare where shortColumn <= 100"))
    checkAnswer(sql("select * from negativeTable where shortColumn is null"), sql("select * from negativeTable_Compare where shortColumn is null"))
    checkAnswer(sql("select * from negativeTable where shortColumn is not null"), sql("select * from negativeTable_Compare where shortColumn is not null"))
    sql("drop table if exists negativeTable")
  }

  override def afterAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    dropTables
  }


  private def dropTables = {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata_Compare")
    sql("drop table if exists uniqdata_Compaction")
    sql("drop table if exists negativeTable")
    sql("drop table if exists negativeTable_Compare")
    sql("drop table if exists negativeTable_Compaction")
    sql("drop table if exists complexTable")
  }
}
