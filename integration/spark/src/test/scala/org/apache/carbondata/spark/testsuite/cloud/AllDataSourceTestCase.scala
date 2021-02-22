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

package org.apache.carbondata.spark.testsuite.cloud

import java.io.File

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.DatabaseLocationProvider
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for all data source
 *
 */
class AllDataSourceTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    // TODO these properties only work when running in idea.
    CarbonProperties.getInstance()
        .addProperty(
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE,
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DATABASE
        )
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DATABASE_LOCATION_PROVIDER,
      "org.apache.carbondata.spark.testsuite.cloud.TestProvider")
    dropAll
    sql("create database alldatasource")
    sql("use alldatasource")
    sql(
      s"""
         | create table origin_csv(col1 int, col2 string, col3 date)
         | using csv
         | options('dateFormat'='yyyy-MM-dd', 'timestampFormat'='yyyy-MM-dd HH:mm:ss')
         | """.stripMargin)
    sql("insert into origin_csv select 1, '3aa', to_date('2019-11-11')")
    sql("insert into origin_csv select 2, '2bb', to_date('2019-11-12')")
    sql("insert into origin_csv select 3, '1cc', to_date('2019-11-13')")
  }

  def dropAll {
    dropTableByName("ds_carbon")
    dropTableByName("ds_carbondata")
    dropTableByName("hive_carbon")
    dropTableByName("hive_carbondata")

    sql("drop table if exists tbl_truncate")
    sql("drop table if exists origin_csv")
    sql("drop table if exists tbl_float1")
    sql("drop table if exists tbl_float2")
    sql("drop table if exists ds_options")
    sql("drop table if exists hive_options")
    sql("drop table if exists tbl_update")
    sql("drop table if exists tbl_oldName")
    sql("drop table if exists tbl_newName")
    sql("drop table if exists tbl_insert_p_nosort")
    sql("drop table if exists tbl_insert_overwrite")
    sql("drop table if exists tbl_insert_overwrite_p")
    sql("drop table if exists tbl_metrics_ns")
    sql("drop table if exists tbl_metrics_ls")
    sql("drop table if exists tbl_metrics_gs")
    sql("drop table if exists tbl_metrics_p_ns")
    sql("drop table if exists tbl_metrics_p_ls")
    sql("drop table if exists tbl_metrics_p_gs")
    sql("drop table if exists ds_sct")
    sql("drop table if exists tbl_complex")
    sql("drop table if exists tbl_complex_carbondata")
    sql("drop table if exists tbl_complex_p")
    sql("drop table if exists tbl_complex_p_carbondata")
    sql("drop database if exists alldatasource cascade")
  }

  def dropTableByName(tableName: String): Unit = {
    sql(s"drop table if exists $tableName")
    sql(s"drop table if exists ${tableName}_p")
    sql(s"drop table if exists ${tableName}_ctas")
    sql(s"drop table if exists ${tableName}_e")
    sql(s"drop table if exists ${tableName}_s")
  }

  override def afterAll: Unit = {
    try {
      dropAll
      sql("use default")
    } finally {
      CarbonProperties.getInstance()
        .addProperty(
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE,
          CarbonCommonConstants.CARBON_INDEX_SCHEMA_STORAGE_DEFAULT
        )
      CarbonProperties.getInstance()
        .removeProperty(CarbonCommonConstants.DATABASE_LOCATION_PROVIDER)
    }
  }

  test("test carbon") {
    verifyDataSourceTable("carbon", "ds_carbon")
    verifyHiveTable("carbon", "hive_carbon")
  }

  test("test carbondata") {
    verifyDataSourceTable("carbondata", "ds_carbondata")
    verifyHiveTable("carbondata", "hive_carbondata")
  }

  test("test partition table") {
    createDataSourcePartitionTable("carbondata", "ds_carbondata_p")
    createHivePartitionTable("carbondata", "hive_carbondata_p")
  }

  test("test table properties of datasource table") {
    val tableName = "ds_options"
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | options("sort_sCope"="global_Sort", "sort_Columns"="coL2", 'global_Sort_partitions'='1')
         | """.stripMargin)

    checkExistence(sql(s"describe formatted ${ tableName }"), true, "global_sort")
    sql(s"insert into table ${ tableName } select * from origin_csv")
    checkAnswer(
      sql(s"select * from ${ tableName }"),
      Seq(
        Row(3, "1cc", java.sql.Date.valueOf("2019-11-13")),
        Row(2, "2bb", java.sql.Date.valueOf("2019-11-12")),
        Row(1, "3aa", java.sql.Date.valueOf("2019-11-11"))
      )
    )
    checkExistence(sql(s"show create table ${ tableName }"), true, "sort_columns")
    sql(s"alter table ${ tableName } " +
        "set tblproperties('sort_Columns'='col2,col1', 'LOAD_MIN_SIZE_INMB'='256')")
    checkExistence(sql(s"show create table ${ tableName }"), true, "load_min_size_inmb")
    sql(s"alter table ${ tableName } unset tblproperties('LOAD_MIN_SIZE_INMB')")
    checkExistence(sql(s"show create table ${ tableName }"), false, "load_min_size_inmb")
    val rows = sql(s"show create table ${ tableName }").collect()
    // drop table
    sql(s"drop table ${ tableName }")
    // create again
    sql(rows(0).getString(0))
    checkExistence(sql(s"describe formatted ${ tableName }"), true, "global_sort")
    sql(s"insert into table ${ tableName } select * from origin_csv")
    checkAnswer(
      sql(s"select * from ${ tableName }"),
      Seq(
        Row(3, "1cc", java.sql.Date.valueOf("2019-11-13")),
        Row(2, "2bb", java.sql.Date.valueOf("2019-11-12")),
        Row(1, "3aa", java.sql.Date.valueOf("2019-11-11"))
      )
    )

    val table = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession)
    assert(table.getBlockletSizeInMB == 64)
    assert(table.getBlockSizeInMB == 1024)
  }

  test("test table properties of hive table") {
    val tableName = "hive_options"
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | stored as carbondata
         | tblproperties(
         | "sort_sCope"="global_Sort", "sort_Columns"="coL2", 'global_Sort_partitions'='1')
         | """.stripMargin)

    checkExistence(sql(s"describe formatted ${ tableName }"), true, "global_sort")
    sql(s"insert into table ${ tableName } select * from origin_csv")
    checkAnswer(
      sql(s"select * from ${ tableName }"),
      Seq(
        Row(3, "1cc", java.sql.Date.valueOf("2019-11-13")),
        Row(2, "2bb", java.sql.Date.valueOf("2019-11-12")),
        Row(1, "3aa", java.sql.Date.valueOf("2019-11-11"))
      )
    )
    checkExistence(sql(s"show create table ${ tableName }"), true, "sort_columns")
    sql(s"alter table ${ tableName } " +
        s"set tblproperties('sort_Columns'='col2,col1', 'LOAD_MIN_SIZE_INMB'='256')")
    checkExistence(sql(s"show create table ${ tableName }"), true, "load_min_size_inmb")
    sql(s"alter table ${ tableName } unset tblproperties('LOAD_MIN_SIZE_INMB')")
    checkExistence(sql(s"show create table ${ tableName }"), false, "load_min_size_inmb")
    val rows = sql(s"show create table ${ tableName }").collect()
    // drop table
    sql(s"drop table ${ tableName }")
    // create again
    sql(rows(0).getString(0))
    checkExistence(sql(s"describe formatted ${ tableName }"), true, "global_sort")
    sql(s"insert into table ${ tableName } select * from origin_csv")
    checkAnswer(
      sql(s"select * from ${ tableName }"),
      Seq(
        Row(3, "1cc", java.sql.Date.valueOf("2019-11-13")),
        Row(2, "2bb", java.sql.Date.valueOf("2019-11-12")),
        Row(1, "3aa", java.sql.Date.valueOf("2019-11-11"))
      )
    )
  }

  ignore("test show create table") {
    val tableName = "ds_sct"
    sql(s"drop table if exists ${ tableName }")
    sql(
      s"""
         | create table ${ tableName }
         | using carbondata
         | as select * from origin_csv
         | """.stripMargin)
    sql(s"show create table $tableName").collect()
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row(
        """CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
          |USING carbondata
          |OPTIONS (
          |  `bad_record_path` '',
          |  `local_dictionary_enable` 'true',
          |  `sort_columns` '',
          |  `comment` ''
          |)
          |""".stripMargin)))

    sql(s"drop table if exists ${ tableName }")
    sql(
      s"""
         | create table ${ tableName }
         | using carbondata
         | options("global_sort_partitions"="1")
         | as select * from origin_csv
         | """.stripMargin)
    sql(s"show create table $tableName").collect()
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row(
        """CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
          |USING carbondata
          |OPTIONS (
          |  `bad_record_path` '',
          |  `local_dictionary_enable` 'true',
          |  `sort_columns` '',
          |  `comment` '',
          |  `global_sort_partitions` '1'
          |)
          |""".stripMargin)))

    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | """.stripMargin)
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row("""CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
                |USING carbondata
                |""".stripMargin)))

    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | options("global_sort_partitions"="1")
         | """.stripMargin)
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row("""CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
                |USING carbondata
                |OPTIONS (
                |  `global_sort_partitions` '1'
                |)
                |""".stripMargin)))

    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | options("sort_scope"="global_sort", "sort_columns"="col2")
         | """.stripMargin)
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row("""CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
                |USING carbondata
                |OPTIONS (
                |  `sort_scope` 'global_sort',
                |  `sort_columns` 'col2'
                |)
                |""".stripMargin)))

    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | partitioned by (col2)
         | """.stripMargin)
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row("""CREATE TABLE `ds_sct` (`col1` INT, `col3` DATE, `col2` STRING)
                |USING carbondata
                |PARTITIONED BY (col2)
                |""".stripMargin)))

    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |create table ${ tableName } (
         | col1 int, col2 string, col3 date
         |)
         | using carbondata
         | options("sort_scope"="global_sort", "sort_columns"="col2")
         | partitioned by (col3)
         | """.stripMargin)
    sql(s"show create table $tableName").collect()
    checkAnswer(sql(s"show create table $tableName"),
      Seq(Row("""CREATE TABLE `ds_sct` (`col1` INT, `col2` STRING, `col3` DATE)
                |USING carbondata
                |OPTIONS (
                |  `sort_scope` 'global_sort',
                |  `sort_columns` 'col2'
                |)
                |PARTITIONED BY (col3)
                |""".stripMargin)))
  }

  test("test add column with comment") {
    val tableName = "tbl_comment"
    sql(
      s"""
         | create table $tableName(
         | id int comment 'id column',
         | age int
         | )
         | using carbondata
         | partitioned by (age)
         | Comment 'test table'
         | """.stripMargin)
    checkExistence(sql(s"desc formatted $tableName"), true, "test table")
    sql(s"alter table $tableName set tblProperties('comment'='new test table')")
    sql(s"alter table $tableName add columns (name string comment 'test column')")
    checkExistence(sql(s"desc $tableName"), true, "test column")
    checkExistence(sql(s"desc formatted $tableName"), true, "new test table")
    checkExistence(sql(s"desc formatted $tableName"), true, "test column")
  }

  test("test external table") {
    verifyExternalDataSourceTable("carbondata", "ds_carbondata")
    verifyExternalHiveTable("carbondata", "hive_carbondata")
  }

  test("test truncate table") {
    val tableName = "tbl_truncate"
    sql(s"create table ${tableName} using carbondata as select * from origin_csv")
    checkAnswer(sql(s"select count(*) from ${tableName}"), Seq(Row(3)))
    sql(s"truncate table ${tableName}")
    checkAnswer(sql(s"select count(*) from ${tableName}"), Seq(Row(0)))
  }

  test("test float") {
    val tableName = "tbl_float"
    sql(s"create table ${tableName}1 (col1 string, col2 float, col3 char(10), " +
        "col4 varchar(20), col5 decimal(10,2)) using carbondata")
    sql(s"describe formatted ${tableName}1").collect()
    sql(s"insert into table ${tableName}1 select 'abc', 1.0, 'a3','b3', 12.34")
    checkAnswer(sql(s"select * from ${tableName}1"), Seq(Row("abc", 1.0f, "a3", "b3", 12.34)))
    sql(s"create table ${tableName}2 (col1 string, col2 float, col3 char(10), " +
        "col4 varchar(20), col5 decimal(10,2)) stored as carbondata")
    sql(s"describe formatted ${tableName}2").collect()
    sql(s"insert into table ${tableName}2 select 'abc', 1.0, 'a3','b3', 12.34")
    checkAnswer(sql(s"select * from ${tableName}2"), Seq(Row("abc", 1.0f, "a3", "b3", 12.34)))
  }

  test("test explain") {
    val tableName = "tbl_update"
    sql(s"create table ${tableName} using carbondata as select * from origin_csv")
    checkExistence(
      sql(s"explain select * from ${tableName} where col1 = 1"),
      true,
      "Scan")
    checkExistence(
      sql(s"explain update ${tableName} set (col2) = ('4aa') where col1 = 1"),
      true,
      "OneRowRelation")
    checkExistence(
      sql(s"explain delete from ${tableName}"),
      true,
      "OneRowRelation")
  }

  test("test rename table") {
    val oldName = "tbl_oldName"
    val newName = "tbl_newName"
    sql(s"create table ${oldName}(id int,name string) using carbondata")
    sql(s"insert into table ${oldName} select 2,'aa'")
    sql(s"ALTER TABLE ${oldName} RENAME TO ${newName}")
    sql(s"create table ${oldName}(id int,name string) using carbondata")
    checkAnswer(
      sql(s"select count(*) from ${newName}"),
      Seq(Row(1))
    )
    checkAnswer(
      sql(s"select * from ${newName}"),
      Seq(Row(2, "aa"))
    )
    checkAnswer(
      sql(s"select count(*) from ${oldName}"),
      Seq(Row(0))
    )
  }

  ignore("output size: insert into partition table") {
    verifyMetrics("tbl_metrics_ns", "no_sort")
    verifyMetrics("tbl_metrics_ls", "local_sort")
    verifyMetrics("tbl_metrics_gs", "global_sort")
    verifyMetricsForPartitionTable("tbl_metrics_p_ns", "no_sort")
    verifyMetricsForPartitionTable("tbl_metrics_p_ls", "local_sort")
    verifyMetricsForPartitionTable("tbl_metrics_p_gs", "global_sort")
  }

  def verifyMetrics(tableName: String, sort_scope: String): Unit = {
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | create table $tableName (
         | col1 int,
         | col2 string,
         | col3 date,
         | col4 timestamp,
         | col5 float
         | )
         | using carbondata
         | options('dateFormat'='yyyy-MM-dd', 'timestampFormat'='yyyy-MM-dd HH:mm:ss',
         | 'sort_scope'='${sort_scope}', 'sort_columns'='col2')
       """.stripMargin)
    sql(
      s"""
         | insert into $tableName (
         |  select col1, col2, col3, to_timestamp('2019-02-02 13:01:01'), 1.2 from origin_csv
         |  union all
         |  select 123,'abc', to_date('2019-01-01'), to_timestamp('2019-02-02 13:01:01'), 1.2)
         |  """.stripMargin
    )
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(4)))
  }

  def verifyMetricsForPartitionTable(tableName: String, sort_scope: String): Unit = {
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | create table $tableName (
         | col1 int,
         | col2 string,
         | col3 date,
         | col4 timestamp,
         | col5 float
         | )
         | using carbondata
         | options('dateFormat'='yyyy-MM-dd', 'timestampFormat'='yyyy-MM-dd HH:mm:ss',
         | 'sort_scope'='${sort_scope}', 'sort_columns'='col2')
         | partitioned by(col3, col4)
       """.stripMargin)
    sql(
      s"""
         | insert into $tableName (
         |  select col1, col2, 1.2, col3, to_timestamp('2019-02-02 13:01:01') from origin_csv
         |  union all
         |  select 123,'abc', 1.2, to_date('2019-01-01'), to_timestamp('2019-02-02 13:01:01'))
         |  """.stripMargin
    )
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(4)))
  }

  test("insert overwrite table") {
    val tableName = "tbl_insert_overwrite"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | create table $tableName (
         | col1 int,
         | col2 string
         | )
         | using carbondata
       """.stripMargin)
    sql(
      s"""
         | insert into $tableName
         |  select 123,'abc'
         |  """.stripMargin
    ).collect()
    sql(
      s"""
         | insert overwrite table $tableName
         |  select 321,'cba'
         |  """.stripMargin
    ).collect()

    checkAnswer(
      sql(s"select * from $tableName"),
      Seq(Row(321, "cba"))
    )
  }

  test("insert overwrite partition table") {
    val tableName = "tbl_insert_overwrite_p"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | create table $tableName (
         | col1 int,
         | col2 string
         | )
         | using carbondata
         | partitioned by (col2)
       """.stripMargin)
    sql(
      s"""
         | insert into $tableName
         |  select 123,'abc'
         |  """.stripMargin
    ).collect()
    sql(
      s"""
         | insert into $tableName (
         |  select 234,'abc'
         |  union all
         |  select 789, 'edf'
         |  )""".stripMargin
    ).collect()
    sql(
      s"""
         | insert into $tableName
         |  select 345,'cba'
         |  """.stripMargin
    ).collect()
    sql(
      s"""
         | insert overwrite table $tableName
         |  select 321,'abc'
         |  """.stripMargin
    ).collect()

    sql(s"clean files for table $tableName").collect()

    checkAnswer(
      sql(s"select * from $tableName order by col1"),
      Seq(Row(321, "abc"), Row(345, "cba"), Row(789, "edf"))
    )
  }

  test("test complex datatype") {
    val tableName1 = "tbl_complex"
    sql(s"drop table if exists $tableName1")
    sql(
      s"""
         | create table $tableName1 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>
         | ) """.stripMargin)
    sql(s"""
        | insert into table $tableName1
        | select
        |   1,
        |   'a',
        |   1.1,
        |   struct('b', 1.2, struct('bc', 1.21)),
        |   array(struct(1.3), struct(1.4)),
        |   map('l1', struct(1.5), 'l2', struct(1.6))
        | """.stripMargin)
    val tableName2 = "tbl_complex_carbondata"
    sql(s"drop table if exists $tableName2")
    sql(
      s"""
         | create table $tableName2 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>
         | )
         | using carbondata """.stripMargin)
    sql(s"insert into table $tableName2 select * from $tableName1")

    checkAnswer(
      sql(
        s"""
           |select
           |  cast(round(col4.ratio, 1) as float),
           |  cast(round(col4.sub.ratio, 2) as float),
           |  cast(round(col5[1].ratio, 1) as float),
           |  cast(round(col6['l1'].ratio, 1) as float)
           | from $tableName2
           |""".stripMargin),
      sql(
        s"""
           |select
           |  col4.ratio, col4.sub.ratio,
           |  col5[1].ratio,
           |  col6['l1'].ratio
           | from $tableName1
           |""".stripMargin)
    )
  }

  test("test complex datatype for partition table") {
    val tableName1 = "tbl_complex_p"
    sql(s"drop table if exists $tableName1")
    sql(
      s"""
         | create table $tableName1 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>,
         | col7 date
         | ) """.stripMargin)
    sql(s"""
           | insert into table $tableName1
           | select
           |   1,
           |   'a',
           |   1.1,
           |   struct('b', 1.2, struct('bc', 1.21)),
           |   array(struct(1.3), struct(1.4)),
           |   map('l1', struct(1.5), 'l2', struct(1.6)),
           |   to_date('2019-01-01')
           | """.stripMargin)

    val tableName2 = "tbl_complex_p_carbondata"
    sql(s"drop table if exists $tableName2")
    sql(
      s"""
         | create table $tableName2 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>,
         | col7 date
         | )
         | using carbondata
         | partitioned by (col7)
         | """.stripMargin)
    sql(s"insert into table $tableName2 select * from $tableName1")
    checkAnswer(
      sql(
        s"""
           |select
           |  cast(round(col4.ratio, 1) as float),
           |  cast(round(col4.sub.ratio, 2) as float),
           |  cast(round(col5[1].ratio, 1) as float),
           |  cast(round(col6['l1'].ratio, 1) as float)
           | from $tableName2
           |""".stripMargin),
      sql(
        s"""
           |select
           |  col4.ratio, col4.sub.ratio,
           |  col5[1].ratio,
           |  col6['l1'].ratio
           | from $tableName1
           |""".stripMargin)
    )
  }

  test("set global_sort_partitions") {
    val tableName = "tbl_gs_set"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | create table $tableName (
         | col1 int,
         | col2 string,
         | col5 float,
         | col3 date,
         | col4 timestamp
         | )
         | using carbondata
         | options('dateFormat'='yyyy-MM-dd', 'timestampFormat'='yyyy-MM-dd HH:mm:ss',
         | 'sort_scope'='global_sort', 'sort_columns'='col2', 'GLOBAL_sort_Partitions'='1')
         | partitioned by (col3, col4)
       """.stripMargin)

    val exception = intercept[RuntimeException](
      sql(s"""alter table $tableName set tblproperties('GLOBAL_sort_Partitions'='2s')"""))
    assert(exception.getMessage.contains("Table property global_sort_partitions : 2s is invalid"))

    var globalSortPartitions = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession).getGlobalSortPartitions
    assert("1".equals(globalSortPartitions))

    sql(s"""alter table $tableName set tblproperties('GLOBAL_sort_Partitions'='1')""")
    globalSortPartitions = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession).getGlobalSortPartitions
    assert("1".equals(globalSortPartitions))
    sql(
      s"""
         | insert into $tableName (
         |  select col1, col2, 1.2, col3, to_timestamp('2019-02-02 13:01:01') from origin_csv
         |  union all
         |  select 123,'abc', 1.2, to_date('2019-01-01'), to_timestamp('2019-02-02 13:01:01'))
         |  """.stripMargin
    )
    sql(s"""alter table $tableName set tblproperties('GLOBAL_sort_Partitions'='2')""")
    globalSortPartitions = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession).getGlobalSortPartitions
    assert("2".equals(globalSortPartitions))
    sql(
      s"""
         | insert into $tableName (
         |  select col1, col2, 1.2, col3, to_timestamp('2019-02-02 13:01:01') from origin_csv
         |  union all
         |  select 123,'abc', 1.2, to_date('2019-01-01'), to_timestamp('2019-02-02 13:01:01'))
         |  """.stripMargin
    )
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(8)))
    sql(s"describe formatted $tableName").collect()
    checkExistence(
      sql(s"describe formatted $tableName"),
      true,
      "GLOBAL SORT PARTITIONS")
    sql(s"""alter table $tableName unset tblproperties('GLOBAL_sort_Partitions')""")
    globalSortPartitions = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession).getGlobalSortPartitions
    assert(globalSortPartitions == null)
  }

  test("update non-carbon table") {
    var exception = intercept[UnsupportedOperationException]{
      sql("update origin_csv set (col2)=(33aa) where col1 = 1")
    }
    assert(exception.getMessage.contains("only CarbonData table support update operation"))

    exception = intercept[UnsupportedOperationException]{
      sql("delete from origin_csv where col1 = 1")
    }
    assert(exception.getMessage.contains("only CarbonData table support delete operation"))

    var exception2 = intercept[NoSuchTableException]{
      sql("update origin_csv121 set (col2)=(33aa) where col1 = 1")
    }
    assert(exception2.getMessage.contains("Table or view 'origin_csv121' not found"))

    exception2 = intercept[NoSuchTableException]{
      sql("delete from origin_csv121 where col1 = 1")
    }
    assert(exception2.getMessage.contains("Table or view 'origin_csv121' not found"))
  }

  def createDataSourcePartitionTable(provider: String, tableName: String): Unit = {
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName}(col1 int, col2 string) using $provider partitioned by (col2)")
    checkLoading(s"${tableName}")
    val carbonTable = CarbonEnv.getCarbonTable(
      Option("alldatasource"), tableName)(sqlContext.sparkSession)
    assert(carbonTable.isHivePartitionTable)
    sql(s"describe formatted ${tableName}").collect()
    sql(s"show partitions ${tableName}").collect()
    sql(s"show create table ${tableName}").collect()
    sql(s"alter table ${tableName} add partition(col2='dd')").collect()
  }

  def createHivePartitionTable(provider: String, tableName: String): Unit = {
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName}(col1 int) partitioned by (col2 string) stored as carbondata")
    checkLoading(s"${tableName}")
    sql(s"describe formatted ${tableName}").collect()
    sql(s"show partitions ${tableName}").collect()
    sql(s"alter table ${tableName} add partition(col2='dd')").collect()
  }

  def verifyDataSourceTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(col1 int, col2 string) using $provider")
    checkLoading(tableName)
    val table1 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }", Option("alldatasource")))
    assert(table1.tableType == CatalogTableType.MANAGED)
    sql(s"create table ${tableName}_ctas using $provider as select * from ${tableName}")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc")))
    sql(s"insert into ${tableName}_ctas select 123, 'abc'")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc"), Row(123, "abc")))
    val table2 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }_ctas", Option("alldatasource")))
    assert(table2.tableType == CatalogTableType.MANAGED)
  }

  def verifyHiveTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(col1 int, col2 string) stored as $provider")
    checkLoading(tableName)
    val table1 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }", Option("alldatasource")))
    assert(table1.tableType == CatalogTableType.MANAGED)
    sql(s"create table ${tableName}_ctas stored as $provider as select * from ${tableName}")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc")))
    sql(s"insert into ${tableName}_ctas select 123, 'abc'")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc"), Row(123, "abc")))
    val table2 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }_ctas", Option("alldatasource")))
    assert(table2.tableType == CatalogTableType.MANAGED)
  }

  def verifyExternalDataSourceTable(provider: String, tableName: String): Unit = {
    val path = s"${ warehouse }/ds_external"
    val ex = intercept[MalformedCarbonCommandException](
      sql(
        s"""
           |create table ${ tableName }_s
           | using ${provider}
           | LOCATION '$path'
           | as select col1, col2 from origin_csv
           | """.stripMargin))
    assert(ex.getMessage.contains("Create external table as select is not allowed"))

    sql(s"create table ${tableName}_s using ${provider} as select * from origin_csv")
    val carbonTable =
      CarbonEnv.getCarbonTable(Option("alldatasource"), s"${tableName}_s")(sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    sql(s"create table  ${tableName}_e using ${provider} location '${tablePath}'")
    checkAnswer(sql(s"select count(*) from ${tableName}_e"), Seq(Row(3)))
    val table2 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }_e", Option("alldatasource")))
    assert(table2.tableType == CatalogTableType.EXTERNAL)
    sql(s"drop table if exists ${tableName}_e")
    assert(!CarbonPlanHelper.isCarbonTable(
      TableIdentifier(s"${tableName}_e", Option("alldatasource")), sqlContext.sparkSession))
    assert(new File(tablePath).exists())
  }

  def verifyExternalHiveTable(provider: String, tableName: String): Unit = {
    val path = s"${ warehouse }/hive_external"
    val ex = intercept[MalformedCarbonCommandException](
      sql(
        s"""
           |create table ${ tableName }_s
           | stored as ${provider}
           | LOCATION '$path'
           | as select col1, col2 from origin_csv
           | """.stripMargin))
    assert(ex.getMessage.contains("Create external table as select is not allowed"))

    sql(s"create table ${tableName}_s stored as ${provider} as select * from origin_csv")
    val carbonTable =
      CarbonEnv.getCarbonTable(Option("alldatasource"), s"${tableName}_s")(sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    sql(s"create table  ${tableName}_e stored as ${provider} location '${tablePath}'")
    checkAnswer(sql(s"select count(*) from ${tableName}_e"), Seq(Row(3)))
    val table2 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(
      TableIdentifier(s"${ tableName }_e", Option("alldatasource")))
    assert(table2.tableType == CatalogTableType.EXTERNAL)
    sql(s"drop table if exists ${tableName}_e")
    assert(!CarbonPlanHelper.isCarbonTable(
      TableIdentifier(s"${tableName}_e", Option("alldatasource")), sqlContext.sparkSession))
    assert(new File(tablePath).exists())
  }

  def checkLoading(tableName: String): Unit = {
    sql(s"insert into $tableName select 123, 'abc'")
    checkAnswer(sql(s"select * from $tableName"),
      Seq(Row(123, "abc")))
  }

}

class TestProvider extends DatabaseLocationProvider {
  override def provide(originalDatabaseName: String): String = {
    return "projectid." + originalDatabaseName;
  }
}
