
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
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest

import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for for create table as select command
 */

class CreateTableAsSelectTestCase extends QueryTest with BeforeAndAfterAll {
         

  //Check create table as select with select from same table name when table exists
  test("CreateTableAsSelect_001_01", Include) {
   sql("drop table if exists ctas_same_table_name").collect
   sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED by 'carbondata'").collect
   intercept[Exception] {
     sql("create table ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name")
   }
  }

  //Check create table as select with select from same table name when table does not exists
  test("CreateTableAsSelect_001_02", Include) {
    sql("drop table if exists ctas_same_table_name").collect
    intercept[Exception] {
      sql("create table ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name")
    }
  }

  //Check create table as select with select from same table name with if not exists clause
  test("CreateTableAsSelect_001_03", Include) {
    sql("drop table if exists ctas_same_table_name").collect
    sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED by 'carbondata'").collect
    sql("create table if not exists ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name").collect
    assert(true)
  }

  //Check create table as select with select from another carbon table
  test("CreateTableAsSelect_001_04", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_carbon").collect
    sql("create table ctas_select_carbon stored by 'carbondata' as select * from carbon_ctas_test").collect
    checkAnswer(sql("select * from ctas_select_carbon"), sql("select * from carbon_ctas_test"))
  }

  //Check create table as select with select from another parquet table
  test("CreateTableAsSelect_001_05", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_parquet").collect
    sql("create table ctas_select_parquet stored by 'carbondata' as select * from parquet_ctas_test").collect
    checkAnswer(sql("select * from ctas_select_parquet"), sql("select * from parquet_ctas_test"))
  }

  //Check test create table as select with select from another hive/orc table
  test("CreateTableAsSelect_001_06", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_orc").collect
    sql("create table ctas_select_orc stored by 'carbondata' as select * from orc_ctas_test").collect
    checkAnswer(sql("select * from ctas_select_orc"), sql("select * from orc_ctas_test"))
  }

  //Check create table as select with where clause in select from carbon table that returns data
  test("CreateTableAsSelect_001_07", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon").collect
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=100").collect
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test where key=100"))
  }

  //Check create table as select with where clause in select from carbon table that does not return data
  test("CreateTableAsSelect_001_08", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon").collect
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=300").collect
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test where key=300"))
  }

  //Check create table as select with where clause in select from carbon table and load again
  test("CreateTableAsSelect_001_09", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon").collect
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=100").collect
    sql("insert into ctas_select_where_carbon select 200,'hive'").collect
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test"))
  }

  //Check create table as select with where clause in select from parquet table
  test("CreateTableAsSelect_001_10", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_parquet").collect
    sql("create table ctas_select_where_parquet stored by 'carbondata' as select * from parquet_ctas_test where key=100").collect
    checkAnswer(sql("select * from ctas_select_where_parquet"), sql("select * from parquet_ctas_test where key=100"))
  }

  //Check create table as select with where clause in select from hive/orc table
  test("CreateTableAsSelect_001_11", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_orc").collect
    sql("create table ctas_select_where_orc stored by 'carbondata' as select * from orc_ctas_test where key=100").collect
    checkAnswer(sql("select * from ctas_select_where_orc"), sql("select * from orc_ctas_test where key=100"))
  }

  //Check create table as select with select directly having the data
  test("CreateTableAsSelect_001_12", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_direct_data").collect
    sql("create table ctas_select_direct_data stored by 'carbondata' as select 300,'carbondata'").collect
    checkAnswer(sql("select * from ctas_select_direct_data"), Seq(Row(300,"carbondata")))
  }

  //Check create table as select with select from another carbon table with more data
  test("CreateTableAsSelect_001_13", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_hugedata1").collect
    sql("DROP TABLE IF EXISTS ctas_select_hugedata2").collect
    sql(s"""CREATE TABLE ctas_select_hugedata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table ctas_select_hugedata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql("create table ctas_select_hugedata2 stored by 'carbondata' as select * from ctas_select_hugedata1").collect
    checkAnswer(sql("select * from ctas_select_hugedata1"), sql("select * from ctas_select_hugedata2"))
    sql("DROP TABLE IF EXISTS ctas_select_hugedata1").collect
    sql("DROP TABLE IF EXISTS ctas_select_hugedata2").collect
  }

  //Check create table as select with where clause in select from parquet table that does not return data
  test("CreateTableAsSelect_001_14", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_parquet").collect
    sql(
      """
        | CREATE TABLE ctas_select_where_parquet
        | STORED BY 'carbondata'
        | AS SELECT * FROM parquet_ctas_test
        | WHERE key=300
      """.stripMargin).collect
    checkAnswer(sql("SELECT * FROM ctas_select_where_parquet"),
      sql("SELECT * FROM parquet_ctas_test where key=300"))
  }

  //Check create table as select with where clause in select from hive/orc table that does not return data
  test("CreateTableAsSelect_001_15", Include) {
    sql("DROP TABLE IF EXISTS ctas_select_where_orc").collect
    sql(
      """
        | CREATE TABLE ctas_select_where_orc
        | STORED BY 'carbondata'
        | AS SELECT * FROM orc_ctas_test
        | WHERE key=100
      """.stripMargin).collect
    checkAnswer(sql("SELECT * FROM ctas_select_where_orc"), sql("SELECT * FROM orc_ctas_test WHERE key=100"))
  }


  override protected def beforeAll() {
   // Dropping existing tables
   sql("DROP TABLE IF EXISTS carbon_ctas_test")
   sql("DROP TABLE IF EXISTS parquet_ctas_test")
   sql("DROP TABLE IF EXISTS orc_ctas_test")

   // create carbon table and insert data
   sql("CREATE TABLE carbon_ctas_test(key INT, value STRING) STORED by 'carbondata'")
   sql("insert into carbon_ctas_test select 100,'spark'")
   sql("insert into carbon_ctas_test select 200,'hive'")

   // create parquet table and insert data
   sql("CREATE TABLE parquet_ctas_test(key INT, value STRING) STORED as parquet")
   sql("insert into parquet_ctas_test select 100,'spark'")
   sql("insert into parquet_ctas_test select 200,'hive'")

   // create hive table and insert data
   sql("CREATE TABLE orc_ctas_test(key INT, value STRING) STORED as ORC")
   sql("insert into orc_ctas_test select 100,'spark'")
   sql("insert into orc_ctas_test select 200,'hive'")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_ctas_test")
    sql("DROP TABLE IF EXISTS parquet_ctas_test")
    sql("DROP TABLE IF EXISTS orc_ctas_test")
    sql("DROP TABLE IF EXISTS ctas_same_table_name")
    sql("DROP TABLE IF EXISTS ctas_select_carbon")
    sql("DROP TABLE IF EXISTS ctas_select_direct_data")
    sql("DROP TABLE IF EXISTS ctas_select_parquet")
    sql("DROP TABLE IF EXISTS ctas_select_orc")
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon")
    sql("DROP TABLE IF EXISTS ctas_select_where_parquet")
    sql("DROP TABLE IF EXISTS ctas_select_where_orc")
    sql("DROP TABLE IF EXISTS ctas_select_direct_data")
    sql("DROP TABLE IF EXISTS ctas_select_hugedata1")
    sql("DROP TABLE IF EXISTS ctas_select_hugedata2")
  }
}