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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory

/**
 * test functionality for create table as select command
 */
class TestCreateTableAsSelect extends QueryTest with BeforeAndAfterAll {

  private def createTablesAndInsertData {
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

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon_ctas_test")
    sql("DROP TABLE IF EXISTS parquet_ctas_test")
    sql("DROP TABLE IF EXISTS orc_ctas_test")
    createTablesAndInsertData
  }

  test("test create table as select with select from same table name when table exists") {
    sql("drop table if exists ctas_same_table_name")
    sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED by 'carbondata'")
    intercept[Exception] {
      sql("create table ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name")
    }
  }

  test("test create table as select with select from same table name when table does not exists") {
    sql("drop table if exists ctas_same_table_name")
    intercept[Exception] {
      sql("create table ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name")
    }
  }

  test("test create table as select with select from same table name with if not exists clause") {
    sql("drop table if exists ctas_same_table_name")
    sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED by 'carbondata'")
    sql("create table if not exists ctas_same_table_name stored by 'carbondata' as select * from ctas_same_table_name")
    assert(true)
  }

  test("test create table as select with select from another carbon table") {
    sql("DROP TABLE IF EXISTS ctas_select_carbon")
    sql("create table ctas_select_carbon stored by 'carbondata' as select * from carbon_ctas_test")
    checkAnswer(sql("select * from ctas_select_carbon"), sql("select * from carbon_ctas_test"))
  }

  test("test create table as select with select from another parquet table") {
    sql("DROP TABLE IF EXISTS ctas_select_parquet")
    sql("create table ctas_select_parquet stored by 'carbondata' as select * from parquet_ctas_test")
    checkAnswer(sql("select * from ctas_select_parquet"), sql("select * from parquet_ctas_test"))
  }

  test("test create table as select with select from another hive/orc table") {
    sql("DROP TABLE IF EXISTS ctas_select_orc")
    sql("create table ctas_select_orc stored by 'carbondata' as select * from orc_ctas_test")
    checkAnswer(sql("select * from ctas_select_orc"), sql("select * from orc_ctas_test"))
  }

  test("test create table as select with where clause in select from carbon table that returns data") {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon")
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=100")
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test where key=100"))
  }

  test(
    "test create table as select with where clause in select from carbon table that does not return data") {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon")
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=300")
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test where key=300"))
  }

  test("test create table as select with where clause in select from carbon table and load again") {
    sql("DROP TABLE IF EXISTS ctas_select_where_carbon")
    sql("create table ctas_select_where_carbon stored by 'carbondata' as select * from carbon_ctas_test where key=100")
    sql("insert into ctas_select_where_carbon select 200,'hive'")
    checkAnswer(sql("select * from ctas_select_where_carbon"), sql("select * from carbon_ctas_test"))
  }

  test("test create table as select with where clause in select from parquet table") {
    sql("DROP TABLE IF EXISTS ctas_select_where_parquet")
    sql("create table ctas_select_where_parquet stored by 'carbondata' as select * from parquet_ctas_test where key=100")
    checkAnswer(sql("select * from ctas_select_where_parquet"), sql("select * from parquet_ctas_test where key=100"))
  }

  test("test create table as select with where clause in select from hive/orc table") {
    sql("DROP TABLE IF EXISTS ctas_select_where_orc")
    sql("create table ctas_select_where_orc stored by 'carbondata' as select * from orc_ctas_test where key=100")
    checkAnswer(sql("select * from ctas_select_where_orc"), sql("select * from orc_ctas_test where key=100"))
  }

  test("test create table as select with select directly having the data") {
    sql("DROP TABLE IF EXISTS ctas_select_direct_data")
    sql("create table ctas_select_direct_data stored by 'carbondata' as select 300,'carbondata'")
    checkAnswer(sql("select * from ctas_select_direct_data"), Seq(Row(300, "carbondata")))
  }

  test("test create table as select with TBLPROPERTIES") {
    sql("DROP TABLE IF EXISTS ctas_tblproperties_test")
    sql(
      "create table ctas_tblproperties_test stored by 'carbondata' TBLPROPERTIES" +
        "('DICTIONARY_INCLUDE'='key', 'sort_scope'='global_sort') as select * from carbon_ctas_test")
    checkAnswer(sql("select * from ctas_tblproperties_test"), sql("select * from carbon_ctas_test"))
    val carbonTable = CarbonEnv.getInstance(Spark2TestQueryExecutor.spark).carbonMetastore
      .lookupRelation(Option("default"), "ctas_tblproperties_test")(Spark2TestQueryExecutor.spark)
      .asInstanceOf[CarbonRelation].carbonTable
    val metadataFolderPath: CarbonFile = FileFactory.getCarbonFile(carbonTable.getMetaDataFilepath)
    assert(metadataFolderPath.exists())
    val dictFiles: Array[CarbonFile] = metadataFolderPath.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.contains(".dict") || file.getName.contains(".sortindex")
      }
    })
    assert(dictFiles.length == 3)
  }

  test("test create table as select with column name as tupleid") {
    intercept[Exception] {
      sql("create table t2 stored by 'carbondata' as select count(value) AS tupleid from carbon_ctas_test")
    }
  }

  test("test create table as select with column name as positionid") {
    intercept[Exception] {
      sql("create table t2 stored by 'carbondata' as select count(value) AS positionid from carbon_ctas_test")
    }
  }

  test("test create table as select with column name as positionreference") {
    intercept[Exception] {
      sql("create table t2 stored by 'carbondata' as select count(value) AS positionreference from carbon_ctas_test")
    }
  }

  test("test create table as select with where clause in select from parquet table that does not return data") {
    sql("DROP TABLE IF EXISTS ctas_select_where_parquet")
    sql(
      """
        | CREATE TABLE ctas_select_where_parquet
        | STORED BY 'carbondata'
        | as select * FROM parquet_ctas_test
        | where key=300""".stripMargin)
    checkAnswer(sql("SELECT * FROM ctas_select_where_parquet"),
      sql("SELECT * FROM parquet_ctas_test where key=300"))
  }

  test("test create table as select with where clause in select from hive/orc table that does not return data") {
    sql("DROP TABLE IF EXISTS ctas_select_where_orc")
    sql(
      """
        | CREATE TABLE ctas_select_where_orc
        | STORED BY 'carbondata'
        | AS SELECT * FROM orc_ctas_test
        | where key=300""".stripMargin)
    checkAnswer(sql("SELECT * FROM ctas_select_where_orc"),
      sql("SELECT * FROM orc_ctas_test where key=300"))
  }

  test("test create table as select with select from same carbon table name with if not exists clause") {
    sql("drop table if exists ctas_same_table_name")
    sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED BY 'carbondata'")
    checkExistence(sql("SHOW TABLES"), true, "ctas_same_table_name")
    sql(
      """
        | CREATE TABLE IF NOT EXISTS ctas_same_table_name
        | STORED BY 'carbondata'
        | AS SELECT * FROM ctas_same_table_name
      """.stripMargin)
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE ctas_same_table_name
          | STORED BY 'carbondata'
          | AS SELECT * FROM ctas_same_table_name
        """.stripMargin)
    }
  }

  test("test create table as select with select from same carbon table name with if not exists clause and source table not exists") {
    sql("DROP TABLE IF EXISTS ctas_same_table_name")
    checkExistence(sql("SHOW TABLES"), false, "ctas_same_table_name")
    intercept[Exception] {
      sql(
        """
          | CREATE TABLE IF NOT EXISTS ctas_same_table_name
          | STORED BY 'carbondata'
          | AS SELECT * FROM ctas_same_table_name
        """.stripMargin)
    }
  }

  test("test create table as select with select from same carbon table name with if not exists clause and source table exists") {
    sql("DROP TABLE IF EXISTS ctas_same_table_name")
    sql("DROP TABLE IF EXISTS ctas_if_table_name")
    sql("CREATE TABLE ctas_same_table_name(key INT, value STRING) STORED BY 'carbondata'")
    sql(
      """
        | CREATE TABLE IF NOT EXISTS ctas_if_table_name
        | STORED BY 'carbondata'
        | AS SELECT * FROM ctas_same_table_name
      """.stripMargin)
    checkExistence(sql("show tables"), true, "ctas_if_table_name")
  }

  test("Add example for documentation") {
    sql("DROP TABLE IF EXISTS target_table")
    sql("DROP TABLE IF EXISTS source_table")
    // create carbon table and insert data
    sql(
      """
        | CREATE TABLE source_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        |     STORED by 'carbondata'
        |     """.stripMargin)
    sql("INSERT INTO source_table SELECT 1,'bob','shenzhen',27")
    sql("INSERT INTO source_table SELECT 2,'david','shenzhen',31")
    sql(
      """
        | CREATE TABLE target_table
        | STORED BY 'carbondata'
        | AS
        |   SELECT city,avg(age) FROM source_table group by city
      """.stripMargin)
    // results:
    //    sql("SELECT * FROM target_table").show
    //    +--------+--------+
    //    |    city|avg(age)|
    //    +--------+--------+
    //    |shenzhen|    29.0|
    //    +--------+--------+
    checkAnswer(sql("SELECT * FROM target_table"), Seq(Row("shenzhen", 29)))
  }

  override def afterAll {
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
    sql("DROP TABLE IF EXISTS ctas_tblproperties_test")
    sql("DROP TABLE IF EXISTS ctas_if_table_name")
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")

  }
}
