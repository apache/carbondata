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

package org.apache.spark.carbondata.restructure.vectorreader

import java.math.BigDecimal

import scala.collection.mutable.WrappedArray.make

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class ChangeDataTypeTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("DROP TABLE IF EXISTS hivetable")
  }

  test("test change datatype on existing column and load data, insert into hive table") {
    def test_change_column_load_insert(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(
        "CREATE TABLE hivetable(intField BIGINT,stringField STRING,charField STRING,timestampField "
          + "TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS PARQUET")
      sql("INSERT INTO TABLE hivetable SELECT * FROM changedatatypetest")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_column_load_insert()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_column_load_insert()
  }

  test("test datatype change and filter") {
    def test_change_datatype_and_filter(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField > 99"),
        Seq(Row("abc"), Row("abc")))
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField < 99"), Seq())
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField = 100"),
        Seq(Row("abc"), Row("abc")))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_datatype_and_filter
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_datatype_and_filter
  }


  test("test change int datatype and load data") {
    def test_change_int_and_load(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT SUM(intField) FROM changedatatypetest"), Row(200))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_int_and_load()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_int_and_load()
  }

  test("test change decimal datatype and compaction") {
    def test_change_decimal_and_compaction(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE decimalField decimalField DECIMAL(9,5)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT decimalField FROM changedatatypetest"),
        Seq(Row(new BigDecimal("21.23").setScale(5)), Row(new BigDecimal("21.23").setScale(5))))
      sql("ALTER TABLE changedatatypetest COMPACT 'major'")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "0 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "1 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "0.1 Success")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_decimal_and_compaction()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_decimal_and_compaction()
  }

  test("test to change int datatype to long") {
    def test_change_int_to_long(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
          "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
        + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intField LONG")
      checkAnswer(sql("SELECT intField FROM changedatatypetest LIMIT 1"), Row(100))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_int_to_long()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_int_to_long()
  }

  test("test data type change for dictionary exclude INT type column") {
    def test_change_data_type(): Unit = {
      beforeAll
    sql("drop table if exists table_sort")
    sql("CREATE TABLE table_sort (imei int,age int,mac string) STORED AS carbondata " +
        "TBLPROPERTIES('SORT_COLUMNS'='imei,age')")
    sql("insert into table_sort select 32674,32794,'MAC1'")
    sql("alter table table_sort change age age bigint")
    sql("insert into table_sort select 32675,9223372036854775807,'MAC2'")
    try {
      sqlContext.setConf("carbon.enable.vector.reader", "true")
      checkAnswer(sql("select * from table_sort"),
        Seq(Row(32674, 32794, "MAC1"), Row(32675, Long.MaxValue, "MAC2")))
    } finally {
      sqlContext.setConf("carbon.enable.vector.reader", "true")
    }
      afterAll
  }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_data_type()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_data_type()
  }

  test("test alter change datatype for complex types") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (name string) STORED AS carbondata")
    // add complex columns
    sql("alter table test_rename add columns(mapField1 MAP<int, int>, " +
        "strField1 struct<a:int,b:decimal(5,2)>, arrField1 array<int>)")
    sql("insert into test_rename values('df',map(5, 6),named_struct('a',1,'b', 123.45),array(1))")
    // change datatype operation
    sql("alter table test_rename change mapField1 mapField1 MAP<int, long>")
    assert(intercept[ProcessMetaDataException] {
      sql("alter table test_rename change strField1 strField1 struct<a:long,b:decimal(3,2)>")
    }.getMessage
      .contains(
        "operation failed for default.test_rename: Alter table data type change or column rename " +
        "operation failed: Given column strfield1.b cannot be modified. Specified precision value" +
        " 3 should be greater than current precision value 5"))
    sql("alter table test_rename change strField1 strField1 struct<a:long,b:decimal(6,2)>")
    sql("alter table test_rename change arrField1 arrField1 array<long>")
    sql("insert into test_rename values('sdf',map(7, 26557544541)," +
        "named_struct('a',26557544541,'b', 1234.45),array(26557544541))")
    // add nested complex columns
    sql("alter table test_rename add columns(mapField2 MAP<int, array<int>>, " +
        "strField2 struct<a:int,b:MAP<int, int>>, arrField2 array<struct<a:int>>)")
    sql("insert into test_rename values('df',map(7, 26557544541),named_struct" +
        "('a',26557544541,'b', 1234.45),array(26557544541),map(5, array(1))," +
        "named_struct('a',1,'b',map(5,6)),array(named_struct('a',1)))")
    // change datatype operation at nested level
    sql("alter table test_rename change mapField2 mapField2 MAP<int, array<long>>")
    sql("alter table test_rename change strField2 strField2 struct<a:int,b:map<int,long>>")
    sql("alter table test_rename change arrField2 arrField2 array<struct<a:long>>")
    sql("insert into test_rename values('sdf',map(7, 26557544541),named_struct" +
        "('a',26557544541,'b', 1234.45),array(26557544541),map(7, array(26557544541))," +
        "named_struct('a',2,'b', map(7, 26557544541)),array(named_struct('a',26557544541)))")
    sql("alter table test_rename compact 'minor'")
    val result1 = java.math.BigDecimal.valueOf(123.45).setScale(2)
    val result2 = java.math.BigDecimal.valueOf(1234.45).setScale(2)
    checkAnswer(sql("select * from test_rename"),
      Seq(
        Row("df", Map(5 -> 6), Row(1, result1), make(Array(1)), null, null, null),
        Row("sdf", Map(7 -> 26557544541L), Row(26557544541L, result2), make(Array(26557544541L)),
          null, null, null),
        Row("df", Map(7 -> 26557544541L), Row(26557544541L, result2), make(Array(26557544541L)),
          Map(5 -> make(Array(1))), Row(1, Map(5 -> 6)), make(Array(Row(1)))),
        Row("sdf", Map(7 -> 26557544541L), Row(26557544541L, result2), make(Array(26557544541L)),
          Map(7 -> make(Array(26557544541L))), Row(2, Map(7 -> 26557544541L)),
          make(Array(Row(26557544541L))))
      ))
  }

  override def afterAll {
    sqlContext.setConf("carbon.enable.vector.reader",
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("DROP TABLE IF EXISTS hivetable")
  }
}
