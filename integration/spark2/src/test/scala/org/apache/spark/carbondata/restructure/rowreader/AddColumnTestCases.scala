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

package org.apache.spark.carbondata.restructure.rowreader

import java.io.{File, FileOutputStream, FileWriter}
import java.math.{BigDecimal, RoundingMode}

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

class AddColumnTestCases extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("DROP TABLE IF EXISTS addcolumntest")
    sql("drop table if exists hivetable")
    sql(
      "CREATE TABLE addcolumntest(intField int,stringField string,timestampField timestamp," +
      "decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE addcolumntest " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    sql(
      "Alter table addcolumntest add columns(charField string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField', 'DEFAULT.VALUE.charfield'='def')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE addcolumntest " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("CREATE TABLE hivetable stored as parquet select * from addcolumntest")
  }

  test("test like query on new column") {
    checkAnswer(sql("select charField from addcolumntest where charField like 'd%'"), Row("def"))
  }

  test("test is not null filter on new column") {
    checkAnswer(sql("select charField from addcolumntest where charField is not null"),
      Seq(Row("abc"), Row("def")))
  }

  test("test is null filter on new column") {
    checkAnswer(sql("select charField from addcolumntest where charField is null"), Seq())
  }

  test("test equals filter on new column") {
    checkAnswer(sql("select charField from addcolumntest where charField = 'abc'"), Row("abc"))
  }

  test("test add dictionary column and test greaterthan/lessthan filter on new column") {
    sql(
      "Alter table addcolumntest add columns(intnewField int) TBLPROPERTIES" +
      "('DICTIONARY_INCLUDE'='intnewField', 'DEFAULT.VALUE.intNewField'='5')")
    checkAnswer(sql("select charField from addcolumntest where intnewField > 2"),
      Seq(Row("abc"), Row("def")))
    checkAnswer(sql("select charField from addcolumntest where intnewField < 2"), Seq())
  }

  test("test compaction after adding new column") {
    sql("Alter table addcolumntest compact 'major'")
    checkExistence(sql("show segments for table addcolumntest"), true, "0Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "1Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "0.1Success")
    checkAnswer(sql("select charField from addcolumntest"), Seq(Row("abc"), Row("def")))
  }

  test("test add msr column and check aggregate") {
    sql(
      "alter table addcolumntest add columns(msrField decimal(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkAnswer(sql("select sum(msrField) from addcolumntest"),
      Row(new BigDecimal("246.90").setScale(2, RoundingMode.HALF_UP)))
  }

  test("test join on new column") {
    checkAnswer(sql(
      "select t1.charField, t2.charField from addcolumntest t1, hivetable t2 where t1.charField =" +
      " t2.charField"),
      Seq(Row("abc", "abc"), Row("def", "def")))
  }

  test("test add and drop column with data loading") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2))STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table carbon_table drop columns(timestampField)")
    sql("select * from carbon_table").collect
    sql("Alter table carbon_table add columns(timestampField timestamp)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data5.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,decimalField,timestampField')")
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test add/drop and change datatype") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2))STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table carbon_table drop columns(charField)")
    sql("select * from carbon_table").collect
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(charField string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data2.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField,charField')")
    sql("select * from carbon_table").collect
    sql("ALTER TABLE carbon_table CHANGE decimalField decimalField decimal(22,6)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data3.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField,charField')")
    sql("DROP TABLE IF EXISTS carbon_table")
  }


  test("test add column compaction") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
      "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table carbon_table add columns(decimalField decimal(6,2))")

    sql("Alter table carbon_table compact 'minor'")

    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to add column with char datatype") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
      "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table carbon_table add columns(newfield char(10)) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='char')")
    checkAnswer(sql("select distinct(newfield) from carbon_table"),Row("char"))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if exception is thrown with wrong char syntax") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +

        "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='charField')")
      sql(
        "Alter table carbon_table add columns(newfield char) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test to add column with varchar datatype") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
      "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table carbon_table add columns(newfield varchar(10)) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='char')")
    checkAnswer(sql("select distinct(newfield) from carbon_table"),Row("char"))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if exception is thrown with wrong varchar syntax") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +

        "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='charField')")
      sql(
        "Alter table carbon_table add columns(newfield varchar) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test ("test to check if exception is thrown if table is locked for updation") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField " +
        "timestamp)STORED BY 'carbondata' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='charField')")
      val lockFilePath = s"${ TestQueryExecutor.storeLocation }/default/carbon_table/meta.lock"
      new File(lockFilePath).createNewFile()
      sql(
        "Alter table carbon_table add columns(newfield string) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      new FileOutputStream(lockFilePath).getChannel.lock()
      sql(
        "Alter table carbon_table drop columns(newfield)")
      new File(lockFilePath).delete()
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS addcolumntest")
    sql("drop table if exists hivetable")
  }
}
