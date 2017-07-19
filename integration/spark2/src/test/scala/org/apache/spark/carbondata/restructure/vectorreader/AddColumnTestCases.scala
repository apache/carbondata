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

import java.io.{File, FileOutputStream, FileWriter}
import java.math.{BigDecimal, RoundingMode}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class AddColumnTestCases extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
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
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("select charField from addcolumntest where charField like 'd%'"), Row("def"))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("select charField from addcolumntest where charField like 'd%'"), Row("def"))
  }

  test("test is not null filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("select charField from addcolumntest where charField is not null"),
      Seq(Row("abc"), Row("def")))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("select charField from addcolumntest where charField is not null"),
      Seq(Row("abc"), Row("def")))
  }

  test("test is null filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("select charField from addcolumntest where charField is null"), Seq())

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("select charField from addcolumntest where charField is null"), Seq())
  }

  test("test equals filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("select charField from addcolumntest where charField = 'abc'"), Row("abc"))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("select charField from addcolumntest where charField = 'abc'"), Row("abc"))
  }

  test("test add dictionary column and test greaterthan/lessthan filter on new column") ({
    def test_add_and_filter() = {
      sql(
        "Alter table addcolumntest add columns(intnewField int) TBLPROPERTIES" +
          "('DICTIONARY_INCLUDE'='intnewField', 'DEFAULT.VALUE.intNewField'='5')")
      checkAnswer(sql("select charField from addcolumntest where intnewField > 2"),
        Seq(Row("abc"), Row("def")))
      checkAnswer(sql("select charField from addcolumntest where intnewField < 2"), Seq())
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_and_filter()
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_and_filter
  })

  test("test add msr column and check aggregate") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql(
      "alter table addcolumntest add columns(msrField DECIMAL(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkAnswer(sql("select sum(msrField) from addcolumntest"),
      Row(new BigDecimal("246.90").setScale(2, RoundingMode.HALF_UP)))

    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql(
      "alter table addcolumntest add columns(msrField decimal(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkAnswer(sql("select sum(msrField) from addcolumntest"),
      Row(new BigDecimal("246.90").setScale(2, RoundingMode.HALF_UP)))
  }

  test("test join on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql(
      "select t1.charField, t2.charField from addcolumntest t1, hivetable t2 where t1.charField =" +
      " t2.charField"),
      Seq(Row("abc", "abc"), Row("def", "def")))
  }

  test("test compaction after adding new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("Alter table addcolumntest compact 'major'")
    checkExistence(sql("show segments for table addcolumntest"), true, "0Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "1Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "0.1Success")
    checkAnswer(sql("select charField from addcolumntest"), Seq(Row("abc"), Row("def")))

    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("Alter table addcolumntest compact 'major'")
    checkExistence(sql("show segments for table addcolumntest"), true, "0Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "1Compacted")
    checkExistence(sql("show segments for table addcolumntest"), true, "0.1Success")
    checkAnswer(sql("select charField from addcolumntest"), Seq(Row("abc"), Row("def")))
  }

  test("test add and drop column with data loading") ({
    def test_add_drop_load() = {
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
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_drop_load()
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_drop_load()
  })

  test("test add/drop and change datatype") ({
    def test_add_drop_change() = {
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
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_drop_change
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_drop_change
  })


  test("test add column compaction") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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

  test("test to check if exception is thrown if table is locked for updation") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
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

  test("test to check if select * works for new added column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField int,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2))STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_new add columns(newField string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='newField','DEFAULT.VALUE.newField'='def')")
    checkAnswer(sql("select * from carbon_new limit 1"),
      Row(new Integer(100),
        "spark",
        "abc",
        Timestamp.valueOf("2015-04-23 00:00:00.0"),
        new BigDecimal(21.23).setScale(2, RoundingMode.HALF_UP),
        "def"))
    sql("drop table carbon_new")
  }

  test("test to check data if all columns are provided in select") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField int,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2))STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_new add columns(newField string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='newField')")
    assert(sql(
      "select intField,stringField,charField,timestampField,decimalField, newField from " +
      "carbon_new limit 1").count().equals(1L))
    sql("drop table carbon_new")
  }

  test("test to check data if new column query order is different from schema order") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField int,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2))STORED BY 'carbondata' TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_new add columns(newField string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='newField','DEFAULT.VALUE.newField'='def')")
    checkAnswer(sql(
      "select intField,stringField,charField,newField,timestampField,decimalField from " +
      "carbon_new limit 1"), Row(new Integer(100),
      "spark",
      "abc",
      "def",
      Timestamp.valueOf("2015-04-23 00:00:00.0"),
      new BigDecimal(21.23).setScale(2, RoundingMode.HALF_UP)))
    sql("drop table carbon_new")
  }

  test("test to check if vector result collector is able to fetch large amount of data") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      """CREATE TABLE carbon_new (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB
        |timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1
        |decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2
        |double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES
        |("TABLE_BLOCKSIZE"= "256 MB")""".stripMargin)
    sql("alter table carbon_new drop columns(CUST_NAME)")
    sql(s"LOAD DATA INPATH '$resourcesPath/restructure/data_2000.csv' into table " +
        "carbon_new OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2," +
        "INTEGER_COLUMN1')")
    sql(
      """alter table carbon_new add columns(CUST_NAME string) TBLPROPERTIES
        ('DICTIONARY_EXCLUDE'='CUST_NAME', 'DEFAULT.VALUE.CUST_NAME'='testuser')""")
    checkAnswer(sql("select distinct(CUST_NAME) from carbon_new"),Row("testuser"))
  }

  test("test for checking newly added measure column for is null condition") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_measure_is_null")
    sql("CREATE TABLE carbon_measure_is_null (CUST_ID int,CUST_NAME String) STORED BY 'carbondata'")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' into table carbon_measure_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME')")
    sql("ALTER TABLE carbon_measure_is_null ADD COLUMNS (a6 int)")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' into table carbon_measure_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,a6')")
    sql("select a6 from carbon_measure_is_null where a6 is null").show
    checkAnswer(sql("select * from carbon_measure_is_null"),
      sql("select * from carbon_measure_is_null where a6 is null"))
    checkAnswer(sql("select count(*) from carbon_measure_is_null where a6 is not null"), Row(0))
    sql("DROP TABLE IF EXISTS carbon_measure_is_null")
  }
  test("test to check if intField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField int) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='67890')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(67890))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if shortField returns correct result") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField short) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='1')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(1))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if doubleField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField double) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='1457567.87')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(1457567.87))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if decimalField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField decimal(5,2)) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='21.87')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(21.87))
    sql("DROP TABLE IF EXISTS carbon_table")
  }


  test("test for checking newly added dictionary column for is null condition") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_dictionary_is_null")
    sql(
      "CREATE TABLE carbon_dictionary_is_null (CUST_ID int,CUST_NAME String) STORED BY " +
      "'carbondata'")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' into table " +
      s"carbon_dictionary_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME')")
    sql(
      "ALTER TABLE carbon_dictionary_is_null ADD COLUMNS (a6 int) tblproperties" +
      "('dictionary_include'='a6')")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' into table " +
      s"carbon_dictionary_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,a6')")
    checkAnswer(sql("select * from carbon_dictionary_is_null"),
      sql("select * from carbon_dictionary_is_null where a6 is null"))
    checkAnswer(sql("select count(*) from carbon_dictionary_is_null where a6 is not null"), Row(0))
    sql("DROP TABLE IF EXISTS carbon_dictionary_is_null")
  }

  test("test add column for new decimal column filter query") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_decimal_filter")
    sql(
      "create table alter_decimal_filter (n1 string, n2 int, n3 decimal(3,2)) stored by " +
      "'carbondata'")
    sql("insert into alter_decimal_filter select 'xx',1,1.22")
    sql("insert into alter_decimal_filter select 'xx',1,1.23")
    sql("alter table alter_decimal_filter change n3 n3 decimal(8,4)")
    sql("insert into alter_decimal_filter select 'dd',2,111.111")
    sql("select * from alter_decimal_filter where n3 = 1.22").show()
    checkAnswer(sql("select * from alter_decimal_filter where n3 = 1.22"),
      Row("xx", 1, new BigDecimal(1.2200).setScale(4, RoundingMode.HALF_UP)))
    sql("DROP TABLE IF EXISTS alter_decimal_filter")
  }

  test("test add column with date") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField date) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='2017-01-01')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(Date.valueOf("2017-01-01")))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test add column with timestamp") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField int,stringField string,charField string,timestampField timestamp, decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "Alter table carbon_table add columns(newField timestamp) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='01-01-2017 00:00:00.0')")
    checkAnswer(sql("select distinct(newField) from carbon_table"), Row(Timestamp.valueOf("2017-01-01 00:00:00.0")))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test compaction with all dictionary columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_dict")
    sql("CREATE TABLE alter_dict(stringField string,charField string) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_dict options('FILEHEADER'='stringField,charField')")
    sql("Alter table alter_dict drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_dict options('FILEHEADER'='stringField')")
    sql("Alter table alter_dict compact 'major'")
    checkExistence(sql("show segments for table alter_dict"), true, "0Compacted")
    checkExistence(sql("show segments for table alter_dict"), true, "1Compacted")
    checkExistence(sql("show segments for table alter_dict"), true, "0.1Success")
    sql("DROP TABLE IF EXISTS alter_dict")
  }

  test("test sort_columns for add columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_sort_columns")
    sql(
      "CREATE TABLE alter_sort_columns(stringField string,charField string) STORED BY 'carbondata'")
    val caught = intercept[MalformedCarbonCommandException] {
      sql(
        "Alter table alter_sort_columns add columns(newField Int) tblproperties" +
        "('sort_columns'='newField')")
    }
    assert(caught.getMessage.equals("Unsupported Table property in add column: sort_columns"))
  }

  test("test compaction with all no dictionary columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_no_dict")
    sql("CREATE TABLE alter_no_dict(stringField string,charField string) STORED BY 'carbondata' TBLPROPERTIES('DICTIONARY_EXCLUDE'='stringField,charField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_no_dict options('FILEHEADER'='stringField,charField')")
    sql("Alter table alter_no_dict drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_no_dict options('FILEHEADER'='stringField')")
    sql("Alter table alter_no_dict compact 'major'")
    checkExistence(sql("show segments for table alter_no_dict"), true, "0Compacted")
    checkExistence(sql("show segments for table alter_no_dict"), true, "1Compacted")
    checkExistence(sql("show segments for table alter_no_dict"), true, "0.1Success")
    sql("DROP TABLE IF EXISTS alter_no_dict")
  }

  test("no inverted index load and alter table") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("drop table if exists indexAlter")
    sql(
      """
        CREATE TABLE IF NOT EXISTS indexAlter
        (ID Int, date Timestamp, country String,
        name String, phonetype String, serialname String)
        STORED BY 'org.apache.carbondata.format'
        TBLPROPERTIES('NO_INVERTED_INDEX'='country,name,phonetype')
      """)

    val testData2 = s"$resourcesPath/source.csv"

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' into table indexAlter
           """)

    sql("alter table indexAlter add columns(salary String) tblproperties('no_inverted_index'='salary')")
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' into table indexAlter
           """)
    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM indexAlter
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("china", 96), Row("france", 1))
    )

  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS addcolumntest")
    sql("drop table if exists hivetable")
    sql("drop table if exists alter_sort_columns")
    sql("drop table if exists indexAlter")
    sqlContext.setConf("carbon.enable.vector.reader", "false")
  }
}
