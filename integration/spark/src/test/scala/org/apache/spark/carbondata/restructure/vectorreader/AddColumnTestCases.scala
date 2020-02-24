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

import java.io.{File, FileOutputStream}
import java.math.{BigDecimal, RoundingMode}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

class AddColumnTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS addcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
    sql(
      "CREATE TABLE addcolumntest(intField INT,stringField STRING,timestampField TIMESTAMP," +
      "decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE addcolumntest " +
        s"OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    sql(
      "ALTER TABLE addcolumntest ADD COLUMNS(charField STRING) TBLPROPERTIES" +
      "('DEFAULT.VALUE.charfield'='def')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE addcolumntest " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("CREATE TABLE hivetable STORED AS PARQUET SELECT * FROM addcolumntest")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyy")
  }

  test("test like query on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField LIKE 'd%'"), Row("def"))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField LIKE 'd%'"), Row("def"))
  }

  test("test is not null filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField IS NOT NULL"),
      Seq(Row("abc"), Row("def")))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField IS NOT NULL"),
      Seq(Row("abc"), Row("def")))
  }

  test("test is null filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField IS NULL"), Seq())

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField IS NULL"), Seq())
  }

  test("test equals filter on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField = 'abc'"), Row("abc"))

    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql("SELECT charField FROM addcolumntest WHERE charField = 'abc'"), Row("abc"))
  }

  test("test add dictionary column and test greaterthan/lessthan filter on new column") {
    def test_add_and_filter() = {
      sql(
        "ALTER TABLE addcolumntest ADD COLUMNS(intnewField INT) TBLPROPERTIES" +
          "('DEFAULT.VALUE.intNewField'='5')")
      checkAnswer(sql("SELECT charField FROM addcolumntest WHERE intnewField > 2"),
        Seq(Row("abc"), Row("def")))
      checkAnswer(sql("SELECT charField FROM addcolumntest WHERE intnewField < 2"), Seq())
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_and_filter()
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_and_filter
  }

  test("test add msr column and check aggregate") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql(
      "ALTER TABLE addcolumntest ADD COLUMNS(msrField DECIMAL(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkAnswer(sql("SELECT SUM(msrField) FROM addcolumntest"),
      Row(new BigDecimal("246.90").setScale(2, RoundingMode.HALF_UP)))

    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql(
      "ALTER TABLE addcolumntest ADD COLUMNS(msrField DECIMAL(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkAnswer(sql("SELECT SUM(msrField) FROM addcolumntest"),
      Row(new BigDecimal("246.90").setScale(2, RoundingMode.HALF_UP)))
  }

  test("test join on new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    checkAnswer(sql(
      "SELECT t1.charField, t2.charField FROM addcolumntest t1, hivetable t2 WHERE t1.charField =" +
      " t2.charField"),
      Seq(Row("abc", "abc"), Row("def", "def")))
  }

  test("test compaction after adding new column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("ALTER TABLE addcolumntest COMPACT 'major'")
    sql("SHOW SEGMENTS FOR TABLE addcolumntest").show(100, false)
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "0 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "1 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "0.1 Success")
    checkAnswer(sql("SELECT charField FROM addcolumntest"), Seq(Row("abc"), Row("def")))

    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("ALTER TABLE addcolumntest COMPACT 'major'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "0 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "1 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE addcolumntest"), true, "0.1 Success")
    checkAnswer(sql("SELECT charField FROM addcolumntest"), Seq(Row("abc"), Row("def")))
  }

  test("test add and drop column with data loading") {
    def test_add_drop_load() = {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
        "TIMESTAMP,decimalField DECIMAL(6,2))STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE carbon_table DROP COLUMNS(timestampField)")
      sql("SELECT * FROM carbon_table").collect
      sql("ALTER TABLE carbon_table ADD COLUMNS(timestampField TIMESTAMP)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data5.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,charField,decimalField,timestampField')")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_drop_load()
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_drop_load()
  }

  test("test add/drop and change datatype") {
    def test_add_drop_change() = {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
        "TIMESTAMP,decimalField DECIMAL(6,2))STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE carbon_table DROP COLUMNS(charField)")
      sql("SELECT * FROM carbon_table").collect
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
      sql("ALTER TABLE carbon_table ADD COLUMNS(charField STRING) ")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data2.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField,charField')")
      sql("SELECT * FROM carbon_table").collect
      sql("ALTER TABLE carbon_table CHANGE decimalField decimalField DECIMAL(22,6)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data3.csv' INTO TABLE carbon_table " +
          s"OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField,charField')")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_add_drop_change
    afterAll
    beforeAll
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_add_drop_change
  }


  test("test add column compaction") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP)STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("ALTER TABLE carbon_table ADD COLUMNS(decimalField DECIMAL(6,2))")

    sql("ALTER TABLE carbon_table COMPACT 'minor'")

    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to add column with char datatype") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP)STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("ALTER TABLE carbon_table ADD COLUMNS(newfield char(10)) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='char')")
    checkAnswer(sql("SELECT DISTINCT(newfield) FROM carbon_table"),Row("char"))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if exception is thrown with wrong char syntax") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    try {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
        "TIMESTAMP)STORED AS carbondata ")
      sql(
        "ALTER TABLE carbon_table ADD COLUMNS(newfield char) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      sql("DROP TABLE IF EXISTS carbon_table")
      assert(true)
    }
    catch {
      case _: Throwable => assert(false)
    }
  }

  test("test to add column with varchar datatype") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP)STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("ALTER TABLE carbon_table ADD COLUMNS(newfield varchar(10)) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='char')")
    checkAnswer(sql("SELECT DISTINCT(newfield) FROM carbon_table"),Row("char"))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if exception is thrown with wrong varchar syntax") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    try {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +

        "TIMESTAMP)STORED AS carbondata ")
      sql(
        "ALTER TABLE carbon_table ADD COLUMNS(newfield varchar) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      sql("DROP TABLE IF EXISTS carbon_table")
      assert(true)
    }
    catch {
      case exception:Exception => assert(false)
    }
  }

  test("test to check if exception is thrown if TABLE is locked for updation") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS carbon_table")
      sql(
        "CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField " +
        "TIMESTAMP)STORED AS carbondata ")
      val lockFilePath = s"${ TestQueryExecutor.storeLocation }/default/carbon_table/meta.lock"
      new File(lockFilePath).createNewFile()
      sql(
        "ALTER TABLE carbon_table ADD COLUMNS(newfield STRING) TBLPROPERTIES ('DEFAULT.VALUE.newfield'='c')")
      new FileOutputStream(lockFilePath).getChannel.lock()
      sql(
        "ALTER TABLE carbon_table DROP COLUMNS(newfield)")
      new File(lockFilePath).delete()
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test to check if select * works for new added column") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP,decimalField DECIMAL(6,2))STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_new ADD COLUMNS(newField STRING) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='def')")
    checkAnswer(sql("SELECT * FROM carbon_new LIMIT 1"),
      Row(new Integer(100),
        "spark",
        "abc",
        Timestamp.valueOf("2015-04-23 00:00:00.0"),
        new BigDecimal(21.23).setScale(2, RoundingMode.HALF_UP),
        "def"))
    sql("DROP TABLE carbon_new")
  }

  test("test to check data if all columns are provided in select") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP,decimalField DECIMAL(6,2))STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_new ADD COLUMNS(newField STRING) ")
    assert(sql(
      "SELECT intField,stringField,charField,timestampField,decimalField, newField FROM " +
      "carbon_new LIMIT 1").count().equals(1L))
    sql("DROP TABLE carbon_new")
  }

  test("test to check data if new column query order is different from schema order") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      "CREATE TABLE carbon_new(intField INT,stringField STRING,charField STRING,timestampField " +
      "TIMESTAMP,decimalField DECIMAL(6,2))STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_new " +
        s"OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_new ADD COLUMNS(newField STRING) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='def')")
    checkAnswer(sql(
      "SELECT intField,stringField,charField,newField,timestampField,decimalField FROM " +
      "carbon_new LIMIT 1"), Row(new Integer(100),
      "spark",
      "abc",
      "def",
      Timestamp.valueOf("2015-04-23 00:00:00.0"),
      new BigDecimal(21.23).setScale(2, RoundingMode.HALF_UP)))
    sql("DROP TABLE carbon_new")
  }

  test("test to check if vector result collector is able to fetch large amount of data") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql(
      """CREATE TABLE carbon_new (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB
        |TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1
        |decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2
        |double,INTEGER_COLUMN1 INT) STORED AS carbondata TBLPROPERTIES
        |("TABLE_BLOCKSIZE"= "256 MB")""".stripMargin)
    sql("ALTER TABLE carbon_new DROP COLUMNS(CUST_NAME)")
    sql(s"LOAD DATA INPATH '$resourcesPath/restructure/data_2000.csv' INTO TABLE " +
        "carbon_new OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2," +
        "INTEGER_COLUMN1')")
    sql(
      """ALTER TABLE carbon_new ADD COLUMNS(CUST_NAME STRING) TBLPROPERTIES
        ('DEFAULT.VALUE.CUST_NAME'='testuser')""")
    checkAnswer(sql("SELECT DISTINCT(CUST_NAME) FROM carbon_new"),Row("testuser"))
  }

  test("test for checking newly added measure column for is null condition") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_measure_is_null")
    sql("CREATE TABLE carbon_measure_is_null (CUST_ID INT,CUST_NAME STRING) STORED AS carbondata")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' INTO TABLE carbon_measure_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME')")
    sql("ALTER TABLE carbon_measure_is_null ADD COLUMNS (a6 INT)")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' INTO TABLE carbon_measure_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,a6')")
    sql("SELECT a6 FROM carbon_measure_is_null WHERE a6 IS NULL").show
    checkAnswer(sql("SELECT * FROM carbon_measure_is_null"),
      sql("SELECT * FROM carbon_measure_is_null WHERE a6 IS NULL"))
    checkAnswer(sql("SELECT count(*) FROM carbon_measure_is_null WHERE a6 IS NOT NULL"), Row(0))
    sql("DROP TABLE IF EXISTS carbon_measure_is_null")
  }
  test("test to check if intField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField INT) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='67890')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(67890))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if intField returns correct result - dictionary exclude") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField INT) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='67890')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(67890))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if bigintField returns correct result - dictionary exclude") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField bigint) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='67890')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(67890))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if shortField returns correct result") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField short) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='1')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(1))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if doubleField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField double) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='1457567.87')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(1457567.87))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test to check if decimalField returns correct result") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField DECIMAL(5,2)) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='21.87')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(21.87))
    sql("DROP TABLE IF EXISTS carbon_table")
  }


  test("test for checking newly added dictionary column for is null condition") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_dictionary_is_null")
    sql(
      "CREATE TABLE carbon_dictionary_is_null (CUST_ID INT,CUST_NAME STRING)STORED AS carbondata")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' INTO TABLE " +
      s"carbon_dictionary_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME')")
    sql(
      "ALTER TABLE carbon_dictionary_is_null ADD COLUMNS (a6 INT) ")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/restructure/data6.csv' INTO TABLE " +
      s"carbon_dictionary_is_null" +
      s" OPTIONS" +
      s"('BAD_RECORDS_LOGGER_ENABLE'='TRUE', " +
      s"'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,a6')")
    checkAnswer(sql("SELECT * FROM carbon_dictionary_is_null"),
      sql("SELECT * FROM carbon_dictionary_is_null WHERE a6 IS NULL"))
    checkAnswer(sql("SELECT count(*) FROM carbon_dictionary_is_null WHERE a6 IS NOT NULL"), Row(0))
    sql("DROP TABLE IF EXISTS carbon_dictionary_is_null")
  }

  test("test add column for new decimal column filter query") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_decimal_filter")
    sql(
      "CREATE TABLE alter_decimal_filter (n1 STRING, n2 INT, n3 DECIMAL(3,2)) STORED AS carbondata")
    sql("INSERT INTO alter_decimal_filter SELECT 'xx',1,1.22")
    sql("INSERT INTO alter_decimal_filter SELECT 'xx',1,1.23")
    sql("ALTER TABLE alter_decimal_filter CHANGE n3 n3 DECIMAL(8,4)")
    sql("INSERT INTO alter_decimal_filter SELECT 'dd',2,111.111")
    sql("SELECT * FROM alter_decimal_filter WHERE n3 = 1.22").show()
    checkAnswer(sql("SELECT * FROM alter_decimal_filter WHERE n3 = 1.22"),
      Row("xx", 1, new BigDecimal(1.2200).setScale(4, RoundingMode.HALF_UP)))
    sql("DROP TABLE IF EXISTS alter_decimal_filter")
  }

  test("test add column with date") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField date) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='2017-01-01')")
    sql("select * from carbon_table").show(100, false)
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(Date.valueOf("2017-01-01")))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test add column with timestamp") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("CREATE TABLE carbon_table(intField INT,stringField STRING,charField STRING,timestampField TIMESTAMP, decimalField DECIMAL(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE carbon_table OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql(
      "ALTER TABLE carbon_table ADD COLUMNS(newField TIMESTAMP) TBLPROPERTIES" +
      "('DEFAULT.VALUE.newField'='01-01-2017 00:00:00.0')")
    checkAnswer(sql("SELECT DISTINCT(newField) FROM carbon_table"), Row(Timestamp.valueOf("2017-01-01 00:00:00.0")))
    sql("DROP TABLE IF EXISTS carbon_table")
  }

  test("test compaction with all dictionary columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_dict")
    sql("CREATE TABLE alter_dict(stringField STRING,charField STRING) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_dict OPTIONS('FILEHEADER'='stringField,charField')")
    sql("ALTER TABLE alter_dict DROP COLUMNS(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_dict OPTIONS('FILEHEADER'='stringField')")
    sql("ALTER TABLE alter_dict COMPACT 'major'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_dict"), true, "0 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_dict"), true, "1 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_dict"), true, "0.1 Success")
    sql("DROP TABLE IF EXISTS alter_dict")
  }

  test("test sort_columns for add columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_sort_columns")
    sql(
      "CREATE TABLE alter_sort_columns(stringField STRING,charField STRING) STORED AS carbondata")
    val caught = intercept[MalformedCarbonCommandException] {
      sql(
        "ALTER TABLE alter_sort_columns ADD COLUMNS(newField INT) TBLPROPERTIES" +
        "('sort_columns'='newField')")
    }
    assert(caught.getMessage.equals("Unsupported Table property in add column: sort_columns"))
  }

  test("test compaction with all no dictionary columns") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS alter_no_dict")
    sql("CREATE TABLE alter_no_dict(stringField STRING,charField STRING) STORED AS carbondata ")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_no_dict OPTIONS('FILEHEADER'='stringField,charField')")
    sql("ALTER TABLE alter_no_dict DROP COLUMNS(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data7.csv' INTO TABLE alter_no_dict OPTIONS('FILEHEADER'='stringField')")
    sql("ALTER TABLE alter_no_dict COMPACT 'major'")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_no_dict"), true, "0 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_no_dict"), true, "1 Compacted")
    checkExistence(sql("SHOW SEGMENTS FOR TABLE alter_no_dict"), true, "0.1 Success")
    sql("DROP TABLE IF EXISTS alter_no_dict")
  }

  test("no inverted index load and alter table") {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS indexAlter")
    sql(
      """
        CREATE TABLE indexAlter
        (ID Int, date TIMESTAMP, country STRING,
        name STRING, phonetype STRING, serialname STRING)
        STORED AS carbondata
        TBLPROPERTIES('NO_INVERTED_INDEX'='country,name,phonetype')
      """)

    val testData2 = s"$resourcesPath/source.csv"

    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' INTO TABLE indexAlter
           """)

    sql("ALTER TABLE indexAlter ADD COLUMNS(salary STRING) TBLPROPERTIES('no_inverted_index'='salary')")
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData2' INTO TABLE indexAlter
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

  test("no inverted index after alter command") {
    sql("drop table if exists NO_INVERTED_CARBON")
    sql(
      """
           CREATE TABLE IF NOT EXISTS NO_INVERTED_CARBON
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('NO_INVERTED_INDEX'='city')
      """)

    sql("alter table NO_INVERTED_CARBON add columns(col1 string,col2 string) tblproperties('NO_INVERTED_INDEX'='col2')")
    checkExistence(sql("desc formatted NO_INVERTED_CARBON"),false,"Inverted Index Columns name, col1")
  }

  // sort_columns cannot be given for newly added column, so inverted index will not be displayed
  // if it is not in sort_columns
  ignore("inverted index after alter command") {
    sql("drop table if exists NO_INVERTED_CARBON")
    sql(
      """
           CREATE TABLE IF NOT EXISTS NO_INVERTED_CARBON
           (id Int, name String, city String)
           STORED AS carbondata
           TBLPROPERTIES('INVERTED_INDEX'='city')
      """)

    sql("alter table NO_INVERTED_CARBON add columns(col1 string,col2 string) tblproperties('INVERTED_INDEX'='col2')")
    val df = sql("describe formatted NO_INVERTED_CARBON")
    checkExistence(df, true, "Inverted Index Columns city, col2")
  }

  test("test rename textFileTable") {
    sql("drop table if exists renameTextFileTable")
    sql("drop table if exists new_renameTextFileTable")
    sql("create table renameTextFileTable (id int,time string) row format delimited fields terminated by ',' stored as textfile ")
    sql("alter table renameTextFileTable rename to new_renameTextFileTable")
    checkAnswer(sql("DESC new_renameTextFileTable"),Seq(Row("id","int",null),Row("time","string",null)))
    intercept[Exception] {
      sql("select * from renameTextFileTable")
    }
    sql("drop table if exists new_renameTextFileTable")
    sql("drop table if exists renameTextFileTable")
  }

  test("test rename [create table, rename, create same table with different schema]"){
    sql("drop table if exists t5")
    sql("drop table if exists t6")

    sql("create table t5 (c1 string, c2 int) STORED AS carbondata")
    sql("insert into t5 select 'asd',1")
    sql("alter table t5 rename to t6")
    sql("create table t5 (c1 string, c2 int,c3 string) STORED AS carbondata")
    sql("insert into t5 select 'asd',1,'sdf'")
    val t5: CarbonTable = CarbonEnv.getCarbonTable(None, "t5")(sqlContext.sparkSession)
    assert(t5.getTablePath
      .contains(t5.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId))
    checkAnswer(sql("select * from t5"),Seq(Row("asd",1,"sdf")))
  }

  override def afterAll {
    sqlContext.setConf(
      "carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS addcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
    sql("DROP TABLE IF EXISTS alter_sort_columns")
    sql("DROP TABLE IF EXISTS indexAlter")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_new")
    sql("DROP TABLE IF EXISTS carbon_measure_is_null")
    sql("DROP TABLE IF EXISTS carbon_dictionary_is_null")
    sql("DROP TABLE IF EXISTS alter_decimal_filter")
    sql("DROP TABLE IF EXISTS alter_dict")
    sql("DROP TABLE IF EXISTS alter_sort_columns")
    sql("DROP TABLE IF EXISTS alter_no_dict")
    sql("drop table if exists NO_INVERTED_CARBON")
    sql("drop table if exists new_renameTextFileTable")
    sql("drop table if exists renameTextFileTable")

  }
}
