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
package org.apache.carbondata.spark.testsuite.booleantype

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypesLoadTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  override def beforeAll(): Unit = {
    printConfiguration()
  }

  override def beforeEach(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
    sql("drop table if exists badRecords")
    sql("CREATE TABLE if not exists carbon_table(booleanField BOOLEAN) STORED AS carbondata")
    sql(
      s"""
         | CREATE TABLE badRecords(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | decimalField DECIMAL(18,2),
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
    sql("drop table if exists badRecords")
  }

  test("Loading table: support boolean data type format") {
    val fileLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanOnlyBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false), Row(null), Row(null), Row(null)))
  }

  test("Loading table: support boolean data type format, different format") {
    val fileLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)
        , Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("Loading table: support boolean and other data type") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Loading table: data columns is less than table defined columns") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, null), Row(false, 17, null), Row(false, 11, null),
        Row(true, 10, null), Row(true, 10, null), Row(true, 14, null),
        Row(false, 10, null), Row(false, 10, null), Row(false, 16, null), Row(false, 10, null))
    )
  }

  test("Loading table: support boolean and other data type, data columns bigger than table defined columns") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/mm/dd")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | timestampField TIMESTAMP,
         | dateField DATE
         | )
         | STORED AS carbondata
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

//    checkAnswer(
//      sql("select booleanField,intField from boolean_table"),
//      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
//        Row(true, 10), Row(true, 10), Row(true, 14),
//        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
//    )
    sql("select * from boolean_table where dateField < '2015-01-24'").show
  }

  test("Loading table: support boolean and other data type, with file header") {
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanWithFileHeader.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Loading table: create with TABLE_BLOCKSIZE, NO_INVERTED_INDEX, SORT_SCOPE") {
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('TABLE_BLOCKSIZE'='512','NO_INVERTED_INDEX'='charField', 'SORT_SCOPE'='GLOBAL_SORT')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(sql("select booleanField from boolean_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = true"),
      Row(4))

    checkAnswer(sql("select booleanField from boolean_table where booleanField = false"),
      Seq(Row(false), Row(false), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false"),
      Row(6))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = null"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false or booleanField = true"),
      Row(10))

    if (SparkTestQueryExecutor.spark.version.startsWith("2.1")) {
      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
        Row(0))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from boolean_table where booleanField = \"true\"
           |""".stripMargin),
        Row(0))

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
        Row(0))

    } else {
      // On Spark-2.2 onwards the filter values are eliminated from quotes and pushed to carbon
      // layer. So 'true' will be converted to true and pushed to carbon layer. So in case of
      // condition 'true' and true both output same results.

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
        Row(4))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from boolean_table where booleanField = \"true\"
           |""".stripMargin),
        Row(4))

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
        Row(6))

    }
  }

  test("Loading table: load with DELIMITER, QUOTECHAR, COMMENTCHAR, MULTILINE, ESCAPECHAR, COMPLEX_DELIMITER_LEVEL_1") {
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('TABLE_BLOCKSIZE'='512','NO_INVERTED_INDEX'='charField', 'SORT_SCOPE'='GLOBAL_SORT')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanWithFileHeader.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('DELIMITER'=',','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','COMPLEX_DELIMITER_LEVEL_1'='#','COMPLEX_DELIMITER_LEVEL_2'=':')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(sql("select booleanField from boolean_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = true"),
      Row(4))

    checkAnswer(sql("select booleanField from boolean_table where booleanField = false"),
      Seq(Row(false), Row(false), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false"),
      Row(6))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = null"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false or booleanField = true"),
      Row(10))

    if (SparkTestQueryExecutor.spark.version.startsWith("2.1")) {
      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
        Row(0))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from boolean_table where booleanField = \"true\"
           |""".stripMargin),
        Row(0))

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
        Row(0))

    } else {
      // On Spark-2.2 onwards the filter values are eliminated from quotes and pushed to carbon
      // layer. So 'true' will be converted to true and pushed to carbon layer. So in case of
      // condition 'true' and true both output same results.

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
        Row(4))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from boolean_table where booleanField = \"true\"
           |""".stripMargin),
        Row(4))

      checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
        Row(6))
    }
  }

  test("Loading table: bad_records_action is FORCE") {
    val fileLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField','bad_records_logger_enable'='true','bad_records_action'='FORCE')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("Loading table: bad_records_action is FORCE, support boolean and other data type") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBadRecords.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE badRecords
         | options('bad_records_logger_enable'='true','bad_records_action'='FORCE','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from badRecords"),
      Seq(Row(true, 10, true), Row(null, 17, true), Row(null, 11, true),
        Row(null, 10, true), Row(null, 10, true), Row(true, 14, null),
        Row(false, 10, null), Row(false, 10, null), Row(false, 16, null), Row(false, 10, false))
    )
  }

  test("Loading table: bad_records_action is IGNORE, support boolean and other data type") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBadRecords.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE badRecords
         | options('bad_records_logger_enable'='true','bad_records_action'='IGNORE','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from badRecords"),
      Seq(Row(true, 10, true), Row(false, 10, false))
    )
  }

  test("Loading table: bad_records_action is REDIRECT, support boolean and other data type") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBadRecords.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE badRecords
         | options('bad_records_logger_enable'='true','bad_records_action'='REDIRECT','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from badRecords"),
      Seq(Row(true, 10, true), Row(false, 10, false))
    )
  }

  test("Loading table: bad_records_action is FAIL") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBadRecords.csv"
    val exception_insert: Exception = intercept[Exception] {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${storeLocation}'
           | INTO TABLE badRecords
           | options('bad_records_logger_enable'='true','bad_records_action'='FAIL','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    }
    assert(exception_insert.getMessage.contains("The value with column name booleanfield and column data type BOOLEAN is not a valid BOOLEAN type"))
  }

  test("Loading overwrite: into and then overwrite table with another table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table3(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | stringField STRING,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE boolean_table4(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | stringField STRING,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | overwrite INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert overwrite table boolean_table2 select * from boolean_table")
    sql("insert overwrite table boolean_table3 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table")
    sql("insert overwrite table boolean_table4 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table where shortField > 3")

    checkAnswer(
      sql("select booleanField,intField from boolean_table2"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(
      sql("select booleanField,intField from boolean_table3"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(
      sql("select booleanField,intField from boolean_table4"),
      Seq(Row(false, 17), Row(false, 16))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table2"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table3"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )
  }

  test("Loading overwrite: support boolean data type format, different format") {
    val fileLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | OVERWRITE INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)


    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)
        , Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("Loading overwrite: support boolean and other data type") {
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | OVERWRITE INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Comparing table: support boolean and other data type") {
    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert into boolean_table2 select * from boolean_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table2"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table where exists (select booleanField,intField,booleanField2 " +
        "from boolean_table2 where boolean_table.intField=boolean_table2.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )
  }

  test("Loading table: unsafe, support boolean and other data type") {
    initConf
    sql("drop table if exists boolean_table")
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
    sql("drop table if exists boolean_table")
    defaultConf
  }

  test("Loading table: unsafe, bad_records_action is IGNORE, support boolean and other data type") {
    initConf
    sql("drop table if exists badRecords")
    sql(
      s"""
         | CREATE TABLE badRecords(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | decimalField DECIMAL(18,2),
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBadRecords.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE badRecords
         | options('bad_records_logger_enable'='true','bad_records_action'='IGNORE','FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from badRecords"),
      Seq(Row(true, 10, true), Row(false, 10, false))
    )
    sql("drop table if exists badRecords")
    defaultConf()
  }

  def initConf(): Unit ={
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")
  }

  def defaultConf(): Unit ={
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS_DEFAULT)
  }
}
