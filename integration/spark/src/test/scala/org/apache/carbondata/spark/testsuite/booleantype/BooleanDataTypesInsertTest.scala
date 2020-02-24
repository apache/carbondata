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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

class BooleanDataTypesInsertTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    sql("drop table if exists boolean_one_column")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    sql("CREATE TABLE if not exists boolean_one_column(booleanField BOOLEAN) STORED AS carbondata")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists boolean_one_column")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("Inserting and selecting table: one column boolean, should support") {
    sql("insert into boolean_one_column values(true)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true))
    )
  }

  test("Inserting and selecting table: one column boolean and many rows, should support") {
    sql("insert into boolean_one_column values(true)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(TRUE)")
    sql("insert into boolean_one_column values('true')")
    sql("insert into boolean_one_column values(False)")
    sql("insert into boolean_one_column values(false)")
    sql("insert into boolean_one_column values(FALSE)")
    sql("insert into boolean_one_column values('false')")
    sql("insert into boolean_one_column values('tr')")
    sql("insert into boolean_one_column values(null)")
    sql("insert into boolean_one_column values('truEe')")
    sql("insert into boolean_one_column values('falsEe')")
    sql("insert into boolean_one_column values('t')")
    sql("insert into boolean_one_column values('f')")

    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null))
    )
  }

  test("Inserting and selecting table: create one column boolean table and insert two columns") {
    // send to old flow, as for one column two values are inserted.
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("insert into boolean_one_column values(true,false)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(false,true)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(false))
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
        CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
  }

  test("Inserting and selecting table: two columns boolean and many rows, should support") {
    sql("CREATE TABLE if not exists boolean_table2(col1 BOOLEAN, col2 BOOLEAN) STORED AS carbondata")

    sql("insert into boolean_table2 values(true,true)")
    sql("insert into boolean_table2 values(True,false)")
    sql("insert into boolean_table2 values(TRUE,false)")
    sql("insert into boolean_table2 values(false,true)")
    sql("insert into boolean_table2 values(FALSE,false)")
    sql("insert into boolean_table2 values('false',false)")
    sql("insert into boolean_table2 values(null,true)")

    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(true, true), Row(true, false), Row(true, false),
        Row(false, true), Row(false, false), Row(false, false), Row(null, true))
    )
  }

  test("Inserting and selecting table: two columns and other data type, should support") {
    sql("CREATE TABLE if not exists boolean_table2(col1 INT, col2 BOOLEAN) STORED AS carbondata")

    sql("insert into boolean_table2 values(1,true)")
    sql("insert into boolean_table2 values(100,true)")
    sql("insert into boolean_table2 values(1991,false)")
    sql("insert into boolean_table2 values(906,false)")
    sql("insert into boolean_table2 values(218,false)")
    sql("insert into boolean_table2 values(1011,false)")

    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(1, true), Row(100, true), Row(1991, false),
        Row(906, false), Row(218, false), Row(1011, false))
    )
  }

  test("Inserting into table with another table: support boolean data type and other format") {
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

    sql("insert into boolean_table2 select * from boolean_table")
    sql("insert into boolean_table3 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table")
    sql("insert into boolean_table4 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table where shortField > 3")

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

  test("Inserting with the order of data type in source and target table columns being different") {
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

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | booleanField BOOLEAN,
         | shortField SHORT,
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

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert into boolean_table2 select * from boolean_table")

    checkAnswer(
      sql("select booleanField,intField from boolean_table2"),
      Seq(Row(true, 10), Row(true, 17), Row(true, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(true, 10), Row(true, 10), Row(true, 16), Row(true, 10))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table2"),
      Seq(Row(true, 10, true), Row(true, 17, true), Row(true, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(true, 10, false), Row(true, 10, false), Row(true, 16, false), Row(true, 10, false))
    )
  }

  ignore("Inserting with the number of data type in source and target table columns being different, source more than target") {
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

    sql("insert into boolean_table2 select * from boolean_table")
  }

  test("Inserting with the number of data type in source and target table columns being different, source less than target") {
    val exception_insert: Exception =intercept[Exception] {
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

      val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
      val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${storeLocation}'
           | INTO TABLE boolean_table
           | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
      sql("insert into boolean_table2 select * from boolean_table")
    }
    assert(exception_insert.getMessage.contains("Cannot insert into target table because number of columns mismatch"))
  }

  test("Inserting into Hive table from carbon table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE carbon_table(
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
         | CREATE TABLE hive_table(
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
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert into hive_table select * from carbon_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table where exists (select booleanField,intField,booleanField2 " +
        "from hive_table where carbon_table.intField=hive_table.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )

    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("Inserting into carbon table from Hive table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE hive_table(
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
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE carbon_table(
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

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${FileFactory.getUpdatedFilePath(storeLocation)}'
         | INTO TABLE hive_table
           """.stripMargin)

    sql("insert into carbon_table select * from hive_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table where exists (select booleanField,intField,booleanField2 " +
        "from carbon_table where hive_table.intField=carbon_table.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )
  }

  test("Inserting overwrite: one column boolean and many rows, should support") {
    sql("insert into boolean_one_column values(True)")

    sql("insert overwrite table boolean_one_column values(false)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(false))
    )

    sql("insert overwrite table boolean_one_column values(true)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true))
    )

    sql("insert overwrite table boolean_one_column values(null)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(null))
    )

    sql("insert overwrite table boolean_one_column values(true)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true))
    )

    sql("insert overwrite table boolean_one_column values('t')")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true))
    )
  }

  test("Inserting overwrite: create one column boolean table and insert two columns") {
    // send to old flow, as for one column two values are inserted.
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("insert overwrite table boolean_one_column values(true,false)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true))
    )
    sql("insert overwrite table boolean_one_column values(True)")
    sql("insert overwrite table boolean_one_column values(false,true)")
    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(false))
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
        CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
  }

  test("Inserting overwrite: two columns boolean and many rows, should support") {
    sql("CREATE TABLE if not exists boolean_table2(col1 BOOLEAN, col2 BOOLEAN) STORED AS carbondata")

    sql("insert overwrite table boolean_table2 values(true,true)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(true, true))
    )

    sql("insert overwrite table boolean_table2 values(True,false)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(true, false))
    )
    sql("insert overwrite table boolean_table2 values(FALSE,false)")
    sql("insert overwrite table boolean_table2 values('false',false)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(false, false))
    )
    sql("insert overwrite table boolean_table2 values(null,true)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(null, true))
    )
  }

  test("Inserting overwrite: two columns and other data type, should support") {
    sql("CREATE TABLE if not exists boolean_table2(col1 INT, col2 BOOLEAN) STORED AS carbondata")
    sql("insert overwrite table boolean_table2 values(1,true)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(1, true))
    )
    sql("insert overwrite table boolean_table2 values(100,true)")
    sql("insert overwrite table boolean_table2 values(1991,false)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(1991, false))
    )
    sql("insert overwrite table boolean_table2 values(906,false)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(906, false))
    )
    sql("insert overwrite table boolean_table2 values(218,false)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(218, false))
    )
    sql("insert overwrite table boolean_table2 values(1011,true)")
    checkAnswer(
      sql("select * from boolean_table2"),
      Seq(Row(1011, true))
    )
  }

  test("Inserting overwrite: overwrite table with another table: support boolean data type and other format") {
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

  test("Inserting overwrite: overwrite table Hive table from carbon table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE carbon_table(
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
         | CREATE TABLE hive_table(
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
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert overwrite table hive_table select * from carbon_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table where exists (select booleanField,intField,booleanField2 " +
        "from hive_table where carbon_table.intField=hive_table.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )
  }

  test("Inserting overwrite: overwrite table carbon table from Hive table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE hive_table(
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
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE carbon_table(
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

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${FileFactory.getUpdatedFilePath(storeLocation)}'
         | INTO TABLE hive_table
           """.stripMargin)

    sql("insert overwrite table carbon_table select * from hive_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from carbon_table"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from hive_table where exists (select booleanField,intField,booleanField2 " +
        "from carbon_table where hive_table.intField=carbon_table.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )
  }

  test("Inserting table with bad records, and SORT_COLUMNS is boolean column") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      s"""
         | CREATE TABLE if not exists carbon_table(
         | cc BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='cc')
       """.stripMargin)
    sql("insert into carbon_table values(true)")
    sql("insert into carbon_table values(True)")
    sql("insert into carbon_table values(TRUE)")
    sql("insert into carbon_table values('true')")
    sql("insert into carbon_table values(False)")
    sql("insert into carbon_table values(false)")
    sql("insert into carbon_table values(FALSE)")
    sql("insert into carbon_table values('false')")
    sql("insert into carbon_table values('tr')")
    sql("insert into carbon_table values(null)")
    sql("insert into carbon_table values('truEe')")
    sql("insert into carbon_table values('falSee')")
    sql("insert into carbon_table values('t')")
    sql("insert into carbon_table values('f')")
    checkAnswer(
      sql("select * from carbon_table"),
      Seq(
        Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null)))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

}
