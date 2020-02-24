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
import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypesFilterTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    defaultConfig()
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanOnlyBoolean.csv"

    sql("CREATE TABLE if not exists carbon_table(booleanField BOOLEAN) STORED AS carbondata")
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$storeLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    val booleanLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
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
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${booleanLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData')
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
  }

  test("Filtering table: support boolean, Expression: EqualToExpression, IsNotNullExpression, Or") {
    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField = true"),
      Row(4))

    if (SparkTestQueryExecutor.spark.version.startsWith("2.1")) {
      checkAnswer(sql("select count(*) from carbon_table where booleanField = 'true'"),
        Row(0))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from carbon_table where booleanField = \"true\"
           |""".stripMargin),
        Row(0))

      checkAnswer(sql("select count(*) from carbon_table where booleanField = 'false'"),
        Row(0))

    } else {
      // On Spark-2.2 onwards the filter values are eliminated from quotes and pushed to carbon
      // layer. So 'true' will be converted to true and pushed to carbon layer. So in case of
      // condition 'true' and true both output same results.
      checkAnswer(sql("select count(*) from carbon_table where booleanField = 'true'"),
        Row(4))

      checkAnswer(sql(
        s"""
           |select count(*)
           |from carbon_table where booleanField = \"true\"
           |""".stripMargin),
        Row(4))

      checkAnswer(sql("select count(*) from carbon_table where booleanField = 'false'"),
        Row(4))
    }

    checkAnswer(sql("select * from carbon_table where booleanField = false"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField = false"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField = null"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField = false or booleanField = true"),
      Row(8))
  }

  test("Filtering table: support boolean, Expression: InExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField in (true)"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField in (true)"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField in (false)"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField in (false)"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField in (true,false)"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField in (true,false)"),
      Row(8))
  }

  test("Filtering table: support boolean, Expression: NotInExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField not in (false)"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField not in (true)"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField not in (true)"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField not in (true)"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField not in (null)"),
      Seq.empty)

    checkAnswer(sql("select count(*) from carbon_table where booleanField not in (null)"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField not in (true,false)"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: NotEqualsExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField != false"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField != false"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField != true"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField != true"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField != null"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField != false or booleanField != true"),
      Row(8))
  }

  test("Filtering table: support boolean, Expression: GreaterThanEqualToExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField >= true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField >= true"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField >= false"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField >=false"),
      Row(8))

    checkAnswer(sql("select count(*) from carbon_table where booleanField >= null"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: GreaterThanExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField > false"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField > false"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField > false"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField > null"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: LessThanEqualToExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField <= false"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField <= false"),
      Row(4))

    checkAnswer(sql("select * from carbon_table where booleanField <= true"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField <= true"),
      Row(8))

    checkAnswer(sql("select count(*) from carbon_table where booleanField <= null"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: LessThanExpression") {
    checkAnswer(sql("select * from carbon_table where booleanField < true"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField < true"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField < false"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField < null"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: between") {
    checkAnswer(sql("select * from carbon_table where booleanField < true and booleanField >= false"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField < true and booleanField >= false"),
      Row(4))
  }

  test("Filtering table: support boolean, Expression: empty") {
    checkAnswer(sql("select count(*) from carbon_table where booleanField =''"),
      Row(0))
  }

  test("Filtering table: support boolean, Expression: IsNotNull") {
    checkAnswer(sql("select count(*) from carbon_table where booleanField is not null"),
      Row(8))
    checkAnswer(sql("select * from carbon_table where booleanField is not null"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)))
  }

  test("Filtering table: support boolean, Expression: IsNull") {
    checkAnswer(sql("select count(*) from carbon_table where booleanField is null"),
      Row(3))
    checkAnswer(sql("select * from carbon_table where booleanField is null"),
      Seq(Row(null), Row(null), Row(null)))
  }

  test("Filtering table: support boolean and other data type, and") {
    checkAnswer(sql("select count(*) from boolean_table where booleanField = false and shortField = 1"),
      Row(3))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false and stringField='flink'"),
      Row(1))

    checkAnswer(sql("select intField,booleanField,stringField from boolean_table where booleanField = false and stringField='flink'"),
      Row(11, false, "flink"))
  }

  test("Filtering table: support boolean and other data type, GreaterThanEqualToExpression") {
    checkAnswer(sql("select count(*) from boolean_table where booleanField <= false and shortField >= 1"),
      Row(6))
  }

  test("Filtering table: support boolean, like") {
    checkAnswer(sql("select * from carbon_table where booleanField like 't%'"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table where booleanField like 'tru%'"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table where booleanField like '%ue'"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 't%'"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'T%'"),
      Row(0))

    checkAnswer(sql("select * from carbon_table where booleanField like 'f%'"),
      Seq(Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'f%'"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'F%'"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'n%'"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'f%' or booleanField like 't%'"),
      Row(8))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like '%e'"),
      Row(8))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like '%a%'"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like '%z%'"),
      Row(0))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like '%e'"),
      Row(8))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like '_rue'"),
      Row(4))

    checkAnswer(sql("select count(*) from carbon_table where booleanField like 'f___e'"),
      Row(4))
  }

  test("Filtering table: support boolean and other data type, two boolean column") {
    sql("drop table if exists boolean_table2")
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val booleanLocation2 = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
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

       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${booleanLocation2}'
         | INTO TABLE boolean_table2
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
       """.stripMargin)
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = false and booleanField2 = false"),
      Row(4))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = true and booleanField2 = true"),
      Row(3))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = false and booleanField2 = true"),
      Row(2))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = true and booleanField2 = false"),
      Row(1))
    sql("drop table if exists boolean_table2")
  }

  test("Filtering table: support boolean and other data type, load twice") {
    sql("drop table if exists boolean_table2")
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val booleanLocation2 = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
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

       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${booleanLocation2}'
         | INTO TABLE boolean_table2
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${booleanLocation2}'
         | INTO TABLE boolean_table2
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
       """.stripMargin)
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = false and booleanField2 = false"),
      Row(8))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = true and booleanField2 = true"),
      Row(6))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = false and booleanField2 = true"),
      Row(4))
    checkAnswer(sql("select count(*) from boolean_table2 where booleanField = true and booleanField2 = false"),
      Row(2))
    sql("drop table if exists boolean_table2")
  }

}
