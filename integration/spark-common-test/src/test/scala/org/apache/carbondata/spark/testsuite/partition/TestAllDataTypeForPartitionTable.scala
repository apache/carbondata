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
package org.apache.carbondata.spark.testsuite.partition

import java.sql.{Date, Timestamp}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import scala.collection.mutable

import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest

class TestAllDataTypeForPartitionTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    dropTable
  }

  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    dropTable
  }

  def dropTable = {
    sql("drop table if exists allTypeTable_hash_smallInt")
    sql("drop table if exists allTypeTable_hash_int")
    sql("drop table if exists allTypeTable_hash_bigint")
    sql("drop table if exists allTypeTable_hash_float")
    sql("drop table if exists allTypeTable_hash_double")
    sql("drop table if exists allTypeTable_hash_decimal")
    sql("drop table if exists allTypeTable_hash_timestamp")
    sql("drop table if exists allTypeTable_hash_date")
    sql("drop table if exists allTypeTable_hash_string")
    sql("drop table if exists allTypeTable_hash_varchar")
    sql("drop table if exists allTypeTable_hash_char")

    sql("drop table if exists allTypeTable_list_smallInt")
    sql("drop table if exists allTypeTable_list_int")
    sql("drop table if exists allTypeTable_list_bigint")
    sql("drop table if exists allTypeTable_list_float")
    sql("drop table if exists allTypeTable_list_double")
    sql("drop table if exists allTypeTable_list_decimal")
    sql("drop table if exists allTypeTable_list_timestamp")
    sql("drop table if exists allTypeTable_list_date")
    sql("drop table if exists allTypeTable_list_string")
    sql("drop table if exists allTypeTable_list_varchar")
    sql("drop table if exists allTypeTable_list_char")

    sql("drop table if exists allTypeTable_range_smallInt")
    sql("drop table if exists allTypeTable_range_int")
    sql("drop table if exists allTypeTable_range_bigint")
    sql("drop table if exists allTypeTable_range_float")
    sql("drop table if exists allTypeTable_range_double")
    sql("drop table if exists allTypeTable_range_decimal")
    sql("drop table if exists allTypeTable_range_timestamp")
    sql("drop table if exists allTypeTable_range_date")
    sql("drop table if exists allTypeTable_range_string")
    sql("drop table if exists allTypeTable_range_varchar")
    sql("drop table if exists allTypeTable_range_char")
  }


  test("allTypeTable_hash_smallInt") {
    val tableName = "allTypeTable_hash_smallInt"

    sql(
      s"""create table $tableName(
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(smallIntField smallInt)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = -32768"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 128"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 32767"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_int") {
    val tableName = "allTypeTable_hash_int"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(intField int)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = -2147483648"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 32768"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 2147483647"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_bigint") {
    val tableName = "allTypeTable_hash_bigint"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(bigIntField bigint)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = -9223372036854775808"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 2147483648"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 9223372036854775807"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_float") {
    val tableName = "allTypeTable_hash_float"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(floatField float)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = -2147483648.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483647.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483648.1"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_double") {
    val tableName = "allTypeTable_hash_double"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(doubleField double)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = -9223372036854775808.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = 9223372036854775807.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2")),
        Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))

  }

  test("allTypeTable_hash_decimal") {
    val tableName = "allTypeTable_hash_decimal"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(decimalField decimal(25, 4))
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('-9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775807.1234' as decimal(25, 4))"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_timestamp") {
    val tableName = "allTypeTable_hash_timestamp"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(timestampField timestamp)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-11 00:00:01'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-12 23:59:59'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-13 23:59:59'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  ignore("allTypeTable_hash_date") {
    val tableName = "allTypeTable_hash_date"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(dateField date)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-11'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-12'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-13'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_string") {
    val tableName = "allTypeTable_hash_string"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(stringField string)
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_varchar") {
    val tableName = "allTypeTable_hash_varchar"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(varcharField varchar(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_hash_char") {
    val tableName = "allTypeTable_hash_char"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(charField char(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='hash','num_partitions'='3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_smallInt") {
    val tableName = "allTypeTable_list_smallInt"

    sql(
      s"""create table $tableName(
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(smallIntField smallInt)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-32768, 32767')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = -32768"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 128"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 32767"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_int") {
    val tableName = "allTypeTable_list_int"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(intField int)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-2147483648, 2147483647')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = -2147483648"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 32768"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 2147483647"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_bigint") {
    val tableName = "allTypeTable_list_bigint"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(bigIntField bigint)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-9223372036854775808, 9223372036854775807')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = -9223372036854775808"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 2147483648"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 9223372036854775807"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_float") {
    val tableName = "allTypeTable_list_float"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(floatField float)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-2147483648.1,2147483648.1')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = -2147483648.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483647.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483648.1"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_double") {
    val tableName = "allTypeTable_list_double"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(doubleField double)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-9223372036854775808.1,9223372036854775808.1')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = -9223372036854775808.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = 9223372036854775807.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2")),
        Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))

  }

  test("allTypeTable_list_decimal") {
    val tableName = "allTypeTable_list_decimal"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(decimalField decimal(25, 4))
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='-9223372036854775808.1234, 9223372036854775808.1234')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('-9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775807.1234' as decimal(25, 4))"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_timestamp") {
    val tableName = "allTypeTable_list_timestamp"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(timestampField timestamp)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='2017-06-11 00:00:01, 2017-06-13 23:59:59')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-11 00:00:01'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-12 23:59:59'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-13 23:59:59'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_date") {
    val tableName = "allTypeTable_list_date"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(dateField date)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='2017-06-11,2017-06-13')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-11'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-12'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-13'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_string") {
    val tableName = "allTypeTable_list_string"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(stringField string)
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='abc1,abc2')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_varchar") {
    val tableName = "allTypeTable_list_varchar"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(varcharField varchar(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='abcd1,abcd3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_list_char") {
    val tableName = "allTypeTable_list_char"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(charField char(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='list','list_info'='abcde1,abcde3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_smallInt") {
    val tableName = "allTypeTable_range_smallInt"

    sql(
      s"""create table $tableName(
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(smallIntField smallInt)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='0, 129')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = -32768"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 128"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where smallIntField = 32767"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_int") {
    val tableName = "allTypeTable_range_int"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(intField int)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='-1, 32769')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = -2147483648"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 32768"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where intField = 2147483647"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_bigint") {
    val tableName = "allTypeTable_range_bigint"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(bigIntField bigint)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='-9223372036854775807, 2147483649')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = -9223372036854775808"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 2147483648"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where bigIntField = 9223372036854775807"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_float") {
    val tableName = "allTypeTable_range_float"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(floatField float)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='-2147483647.1,2147483648.1')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = -2147483648.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483647.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where floatField = 2147483648.1"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_double") {
    val tableName = "allTypeTable_range_double"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(doubleField double)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='-9223372036854775807.1,9223372036854775808.1')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = -9223372036854775808.1"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where doubleField = 9223372036854775807.1"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2")),
        Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))

  }

  test("allTypeTable_range_decimal") {
    val tableName = "allTypeTable_range_decimal"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(decimalField decimal(25, 4))
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='-9223372036854775807.1234, 9223372036854775808.1234')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('-9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775807.1234' as decimal(25, 4))"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where decimalField = cast('9223372036854775808.1234' as decimal(25, 4))"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_timestamp") {
    val tableName = "allTypeTable_range_timestamp"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(timestampField timestamp)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='2017-06-11 00:00:02, 2017-06-13 23:59:59')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-11 00:00:01'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-12 23:59:59'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where timestampField = '2017-06-13 23:59:59'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  ignore("allTypeTable_range_date") {
    val tableName = "allTypeTable_range_date"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(dateField date)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='2017-06-12,2017-06-13')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-11'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-12'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where dateField = '2017-06-13'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_string") {
    val tableName = "allTypeTable_range_string"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(stringField string)
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='abc2,abc3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where stringField = 'abc3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_varchar") {
    val tableName = "allTypeTable_range_varchar"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(varcharField varchar(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='abcd2,abcd3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }

  test("allTypeTable_range_char") {
    val tableName = "allTypeTable_range_char"

    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>)
         | partitioned by(charField char(10))
         | stored by 'carbondata'
         | tblproperties('partition_type'='range','range_info'='abcde2,abcde3')
      """.stripMargin)

    sql(s"load data local inpath '$resourcesPath/alldatatypeforpartition.csv' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd1'"),
      Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), Date.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd2'"),
      Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), Date.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"))))

    checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField from $tableName where varcharField = 'abcd3'"),
      Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), Date.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"))))
  }
}
