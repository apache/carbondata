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

package org.apache.spark.carbondata

import scala.collection.mutable

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.DeprecatedFeatureException
import org.apache.carbondata.core.util.CarbonProperties

class CarbonDataSourceSuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    // Drop table
    sql("DROP TABLE IF EXISTS carbon_testtable")
    sql("DROP TABLE IF EXISTS csv_table")
    sql("DROP TABLE IF EXISTS car")

    // Create table
    sql(
      s"""
         | CREATE TABLE carbon_testtable(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    decimalField decimal(13, 0),
         |    timestampField string)
         | USING carbondata OPTIONS('tableName'='carbon_testtable')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE csv_table
         | (  shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    decimalField decimal(13, 0),
         |    timestampField string)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    sql("drop table carbon_testtable")
    sql("DROP TABLE IF EXISTS csv_table")
    sql("DROP TABLE IF EXISTS car")
    sql("drop table if exists sparkunion")
  }

  test("project") {
    sql("select * from carbon_testtable").collect()
  }

  test("agg") {
    sql("select stringField, sum(intField) , sum(decimalField) " +
      "from carbon_testtable group by stringField").collect()

    sql(
      s"""
         | INSERT INTO TABLE carbon_testtable
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | decimalField, from_unixtime(unix_timestamp(timestampField,'yyyy/M/dd')) timestampField
         | FROM csv_table
       """.stripMargin)
  }

  test("Data mismatch because of min/max calculation while loading the data") {
    CarbonProperties.getInstance()
      .addProperty("carbon.blockletgroup.size.in.mb", "16")
      .addProperty("carbon.enable.vector.reader", "true")
      //.addProperty("enable.unsafe.sort", "true")

    val rdd = sqlContext.sparkContext
      .parallelize(1 to 1200000, 4)
      .map { x =>
        ("city" + x % 8, "country" + x % 1103, "planet" + x % 10007, x.toString,
          (x % 16).toShort, x / 2, (x << 1).toLong, x.toDouble / 13, x.toDouble / 11)
      }.map { x =>
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }

    val schema = StructType(
      Seq(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DoubleType, nullable = false)
      )
    )

    val input = sqlContext.createDataFrame(rdd, schema)
    sql(s"drop table if exists testBigData")
    input.write
      .format("carbondata")
      .option("tableName", "testBigData")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    sql(s"select city, sum(m1) from testBigData " +
        s"where country='country12' group by city order by city")
    checkAnswer(
      sql(s"select city, sum(m1) from testBigData " +
          s"where country='country12' group by city order by city"),
      Seq(Row("city0", 544),
        Row("city1", 680),
        Row("city2", 816),
        Row("city3", 952),
        Row("city4", 1088),
        Row("city5", 1224),
        Row("city6", 1360),
        Row("city7", 1496)))
    sql(s"drop table if exists testBigData")
    CarbonProperties.getInstance()
      .addProperty("carbon.blockletgroup.size.in.mb", "64")
  }

  test("exists function verification test")({
    sql("drop table if exists carbonunion")
    sql("drop table if exists sparkunion")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 1000).map(x => (x+"", (x+100)+"")).toDF("c1", "c2")
    df.createOrReplaceTempView("sparkunion")
    df.write
      .format("carbondata")
      .mode(SaveMode.Overwrite)
      .option("tableName", "carbonunion")
      .save()
    checkAnswer(
      sql("select * from carbonunion where c1='200' and exists(select * from carbonunion)"),
      sql("select * from sparkunion where c1='200' and exists(select * from sparkunion)"))
    checkAnswer(
      sql("select * from carbonunion where c1='200' and exists(select * from carbonunion) and exists(select * from carbonunion)"),
      sql("select * from sparkunion where c1='200' and exists(select * from sparkunion) and exists(select * from sparkunion)"))
    sql("drop table if exists sparkunion")
    sql("drop table if exists carbonunion")
  })

  test("test drop database with and without cascade") {
    sql("drop database if exists testdb cascade")
    sql("create database testdb")
    sql("create table testdb.test1(name string, id int)STORED AS carbondata")
    sql("insert into testdb.test1 select 'xx',1")
    sql("insert into testdb.test1 select 'xx',11")
    try {
      sql("drop database testdb")
      sys.error("drop db should fail as one table exist in db")
    } catch {
      case e: Throwable =>
        println(e.getMessage)
    }
    checkAnswer(sql("select count(*) from testdb.test1"), Seq(Row(2)))
    sql("drop table testdb.test1")
    sql("drop database testdb")
  }

  test("test carbon source table with string type in dictionary_exclude") {
    val ex = intercept[DeprecatedFeatureException] {
      sql("create table car( \nL_SHIPDATE string,\nL_SHIPMODE string,\nL_SHIPINSTRUCT string," +
          "\nL_RETURNFLAG string,\nL_RECEIPTDATE string,\nL_ORDERKEY string,\nL_PARTKEY string," +
          "\nL_SUPPKEY string,\nL_LINENUMBER int,\nL_QUANTITY decimal,\nL_EXTENDEDPRICE decimal," +
          "\nL_DISCOUNT decimal,\nL_TAX decimal,\nL_LINESTATUS string,\nL_COMMITDATE string," +
          "\nL_COMMENT string \n) \nUSING carbondata\nOPTIONS (DICTIONARY_EXCLUDE \"L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_COMMENT\")")
    }
    assert(ex.getMessage.contains("Global dictionary is deprecated in CarbonData 2.0"))
  }

  test("test create table with complex datatype") {
    sql("drop table if exists create_source")
    sql("create table create_source(intField int, stringField string, complexField array<string>) USING carbondata")
    sql("drop table create_source")
  }

  test("test create table with complex datatype without tablename in options") {
    sql("DROP TABLE IF EXISTS create_source")
    sql(
      s"""
         | CREATE TABLE create_source(
         | intField INT,
         | stringField STRING,
         | complexField ARRAY<STRING>)
         | USING carbondata
       """.stripMargin)
    sql("DROP TABLE create_source")
  }

  test("test create table with different tableName in options") {
    sql("DROP TABLE IF EXISTS create_source_test")
    sql("DROP TABLE IF EXISTS create_source_test2")
    sql(
      s"""
         | CREATE TABLE create_source_test(
         | intField INT,
         | stringField STRING,
         | complexField ARRAY<STRING>)
         | USING carbondata
       """.stripMargin)
    checkExistence(sql("show tables"), true, "create_source_test")
    checkExistence(sql("show tables"), false, "create_source_test2")
    sql("DROP TABLE IF EXISTS create_source_test")
    sql("DROP TABLE IF EXISTS create_source_test2")
  }

  test("test to create bucket columns with int field") {
    sql("drop table if exists create_source")
    intercept[Exception] {
      sql("create table create_source(intField int, stringField string, complexField array<string>) USING carbondata OPTIONS('bucketnumber'='1', 'bucketcolumns'='intField')")
    }
  }

  test("test to create bucket columns with complex data type field") {
    sql("drop table if exists create_source")
    intercept[Exception] {
      sql("create table create_source(intField int, stringField string, complexField array<string>) USING carbondata OPTIONS('bucketnumber'='1', 'bucketcolumns'='complexField')")
    }
  }

  test("test check results of table with complex data type and bucketing") {
    sql("drop table if exists create_source")
    sql("create table create_source(intField int, stringField string, complexField array<int>) " +
        "USING carbondata OPTIONS('bucketnumber'='1', 'bucketcolumns'='stringField')")
    sql("insert into create_source values(1,'source',array(1,2,3))")
    checkAnswer(sql("select * from create_source"), Row(1,"source", mutable.WrappedArray.newBuilder[Int].+=(1,2,3)))
    sql("drop table if exists create_source")
  }

  test("test create table without tableName in options, should support") {
    sql("drop table if exists carbon_test")
    sql(
      s"""
         | CREATE TABLE carbon_test(
         |    stringField string,
         |    intField int)
         | USING carbondata
      """.
        stripMargin
    )
    sql("drop table if exists carbon_test")
  }

  test("test create table: using") {
    sql("DROP TABLE IF EXISTS usingTable")
    val e: Exception = intercept[ClassNotFoundException] {
      sql("CREATE TABLE usingTable(name STRING) USING abc")
    }
    assert(e.getMessage.contains("Failed to find data source: abc"))
  }
}
