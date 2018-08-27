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
package org.apache.spark.sql.carbondata.datasource


import org.apache.spark.sql.carbondata.datasource.TestUtil._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

class SparkCarbonDataSourceTest extends FunSuite  with BeforeAndAfterAll {


  test("test write using dataframe") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    spark.sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("carbon").saveAsTable("testformat")
    assert(spark.sql("select * from testformat").count() == 10)
    assert(spark.sql("select * from testformat where c1='a0'").count() == 1)
    assert(spark.sql("select * from testformat").count() == 10)
    spark.sql("drop table if exists testformat")
  }

  test("test write using ddl") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    spark.sql("drop table if exists testparquet")
    spark.sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
    spark.sql("insert into carbon_table select * from testparquet")
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c1='a1'"), spark.sql("select * from testparquet where c1='a1'"))
    val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
    DataMapStoreManager.getInstance().clearDataMaps(AbsoluteTableIdentifier.from(warehouse1+"/carbon_table"))
    assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    spark.sql("drop table if exists testparquet")
    spark.sql("drop table if exists testformat")
  }

  test("test read with df write") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")

    val frame = spark.read.format("carbon").load(warehouse1 + "/test_folder")
    frame.show()
    assert(frame.count() == 10)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("test write using subfolder") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())
    df.write.format("carbon").save(warehouse1 + "/test_folder/"+System.nanoTime())

    val frame = spark.read.format("carbon").load(warehouse1 + "/test_folder")
    assert(frame.where("c1='a1'").count() == 3)
    val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
    DataMapStoreManager.getInstance().clearDataMaps(AbsoluteTableIdentifier.from(warehouse1+"/test_folder"))
    assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
  }

  test("test write using partition ddl") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").partitionBy("c2").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon  PARTITIONED by (c2)")
    spark.sql("insert into carbon_table select * from testparquet")
    // TODO fix in 2.1
    if (!spark.sparkContext.version.contains("2.1")) {
      assert(spark.sql("select * from carbon_table").count() == 10)
      TestUtil
        .checkAnswer(spark.sql("select * from carbon_table"),
          spark.sql("select * from testparquet"))
    }
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }

  test("test write with struct type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("describe parquet_table").show(false)
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:string, a2:string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with array type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with nested array and struct type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("describe parquet_table").show(false)
    spark.sql("create table carbon_table(c1 string, c2 array<struct<a1:string, a2:string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with nested struct and array type") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, a2:string>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }


  test("test write using ddl and options") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon options('table_blocksize'='256')")
    TestUtil.checkExistence(spark.sql("describe formatted carbon_table"), true, "table_blocksize")
    spark.sql("insert into carbon_table select * from testparquet")
    spark.sql("select * from carbon_table").show()
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }

  test("test read with nested struct and array type without creating table") {
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    val frame = spark.sql("select * from parquet_table")
    frame.write.format("carbon").save(warehouse1 + "/test_carbon_folder")
    val dfread = spark.read.format("carbon").load(warehouse1 + "/test_carbon_folder")
    dfread.show(false)
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_carbon_folder"))
    spark.sql("drop table if exists parquet_table")
  }


  test("test read and write with date datatype") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '2017-11-11'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with date datatype with wrong format") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '11-11-2017'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate Date) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '2017-11-11 00:00:01'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate timestamp) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11 00:00:01'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype with wrong format") {
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
    spark.sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    spark.sql("insert into  date_table select 11, 'ravi', '11-11-2017 00:00:01'")
    spark.sql("create table date_parquet_table(empno int, empname string, projdate timestamp) using parquet")
    spark.sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017 00:00:01'")
    checkAnswer(spark.sql("select * from date_table"), spark.sql("select * from date_parquet_table"))
    spark.sql("drop table if exists date_table")
    spark.sql("drop table if exists date_parquet_table")
  }

  test("test write with array type with filter") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c1='a1' and c2[0]='b'"), spark.sql("select * from parquet_table where c1='a1' and c2[0]='b'"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }

  test("test write with struct type with filter") {
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")),Array(("1", 1), ("2", 2)), x))
      .toDF("c1", "c2", "c3",  "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    spark.sql("create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, a2:string>>, c3 array<struct<a1:string, a2:int>>, number int) using carbon")
    spark.sql("insert into carbon_table select * from parquet_table")
    assert(spark.sql("select * from carbon_table").count() == 10)
    TestUtil.checkAnswer(spark.sql("select * from carbon_table"), spark.sql("select * from parquet_table"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c2.a1[0]='1' and c1='a1'"), spark.sql("select * from parquet_table where c2._1[0]='1' and c1='a1'"))
    TestUtil.checkAnswer(spark.sql("select * from carbon_table where c2.a1[0]='1' and c3[0].a2=1"), spark.sql("select * from parquet_table where c2._1[0]='1' and c3[0]._2=1"))
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists parquet_table")
  }


  test("test read with df write string issue") {
    spark.sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort , x, x.toLong, x.toDouble, BigDecimal.apply(x),  Array(x+1, x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse1 + "/test_folder/")

    spark.sql(s"create table test123 (c1 string, c2 string, arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18)) using carbon location '$warehouse1/test_folder/'")
    checkAnswer(spark.sql("select * from test123"), spark.read.format("carbon").load(warehouse1 + "/test_folder/"))
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    spark.sql("drop table if exists test123")
  }

  test("test read with df write with empty data") {
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    // Saves dataframe to carbon file

    spark.sql(s"create table test123 (c1 string, c2 string, arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18)) using carbon location '$warehouse1/test_folder/'")
    spark.sql(s"create table test123_par (c1 string, c2 string, arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18)) using carbon location '$warehouse1/test_folder/'")
    TestUtil.checkAnswer(spark.sql("select count(*) from test123"), spark.sql("select count(*) from test123_par"))
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse1 + "/test_folder"))
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
  }

  test("test write with nosort columns") {
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort , x, x.toLong, x.toDouble, BigDecimal.apply(x),  Array(x+1, x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("parquet").saveAsTable("test123_par")

    spark.sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc bigint,  doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc struct<_1:string, _2:decimal(38,18)>) using carbon options('sort_columns'='') location '$warehouse1/test_folder/'")
    spark.sql(s"insert into test123 select * from test123_par")
    checkAnswer(spark.sql("select * from test123"), spark.sql(s"select * from test123_par"))
    spark.sql("drop table if exists test123")
    spark.sql("drop table if exists test123_par")
  }

  test("test complex columns mismatch") {
    spark.sql("drop table if exists array_com_hive")
    spark.sql(s"drop table if exists array_com")
    spark.sql("create table array_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items terminated by '$'")
    spark.sql(s"load data local inpath '$resource/Array.csv' into table array_com_hive")
    spark.sql("create table Array_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) using carbon")
    spark.sql("insert into Array_com select * from array_com_hive")
    TestUtil.checkAnswer(spark.sql("select * from Array_com order by CUST_ID ASC limit 3"), spark.sql("select * from array_com_hive order by CUST_ID ASC limit 3"))
    spark.sql("drop table if exists array_com_hive")
    spark.sql(s"drop table if exists array_com")
  }

  test("test complex columns fail while insert ") {
    spark.sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    spark.sql(s"drop table if exists STRUCT_OF_ARRAY_com")
    spark.sql(" create table STRUCT_OF_ARRAY_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp ,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT float, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by '&'")
    spark.sql(s"load data local inpath '$resource/structofarray.csv' into table STRUCT_OF_ARRAY_com_hive")
    spark.sql("create table STRUCT_OF_ARRAY_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) using carbon")
    spark.sql(" insert into STRUCT_OF_ARRAY_com select * from STRUCT_OF_ARRAY_com_hive")
    TestUtil.checkAnswer(spark.sql("select * from STRUCT_OF_ARRAY_com  order by CUST_ID ASC"), spark.sql("select * from STRUCT_OF_ARRAY_com_hive  order by CUST_ID ASC"))
    spark.sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    spark.sql(s"drop table if exists STRUCT_OF_ARRAY_com")
  }

  test("test partition error in carbon") {
    spark.sql("drop table if exists carbon_par")
    spark.sql("drop table if exists parquet_par")
    spark.sql("create table carbon_par (name string, age int, country string) using carbon partitioned by (country)")
    spark.sql("insert into carbon_par select 'b', '12', 'aa'")
    spark.sql("create table parquet_par (name string, age int, country string) using carbon partitioned by (country)")
    spark.sql("insert into parquet_par select 'b', '12', 'aa'")
    checkAnswer(spark.sql("select * from carbon_par"), spark.sql("select * from parquet_par"))
    spark.sql("drop table if exists carbon_par")
    spark.sql("drop table if exists parquet_par")
  }


  override protected def beforeAll(): Unit = {
    drop
  }

  override def afterAll():Unit = {
    drop
  }

  private def drop = {
    spark.sql("drop table if exists testformat")
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testparquet")
  }
}
