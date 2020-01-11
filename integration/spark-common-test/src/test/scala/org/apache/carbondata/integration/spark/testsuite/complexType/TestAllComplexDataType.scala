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
package org.apache.carbondata.integration.spark.testsuite.complexType

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestAllComplexDataType extends QueryTest with BeforeAndAfterAll {

  private val timestampFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
  private val dateFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT)

  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    dropTables()
  }

  override def afterAll: Unit = {
    if(null != dateFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, dateFormat)
    }
    if(null != timestampFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
    }
    dropTables()
  }

  def dropTables(): Unit = {
    sql("drop table if exists complextable")
    sql("drop table if exists hivetable")
    sql("drop table if exists fileformatTable")
  }

  def checkResults(): Unit = {
    checkAnswer(sql("select * from fileformatTable"), sql("select * from hivetable"))
    checkAnswer(sql("select * from complextable"), sql("select * from hivetable"))
    dropTables()
  }

  def createTables(schema: String): Unit = {
    dropTables()
    sql("create table complextable" + schema + " STORED AS carbondata")
    sql("create table hivetable" + schema + " row format delimited fields terminated by ','")
    sql("create table fileformatTable" + schema + " using carbon")
  }

  test("test insert into array of all primitive types") {
    val schema = "(smallintColumn array<short>, intColumn array<int>, " +
    "bigintColumn array<bigint>, doubleColumn array<double>, decimalColumn array<decimal(10,3)>, " +
    "floatColumn array<float>,timestampColumn array<timestamp>, dateColumn array<date>, " +
    "stringColumn array<string>, booleanColumn array<boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(array(1,2,3), array(4,5,6), array(456,789,123), array(1.2,2.3,3.4), " +
      "array(23.2,23.4,34.5), array(23,56,78), array('2017-01-01 12:00:00.0','2017-04-01 12:00:00.0','2017-05-01 12:00:00.0'), " +
      "array('2017-09-08','2018-08-03','2016-01-08'), array('abc','cde','def'), array(true, false, true))")
    }
    insertData("fileformatTable")
    insertData("complextable")
    insertData("hivetable")
    checkResults()
  }

  test("test insert into array of array of all primitive types") {
    val schema = "(smallintColumn array<array<short>>, intColumn array<array<int>>," +
    "bigintColumn array<array<bigint>>, doubleColumn array<array<double>>, decimalColumn array<array<decimal(10,3)>>, " +
    "floatColumn array<array<float>>,timestampColumn array<array<timestamp>>, dateColumn array<array<date>>, " +
    "stringColumn array<array<string>>, booleanColumn array<array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(array(array(1,2,3)), array(array(4,5,6)), array(array(456," +
        "789,123)), array(array(1.2,2.3,3.4)), array(array(23.2,23.4,34.5)), array(array(23,56,78))," +
        "array(array('2017-01-01 12:00:00.0','2017-04-01 12:00:00.0','2017-05-01 12:00:00.0')), " +
        "array(array('2017-09-08','2018-08-03','2016-01-08')), array(array('abc','cde','def'))," +
        "array(array(true, false, true)))")
    }
    insertData("hivetable")
    insertData("complextable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test insert into array of struct of all primitive types") {
    val schema = "(smallintColumn array<struct<s:short>>, intColumn array<struct<i:int>>, " +
    "bigintColumn array<struct<b:bigint>>, doubleColumn array<struct<d:double>>, decimalColumn array<struct<d:decimal(10,3)>>, " +
    "floatColumn array<struct<f:float>>,timestampColumn array<struct<t:timestamp>>, dateColumn array<struct<d:date>>, " +
    "stringColumn array<struct<s:string>>, booleanColumn array<struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(array(named_struct('s',1)), array(named_struct('i',4)), " +
          "array(named_struct('b',456)), array(named_struct('d',1.2)), array(named_struct('d',23.2))," +
          "array(named_struct('f',23)), array(named_struct('t','2017-01-01 12:00:00.0')), " +
          "array(named_struct('d','2017-09-08')), array(named_struct('s','abc')),array(named_struct('b',true)))")
    }
    insertData("hivetable")
    insertData("complextable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test insert into struct of all primitive types") {
    val schema = "(smallintColumn struct<s:short>, intColumn struct<i:int>, " +
    "bigintColumn struct<b:bigint>, doubleColumn struct<d:double>, decimalColumn struct<d:decimal(10,3)>, " +
    "floatColumn struct<f:float>,timestampColumn struct<t:timestamp>, dateColumn struct<d:date>, " +
    "stringColumn struct<s:string>, booleanColumn struct<b:boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(named_struct('s',1), named_struct('i',4), named_struct('b',456), " +
        "named_struct('d',1.2), named_struct('d',23.2), named_struct('f',23),named_struct('t','2017-01-01 12:00:00.0'), " +
        "named_struct('d','2017-09-08'), named_struct('s','abc'), named_struct('b',true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test insert into struct of array of all primitive types") {
    val schema = "(smallintColumn struct<s:array<short>>, intColumn struct<i:array<int>>, " +
    "bigintColumn struct<b:array<bigint>>, doubleColumn struct<d:array<double>>, decimalColumn struct<d:array<decimal(10,3)>>, " +
    "floatColumn struct<f:array<float>>,timestampColumn struct<t:array<timestamp>>, dateColumn struct<d:array<date>>, " +
    "stringColumn struct<s:array<string>>, booleanColumn struct<b:array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(named_struct('s',array(1)), named_struct('i',array(4)), " +
        "named_struct('b',array(456)), named_struct('d',array(1.2)), named_struct('d',array(23.2))," +
        "named_struct('f',array(23)), named_struct('t',array('2017-01-01 12:00:00.0')), " +
        "named_struct('d',array('2017-09-08')), named_struct('s',array('abc')),named_struct('b',array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test insert into struct of struct of all primitive types") {
    val schema = "(smallintColumn struct<s:struct<s1:short>>, intColumn struct<i:struct<i1:int>>, " +
    "bigintColumn struct<b:struct<b1:bigint>>, doubleColumn struct<d:struct<d1:double>>, decimalColumn struct<d:struct<d1:decimal(10,3)>>, " +
    "floatColumn struct<f:struct<f1:float>>,timestampColumn struct<t:struct<t1:timestamp>>, dateColumn struct<d:struct<d1:date>>, " +
    "stringColumn struct<s:struct<s1:string>>, booleanColumn struct<b:struct<b1:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(named_struct('s',named_struct('s1',1)), named_struct('i',named_struct('i1',4)), " +
          "named_struct('b',named_struct('b1',456)), named_struct('d',named_struct('d1',1.2)), named_struct('d',named_struct('d1',23.2)), " +
          "named_struct('f',named_struct('f1',23)), named_struct('t',named_struct('t1','2017-01-01 12:00:00.0')), " +
          "named_struct('d',named_struct('d1','2017-09-08')), named_struct('s',named_struct('s1','abc')), named_struct('b',named_struct('b1',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as short") {
    val schema = "(smallintColumn map<short,short>, intColumn map<short,int>, " +
    "bigintColumn map<short,bigint>, doubleColumn map<short,double>, decimalColumn map<short,decimal(10,3)>, " +
    "floatColumn map<short,float>,timestampColumn map<short,timestamp>, dateColumn map<short,date>, " +
    "stringColumn map<short,string>, booleanColumn map<short,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,1),map(4,5), map(4,789), map(1,2.3), map(2,23), map(2,56)," +
        "map(2,'2017-04-01 12:00:00.0'), map(1,'2017-09-08'), map(4,'abc'), map(1,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as int") {
    val schema = "(smallintColumn map<int,short>, intColumn map<int,int>, " +
    "bigintColumn map<int,bigint>, doubleColumn map<int,double>, decimalColumn map<int,decimal(10,3)>, " +
    "floatColumn map<int,float>,timestampColumn map<int,timestamp>, dateColumn map<int,date>," +
    "stringColumn map<int,string>, booleanColumn map<int,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,1),map(4,5), map(4,789), map(1,2.3), map(2,23), map(2,56), " +
        "map(2,'2017-04-01 12:00:00.0'), map(1,'2017-09-08'), map(4,'abc'), map(1,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as bigint") {
    val schema = "(smallintColumn map<bigint,short>, intColumn map<bigint,int>, " +
    "bigintColumn map<bigint,bigint>, doubleColumn map<bigint,double>, decimalColumn map<bigint,decimal(10,3)>, " +
    "floatColumn map<bigint,float>,timestampColumn map<bigint,timestamp>, dateColumn map<bigint,date>, " +
    "stringColumn map<bigint,string>, booleanColumn map<bigint,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,1),map(4,5), map(4,789), map(1,2.3), map(2,23), map(2,56), " +
        "map(2,'2017-04-01 12:00:00.0'), map(1,'2017-09-08'), map(4,'abc'), map(1,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as double") {
    val schema = "(smallintColumn map<double,short>, intColumn map<double,int>, " +
    "bigintColumn map<double,bigint>, doubleColumn map<double,double>, decimalColumn map<double,decimal(10,3)>, " +
    "floatColumn map<double,float>,timestampColumn map<double,timestamp>, dateColumn map<double,date>, " +
    "stringColumn map<double,string>, booleanColumn map<double,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1.1,1),map(4.1,5), map(4.1,789), map(1.1,2.3), map(2.1,23), map(2.1,56), " +
        "map(2.1,'2017-04-01 12:00:00.0'), map(1.1,'2017-09-08'), map(4.1,'abc'), map(1.1,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as decimal") {
    val schema = "(smallintColumn map<decimal(10,2),short>, intColumn map<decimal(10,2),int>, " +
    "bigintColumn map<decimal(10,2),bigint>, doubleColumn map<decimal(10,2),double>, decimalColumn map<decimal(10,2),decimal(10,3)>," +
    "floatColumn map<decimal(10,2),float>,timestampColumn map<decimal(10,2),timestamp>, dateColumn map<decimal(10,2),date>, " +
    "stringColumn map<decimal(10,2),string>, booleanColumn map<decimal(10,2),boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"12\" as decimal(10,2))),1),map((cast(\"12\" as decimal(10,2))),5), " +
          "map((cast(\"12\" as decimal(10,2))),789), map((cast(\"12\" as decimal(10,2))),2.3), " +
          "map((cast(\"12\" as decimal(10,2))),23), map((cast(\"12\" as decimal(10,2))),56), " +
          "map((cast(\"12\" as decimal(10,2))),'2017-04-01 12:00:00.0'), map((cast(\"12\" as decimal(10,2))),'2017-09-08'), " +
          "map((cast(\"12\" as decimal(10,2))),'abc'), map((cast(\"12\" as decimal(10,2))),true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as float") {
    val schema = "(smallintColumn map<float,short>, intColumn map<float,int>, " +
    "bigintColumn map<float,bigint>, doubleColumn map<float,double>, decimalColumn map<float,decimal(10,3)>, " +
    "floatColumn map<float,float>,timestampColumn map<float,timestamp>, dateColumn map<float,date>, " +
    "stringColumn map<float,string>, booleanColumn map<float,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,1),map(4,5), map(4,789), map(1,2.3), map(2,23), map(2,56), " +
        "map(2,'2017-04-01 12:00:00.0'), map(1,'2017-09-08'), map(4,'abc'), map(1,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as timestamp") {
    val schema = "(smallintColumn map<timestamp,short>, intColumn map<timestamp,int>, " +
    "bigintColumn map<timestamp,bigint>, doubleColumn map<timestamp,double>, decimalColumn map<timestamp,decimal(10,3)>, " +
    "floatColumn map<timestamp,float>, timestampColumn map<timestamp,timestamp>, dateColumn map<timestamp,date>, " +
    "stringColumn map<timestamp,string>, booleanColumn map<timestamp,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),1),map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),5), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),789), map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),2.3), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),23), map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),56), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),'2017-04-01 12:00:00.0'), map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),'2017-09-08'), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),'abc'), map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as date") {
    val schema = "(smallintColumn map<date,short>, intColumn map<date,int>, bigintColumn map<date,bigint>, " +
    "doubleColumn map<date,double>, decimalColumn map<date,decimal(10,3)>, floatColumn map<date,float>,timestampColumn map<date,timestamp>, " +
    "dateColumn map<date,date>, stringColumn map<date,string>, booleanColumn map<date,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01\" as date)),1), map((cast(\"2017-04-01\" as date)),5), " +
          "map((cast(\"2017-04-01\" as date)),789), map((cast(\"2017-04-01\" as date)),2.3), " +
          "map((cast(\"2017-04-01\" as date)),23), map((cast(\"2017-04-01\" as date)),56), " +
          "map((cast(\"2017-04-01\" as date)),'2017-04-01 12:00:00.0'), map((cast(\"2017-04-01\" as date)),'2017-09-08'), " +
          "map((cast(\"2017-04-01\" as date)),'abc'), map((cast(\"2017-04-01\" as date)),true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as string") {
    val schema = "(smallintColumn map<string,short>, intColumn map<string,int>, bigintColumn map<string,bigint>, " +
    "doubleColumn map<string,double>, decimalColumn map<string,decimal(10,3)>, floatColumn map<string,float>,timestampColumn map<string,timestamp>, " +
    "dateColumn map<string,date>, stringColumn map<string,string>, booleanColumn map<string,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map('abcd',1),map('2017-04-01',5), map('abcd',789), map('abcd',2.3), map('abcd',23)," +
        "map('abcd',56), map('abcd','2017-04-01 12:00:00.0'), map('abcd','2017-09-08'), map('abcd','abc'), map('abcd',true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as boolean") {
    val schema = "(smallintColumn map<boolean,short>, intColumn map<boolean,int>, bigintColumn map<boolean,bigint>, " +
    "doubleColumn map<boolean,double>, decimalColumn map<boolean,decimal(10,3)>, floatColumn map<boolean,float>,timestampColumn map<boolean,timestamp>, " +
    "dateColumn map<boolean,date>, stringColumn map<boolean,string>, booleanColumn map<boolean,boolean>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(true,1),map(true,5), map(true,789), map(false,2.3), map(false,23)," +
          "map(false,56), map(false,'2017-04-01 12:00:00.0'), map(true,'2017-09-08'), map(false,'abc'), map(false,true))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as short and value as array") {
    val schema = "(smallintColumn map<short,array<short>>, intColumn map<short,array<int>>, " +
    "bigintColumn map<short,array<bigint>>, doubleColumn map<short,array<double>>, decimalColumn map<short,array<decimal(10,3)>>, " +
    "floatColumn map<short,array<float>>,timestampColumn map<short,array<timestamp>>, dateColumn map<short,array<date>>, " +
    "stringColumn map<short,array<string>>, booleanColumn map<short,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,array(1)),map(4,array(5)), map(4,array(789)), map(1,array(2.3)), map(2,array(23)), " +
        "map(2,array(56)), map(2,array('2017-04-01 12:00:00.0')), map(1,array('2017-09-08')), map(4,array('abc')), map(1,array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as int and value as array") {
    val schema = "(smallintColumn map<bigint,array<short>>, intColumn map<bigint,array<int>>, " +
    "bigintColumn map<bigint,array<bigint>>, doubleColumn map<bigint,array<double>>, decimalColumn map<bigint,array<decimal(10,3)>>, " +
    "floatColumn map<bigint,array<float>>,timestampColumn map<bigint,array<timestamp>>, dateColumn map<bigint,array<date>>, " +
    "stringColumn map<bigint,array<string>>, booleanColumn map<bigint,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,array(1)),map(4,array(5)), map(4,array(789)), map(1,array(2.3)), map(2,array(23)), " +
          "map(2,array(56)), map(2,array('2017-04-01 12:00:00.0')), map(1,array('2017-09-08')), map(4,array('abc')), map(1,array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as bigint and value as array") {
    val schema = "(smallintColumn map<bigint,array<short>>, intColumn map<bigint,array<int>>, " +
    "bigintColumn map<bigint,array<bigint>>, doubleColumn map<bigint,array<double>>, decimalColumn map<bigint,array<decimal(10,3)>>, " +
    "floatColumn map<bigint,array<float>>,timestampColumn map<bigint,array<timestamp>>, dateColumn map<bigint,array<date>>, " +
    "stringColumn map<bigint,array<string>>, booleanColumn map<bigint,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,array(1)),map(4,array(5)), map(4,array(789)), map(1,array(2.3)), map(2,array(23)), " +
          "map(2,array(56)), map(2,array('2017-04-01 12:00:00.0')), map(1,array('2017-09-08')), map(4,array('abc')), map(1,array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as double and value as array") {
    val schema = "(smallintColumn map<double,array<short>>, intColumn map<double,array<int>>, bigintColumn map<double,array<bigint>>, " +
    "doubleColumn map<double,array<double>>, decimalColumn map<double,array<decimal(10,3)>>, floatColumn map<double,array<float>>,timestampColumn map<double,array<timestamp>>, " +
    "dateColumn map<double,array<date>>, stringColumn map<double,array<string>>, booleanColumn map<double,array<boolean>>)"
    sql("create table complextable" + schema + " STORED AS carbondata")
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1.1,array(1)),map(4.1,array(5)), map(4.1,array(789)), map(1.1,array(2.3)), map(2.1,array(23)), " +
          "map(2.1,array(56)), map(2.1,array('2017-04-01 12:00:00.0')), map(1.1,array('2017-09-08')), map(4.1,array('abc')), map(1.1,array(true)))")
    }
    insertData("complextable")
    sql("create table hivetable" + schema + " row format delimited fields terminated by ','")
    insertData("hivetable")
    sql("create table fileformatTable" + schema + " using carbon")
    insertData("fileformatTable")
    checkResults()
    dropTables()
  }

  test("test map of all primitive types with key as decimal and value as array") {
    val schema = "(smallintColumn map<decimal(10,2),array<short>>, intColumn map<decimal(10,2),array<int>>, bigintColumn map<decimal(10,2),array<bigint>>, " +
    "doubleColumn map<decimal(10,2), array<double>>, decimalColumn map<decimal(10,2),array<decimal(10,3)>>, floatColumn map<decimal(10,2),array<float>>, " +
    "timestampColumn map<decimal(10,2),array<timestamp>>, dateColumn map<decimal(10,2),array<date>>, stringColumn map<decimal(10,2),array<string>>, " +
    "booleanColumn map<decimal(10,2),array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"12\" as decimal(10,2))),array(1)), map((cast(\"12\" as decimal(10,2))),array(5)), " +
          "map((cast(\"12\" as decimal(10,2))),array(789)), map((cast(\"12\" as decimal(10,2))),array(2.3)), " +
          "map((cast(\"12\" as decimal(10,2))),array(23)), map((cast(\"12\" as decimal(10,2))),array(56))," +
          "map((cast(\"12\" as decimal(10,2))),array('2017-04-01 12:00:00.0')), map((cast(\"12\" as decimal(10,2))),array('2017-09-08')), " +
          "map((cast(\"12\" as decimal(10,2))),array('abc')), map((cast(\"12\" as decimal(10,2))),array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as float and value as array") {
    val schema = "(smallintColumn map<float,array<short>>, intColumn map<float,array<int>>, bigintColumn map<float,array<bigint>>, " +
    "doubleColumn map<float,array<double>>, decimalColumn map<float,array<decimal(10,3)>>, floatColumn map<float,array<float>>, timestampColumn map<float,array<timestamp>>, " +
    "dateColumn map<float,array<date>>, stringColumn map<float,array<string>>, booleanColumn map<float,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,array(1)),map(4,array(5)), map(4,array(789)), map(1,array(2.3)), map(2,array(23)), map(2,array(56)), " +
        "map(2,array('2017-04-01 12:00:00.0')), map(1,array('2017-09-08')), map(4,array('abc')), map(1,array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as timestamp and value as array") {
    val schema = "(smallintColumn map<timestamp,array<short>>, intColumn map<timestamp,array<int>>, bigintColumn map<timestamp,array<bigint>>, " +
    "doubleColumn map<timestamp,array<double>>, decimalColumn map<timestamp,array<decimal(10,3)>>, floatColumn map<timestamp,array<float>>, " +
    "timestampColumn map<timestamp,array<timestamp>>, dateColumn map<timestamp,array<date>>, stringColumn map<timestamp,array<string>>, " +
    "booleanColumn map<timestamp,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(1))," +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(5)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(789)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(2.3)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(23))," +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(56)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array('2017-04-01 12:00:00.0')), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array('2017-09-08')), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array('abc'))," +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as date and value as array") {
    val schema = "(smallintColumn map<date,array<short>>, intColumn map<date,array<int>>, bigintColumn map<date,array<bigint>>, " +
    "doubleColumn map<date,array<double>>, decimalColumn map<date,array<decimal(10,3)>>, floatColumn map<date,array<float>>,timestampColumn map<date,array<timestamp>>, " +
    "dateColumn map<date,array<date>>, stringColumn map<date,array<string>>, booleanColumn map<date,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01\" as date)),array(1))," +
          "map((cast(\"2017-04-01\" as date)),array(5)), " +
          "map((cast(\"2017-04-01\" as date)),array(789)), " +
          "map((cast(\"2017-04-01\" as date)),array(2.3)), " +
          "map((cast(\"2017-04-01\" as date)),array(23))," +
          "map((cast(\"2017-04-01\" as date)),array(56))," +
          "map((cast(\"2017-04-01\" as date)),array('2017-04-01 12:00:00.0')), " +
          "map((cast(\"2017-04-01\" as date)),array('2017-09-08')), " +
          "map((cast(\"2017-04-01\" as date)),array('abc'))," +
          "map((cast(\"2017-04-01\" as date)),array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as string and value as array") {
    val schema = "(smallintColumn map<string,array<short>>, intColumn map<string,array<int>>, bigintColumn map<string,array<bigint>>, " +
    "doubleColumn map<string,array<double>>, decimalColumn map<string,array<decimal(10,3)>>, floatColumn map<string,array<float>>," +
    "timestampColumn map<string,array<timestamp>>, dateColumn map<string,array<date>>, stringColumn map<string,array<string>>, " +
    "booleanColumn map<string,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map('abcd',array(1)),map('2017-04-01',array(5)), map('abcd',array(789)), map('abcd',array(2.3)), map('abcd',array(23))," +
        "map('abcd',array(56)), map('abcd',array('2017-04-01 12:00:00.0')), map('abcd',array('2017-09-08')), map('abcd',array('abc')), map('abcd',array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as boolean and value as array") {
    val schema = "(smallintColumn map<boolean,array<short>>, intColumn map<boolean,array<int>>, bigintColumn map<boolean,array<bigint>>, " +
    "doubleColumn map<boolean,array<double>>, decimalColumn map<boolean,array<decimal(10,3)>>, floatColumn map<boolean,array<float>>, timestampColumn map<boolean,array<timestamp>>," +
    "dateColumn map<boolean,array<date>>, stringColumn map<boolean,array<string>>, booleanColumn map<boolean,array<boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(true,array(1)),map(true,array(5)), map(true,array(789)), map(false,array(2.3)), map(false,array(23))," +
        "map(false,array(56)), map(false,array('2017-04-01 12:00:00.0')), map(true,array('2017-09-08')), map(false,array('abc')), map(false,array(true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as short and value as struct") {
    val schema = "(smallintColumn map<short,struct<s:short>>, intColumn map<short,struct<i:int>>, bigintColumn map<short,struct<b:bigint>>, " +
    "doubleColumn map<short,struct<d:double>>, decimalColumn map<short,struct<d:decimal(10,3)>>, floatColumn map<short,struct<f:float>>,timestampColumn map<short,struct<t:timestamp>>," +
    "dateColumn map<short,struct<d:date>>, stringColumn map<short,struct<s:string>>, booleanColumn map<short,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,named_struct('s',1)),map(4,named_struct('i',5)), map(4,named_struct('b',789)), map(1,named_struct('d',2.3)), " +
          "map(2,named_struct('d',23)), map(2,named_struct('f',56)), map(2,named_struct('t','2017-04-01 12:00:00.0')), map(1,named_struct('d','2017-09-08'))," +
          "map(4,named_struct('s','abc')), map(1,named_struct('b', true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as int and value as struct") {
    val schema = "(smallintColumn map<int,struct<s:short>>, intColumn map<int,struct<i:int>>, bigintColumn map<int,struct<b:bigint>>, " +
    "doubleColumn map<int,struct<d:double>>, decimalColumn map<int,struct<d:decimal(10,3)>>, floatColumn map<int,struct<f:float>>,timestampColumn map<int,struct<t:timestamp>>, " +
    "dateColumn map<int,struct<d:date>>, stringColumn map<int,struct<s:string>>, booleanColumn map<int,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,named_struct('s',1)),map(4,named_struct('i',5)),map(4,named_struct('b',789)), map(1,named_struct('d',2.3)), " +
          "map(2,named_struct('d',23)),map(2,named_struct('f',56)), map(2,named_struct('t','2017-04-01 12:00:00.0')), map(1,named_struct('d','2017-09-08'))," +
          "map(4,named_struct('s','abc')), map(1,named_struct('b', true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as bigint and value as struct") {
    val schema = "(smallintColumn map<bigint,struct<s:short>>, intColumn map<bigint,struct<i:int>>, bigintColumn map<bigint,struct<b:bigint>>, " +
    "doubleColumn map<bigint,struct<d:double>>, decimalColumn map<bigint,struct<d:decimal(10,3)>>, floatColumn map<bigint,struct<f:float>>,timestampColumn map<bigint,struct<t:timestamp>>, " +
    "dateColumn map<bigint,struct<d:date>>, stringColumn map<bigint,struct<s:string>>, booleanColumn map<bigint,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(map(1,named_struct('s',1)),map(4,named_struct('i',5)), map(4,named_struct('b',789)), map(1,named_struct('d',2.3)), " +
          "map(2,named_struct('d',23)), map(2,named_struct('f',56)), map(2,named_struct('t','2017-04-01 12:00:00.0')), map(1,named_struct('d','2017-09-08'))," +
          "map(4,named_struct('s','abc')), map(1,named_struct('b', true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as double and value as struct") {
    val schema = "(smallintColumn map<double,struct<s:short>>, intColumn map<double,struct<i:int>>, " +
    "bigintColumn map<double,struct<b:bigint>>, doubleColumn map<double,struct<d:double>>, decimalColumn map<double,struct<d:decimal(10,3)>>, " +
    "floatColumn map<double,struct<f:float>>,timestampColumn map<double,struct<t:timestamp>>, dateColumn map<double,struct<d:date>>, " +
    "stringColumn map<double,struct<s:string>>, booleanColumn map<double,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map(1.1,named_struct('s',1)),map(4.1,named_struct('i',5))," +
          "map(4.1,named_struct('b',789)), map(1.1,named_struct('d',2.3)), " +
          "map(2.1,named_struct('d',23)), map(2.1,named_struct('d',56)), " +
          "map(2.1,named_struct('t','2017-04-01 12:00:00.0')), map(1.1,named_struct('d','2017-09-08')), " +
          "map(4.1,named_struct('s','abc')), map(1.1,named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as decimal and value as struct") {
    val schema = "(smallintColumn map<decimal(10,2),struct<s:short>>, intColumn map<decimal(10,2),struct<i:int>>, " +
    "bigintColumn map<decimal(10,2),struct<b:bigint>>, doubleColumn map<decimal(10,2),struct<d:double>>, decimalColumn map<decimal(10,2),struct<d:decimal(10,3)>>, " +
    "floatColumn map<decimal(10,2),struct<f:float>>,timestampColumn map<decimal(10,2),struct<t:timestamp>>, dateColumn map<decimal(10,2),struct<d:date>>, " +
    "stringColumn map<decimal(10,2),struct<s:string>>, booleanColumn map<decimal(10,2),struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(
        s"insert into $tableName values(" +
        "map((cast(\"12\" as decimal(10,2))),named_struct('s',1)),map((cast(\"12\" as decimal(10,2))),named_struct('i',5)), " +
        "map((cast(\"12\" as decimal(10,2))),named_struct('b',789)), map((cast(\"12\" as decimal(10,2))),named_struct('d',2.3)), " +
        "map((cast(\"12\" as decimal(10,2))),named_struct('d',23)), map((cast(\"12\" as decimal(10,2))),named_struct('f',56)), " +
        "map((cast(\"12\" as decimal(10,2))),named_struct('t','2017-04-01 12:00:00.0')), map((cast(\"12\" as decimal(10,2))),named_struct('d','2017-09-08')), " +
        "map((cast(\"12\" as decimal(10,2))),named_struct('s','abc')), map((cast(\"12\" as decimal(10,2))),named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as float and value as struct") {
    val schema = "(smallintColumn map<float,struct<s:short>>, intColumn map<float,struct<i:int>>, bigintColumn map<float,struct<b:bigint>>, " +
    "doubleColumn map<float,struct<d:double>>, decimalColumn map<float,struct<d:decimal(10,3)>>, floatColumn map<float,struct<f:float>>, timestampColumn map<float,struct<t:timestamp>>, " +
    "dateColumn map<float,struct<d:date>>, stringColumn map<float,struct<s:string>>, booleanColumn map<float,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map(1,named_struct('s',1)),map(4,named_struct('i',5)), " +
          "map(4,named_struct('b',789)), map(1,named_struct('d',2.3)), map(2,named_struct('d',23)), " +
          "map(2,named_struct('f',56)), map(2,named_struct('t','2017-04-01 12:00:00.0')), map(1,named_struct('d','2017-09-08'))," +
          "map(4,named_struct('s','abc')), map(1,named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as timestamp and value as struct") {
    val schema = "(smallintColumn map<timestamp,struct<s:short>>, intColumn map<timestamp,struct<i:int>>, bigintColumn map<timestamp,struct<b:bigint>>, " +
    "doubleColumn map<timestamp,struct<d:double>>, decimalColumn map<timestamp,struct<d:decimal(10,3)>>, floatColumn map<timestamp,struct<f:float>>,timestampColumn map<timestamp,struct<t:timestamp>>, " +
    "dateColumn map<timestamp,struct<d:date>>, stringColumn map<timestamp,struct<s:string>>, booleanColumn map<timestamp,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('s',1)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('i',5)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('b',789)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('d',2.3)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('d',23))," +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('f',56)), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('t','2017-04-01 12:00:00.0')), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('d','2017-09-08')), " +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('s','abc'))," +
          "map((cast(\"2017-04-01 12:00:00.0\" as timestamp)),named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as date and value as struct") {
    val schema = "(smallintColumn map<date,struct<s:short>>, intColumn map<date,struct<i:int>>, bigintColumn map<date,struct<b:bigint>>, " +
    "doubleColumn map<date,struct<d:double>>, decimalColumn map<date,struct<d:decimal(10,3)>>, floatColumn map<date,struct<f:float>>,timestampColumn map<date,struct<t:timestamp>>, " +
    "dateColumn map<date,struct<d:date>>, stringColumn map<date,struct<s:string>>, booleanColumn map<date,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
          "map((cast(\"2017-04-01\" as date)),named_struct('s',1)), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('i',5)), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('b',789)), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('d',2.3)), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('d',23))," +
          "map((cast(\"2017-04-01\" as date)),named_struct('f',56)), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('t','2017-04-01 12:00:00.0')), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('d','2017-09-08')), " +
          "map((cast(\"2017-04-01\" as date)),named_struct('s','abc'))," +
          "map((cast(\"2017-04-01\" as date)),named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as string and value as struct") {
    val schema = "(smallintColumn map<string,struct<s:short>>, intColumn map<string,struct<i:int>>, bigintColumn map<string,struct<b:bigint>>, " +
    "doubleColumn map<string,struct<d:double>>, decimalColumn map<string,struct<d:decimal(10,3)>>, floatColumn map<string,struct<f:float>>,timestampColumn map<string,struct<t:timestamp>>, " +
    "dateColumn map<string,struct<d:date>>, stringColumn map<string,struct<s:string>>, booleanColumn map<string,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
        "map('abcd',named_struct('s',1)),map('2017-04-01',named_struct('i',5)), " +
        "map('abcd',named_struct('b',789)), map('abcd',named_struct('d',2.3)), " +
        "map('abcd',named_struct('d',23)),map('abcd',named_struct('f',56)), " +
        "map('abcd',named_struct('t','2017-04-01 12:00:00.0')),map('abcd',named_struct('d','2017-09-08')), " +
        "map('abcd',named_struct('s','abc')), map('abcd',named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }

  test("test map of all primitive types with key as boolean and value as struct") {
    val schema = "(smallintColumn map<boolean,struct<s:short>>, intColumn map<boolean,struct<i:int>>, bigintColumn map<boolean,struct<b:bigint>>, " +
    "doubleColumn map<boolean,struct<d:double>>, decimalColumn map<boolean,struct<d:decimal(10,3)>>, floatColumn map<boolean,struct<f:float>>,timestampColumn map<boolean,struct<t:timestamp>>, " +
    "dateColumn map<boolean,struct<d:date>>, stringColumn map<boolean,struct<s:string>>, booleanColumn map<boolean,struct<b:boolean>>)"
    createTables(schema)
    def insertData(tableName: String) = {
      sql(s"insert into $tableName values(" +
        "map(true,named_struct('s',1)),map(true,named_struct('i',5)), " +
        "map(true,named_struct('b',789)), map(false,named_struct('d',2.3)), " +
        "map(false,named_struct('d',23)),map(false,named_struct('f',56)), " +
        "map(false,named_struct('t','2017-04-01 12:00:00.0')), map(true,named_struct('d','2017-09-08')), " +
        "map(false,named_struct('s','abc')), map(false,named_struct('b',true)))")
    }
    insertData("complextable")
    insertData("hivetable")
    insertData("fileformatTable")
    checkResults()
  }
}
