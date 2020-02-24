
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

package org.apache.carbondata.cluster.sdv.generated

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, File, InputStream}
import java.sql.Timestamp

import scala.collection.mutable

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.CarbonWriter

/**
 * Test Class for ComplexDataTypeTestCase to verify all scenerios
 */

class ComplexDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  val filePath = TestQueryExecutor.integrationPath + "/spark/src/test/resources"
  val writerPath =
    s"${ resourcesPath }" + "/SparkCarbonFileFormat/WriterOutputComplex/"

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File(writerPath))
    sql("DROP TABLE IF EXISTS complexcarbontable")
    sql("DROP TABLE IF EXISTS test")
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(writerPath))
    sql("DROP TABLE IF EXISTS complexcarbontable")
    sql("DROP TABLE IF EXISTS test")
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  // check create table with complex data type and insert into complex table
  test("test Complex_DataType-001") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(person struct<detail:struct<id:int,name:string,height:double," +
      "status:boolean,dob:date,dobt:timestamp>>) STORED AS carbondata")
    sql("insert into test values(named_struct('detail', named_struct('id', 1, 'name', 'abc', 'height', 4.30, 'status', true, 'dob', '2017-08-09', 'dobt', '2017-08-09 00:00:00.0')))")
    checkAnswer(sql("select * from test"),
      Seq(Row(Row(Row(1, "abc", 4.3, true, java.sql.Date.valueOf("2017-08-09"),
        Timestamp.valueOf("2017-08-09 00:00:00.0"))))))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(p1 array<int>,p2 array<string>,p3 array<double>,p4 array<boolean>,p5 " +
      "array<date>,p6 array<timestamp>) STORED AS carbondata")
    sql("insert into test values(array(1,2,3), array('abc','def','mno'), array(4.30,4.60,5.20), array(true,true,false), array('2017-08-09','2017-08-09','2017-07-07'), array('2017-08-09 00:00:00.0','2017-08-09 00:00:00.0','2017-07-07 00:00:00.0'))")
    checkAnswer(sql("select * from test"),
      Seq(Row(mutable.WrappedArray.make(Array(1, 2, 3)),
        mutable.WrappedArray.make(Array("abc", "def", "mno")),
        mutable.WrappedArray.make(Array(4.3, 4.6, 5.2)),
        mutable.WrappedArray.make(Array(true, true, false)),
        mutable.WrappedArray
          .make(Array(java.sql.Date.valueOf("2017-08-09"),
            java.sql.Date.valueOf("2017-08-09"),
            java.sql.Date.valueOf("2017-07-07"))),
        mutable.WrappedArray
          .make(Array(Timestamp.valueOf("2017-08-09 00:00:00.0"),
            Timestamp.valueOf("2017-08-09 00:00:00.0"),
            Timestamp.valueOf("2017-07-07 00:00:00.0"))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  // check create table with complex data type and load data into complex table
  test("test Complex_DataType-002") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata")
    sql(
      s"LOAD DATA local inpath '$filePath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    checkAnswer(sql("select * from complexcarbontable where deviceInformationId = 1"),
      Seq(Row(1, "109", "4ROM size", "29-11-2015", Row("1AA1", "2BB1"),
        mutable.WrappedArray.make(Array("MAC1", "MAC2", "MAC3")),
        mutable.WrappedArray
          .make(Array(Row(7, "Chinese", "Hubei Province", "yichang", "yichang", "yichang"),
            Row(7, "India", "New Delhi", "delhi", "delhi", "delhi"))),
        Row("29-11-2015", mutable.WrappedArray.make(Array("29-11-2015", "29-11-2015"))),
        109.0, 2738.562)))
  }

  // check create table with complex data type and insert into
  // into complex table
  test("test Complex_DataType-003") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(person struct<detail:struct<id:int,name:string,height:double," +
      "status:boolean,dob:date,dobt:timestamp>>) STORED AS carbondata ")
    sql("insert into test values(named_struct('detail', named_struct('id', 1, 'name', 'abc', 'height', 4.30, 'status', true, 'dob', '2017-08-09', 'dobt', '2017-08-09 00:00:00.0')))")
    checkAnswer(sql("select * from test"),
      Seq(Row(Row(Row(1,
        "abc", 4.3, true, java.sql.Date.valueOf("2017-08-09"),
        Timestamp.valueOf("2017-08-09 00:00:00.0"))))))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(p1 array<int>,p2 array<string>,p3 array<double>,p4 array<boolean>,p5 " +
      "array<date>,p6 array<timestamp>) STORED AS carbondata ")
    sql("insert into test values(array(1,2,3), array('abc','def','mno'), array(4.30,4.60,5.20), array(true,true,false), array('2017-08-09','2017-08-09','2017-07-07'), array('2017-08-09 00:00:00.0','2017-08-09 00:00:00.0','2017-07-07 00:00:00.0'))")
    checkAnswer(sql("select * from test"),
      Seq(Row(mutable.WrappedArray.make(Array(1, 2, 3)),
        mutable.WrappedArray.make(Array("abc", "def", "mno")),
        mutable.WrappedArray.make(Array(4.3, 4.6, 5.2)),
        mutable.WrappedArray.make(Array(true, true, false)),
        mutable.WrappedArray
          .make(Array(java.sql.Date.valueOf("2017-08-09"),
            java.sql.Date.valueOf("2017-08-09"),
            java.sql.Date.valueOf("2017-07-07"))),
        mutable.WrappedArray
          .make(Array(Timestamp.valueOf("2017-08-09 00:00:00.0"),
            Timestamp.valueOf("2017-08-09 00:00:00.0"),
            Timestamp.valueOf("2017-07-07 00:00:00.0"))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  // check ctas with complex datatype table
  test("test Complex_DataType-004") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata")
    sql(
      s"LOAD DATA local inpath '$filePath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    checkAnswer(sql("select count(*) from complexcarbontable"), Seq(Row(100)))
    sql("DROP TABLE IF EXISTS test")
    sql("create table test STORED AS carbondata as select * from complexcarbontable")
    checkAnswer(sql("select count(*) from test"), Seq(Row(100)))
  }

  //check projection pushdown with complex- STRUCT data type
  test("test Complex_DataType-005") {
    sql("DROP TABLE IF EXISTS complexcarbontable")
    sql(
      "create table complexcarbontable (roll int,a struct<b:int,c:string,d:int,e:string," +
      "f:struct<g:int,h:string,i:int>,j:int>) " +
      "STORED AS carbondata")
    sql("insert into complexcarbontable values(1, named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into complexcarbontable values(2, named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into complexcarbontable values(3, named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    checkAnswer(sql("select a.b from complexcarbontable"), Seq(Row(1), Row(1), Row(1)))
    checkAnswer(sql("select a.c from complexcarbontable"), Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select a.d from complexcarbontable"), Seq(Row(2), Row(2), Row(2)))
    checkAnswer(sql("select a.e from complexcarbontable"), Seq(Row("efg"), Row("efg"), Row("efg")))
    checkAnswer(sql("select a.f from complexcarbontable"),
      Seq(Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4))))
    checkAnswer(sql("select a.f.g  from complexcarbontable"), Seq(Row(3), Row(3), Row(3)))
    checkAnswer(sql("select a.f.h  from complexcarbontable"),
      Seq(Row("mno"), Row("mno"), Row("mno")))
    checkAnswer(sql("select a.f.i  from complexcarbontable"), Seq(Row(4), Row(4), Row(4)))
    checkAnswer(sql("select a.f.g,a.f.h,a.f.i  from complexcarbontable"),
      Seq(Row(3, "mno", 4), Row(3, "mno", 4), Row(3, "mno", 4)))
    checkAnswer(sql("select a.b,a.f from complexcarbontable"),
      Seq(Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4))))
    checkAnswer(sql("select a.c,a.f from complexcarbontable"),
      Seq(Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4))))
    checkAnswer(sql("select a.d,a.f from complexcarbontable"),
      Seq(Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4))))
    checkAnswer(sql("select a.j from complexcarbontable"), Seq(Row(5), Row(5), Row(5)))
    checkAnswer(sql("select * from complexcarbontable"),
      Seq(Row(1, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
    checkAnswer(sql("select *,a from complexcarbontable"),
      Seq(Row(1,
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
  }

  // check create table with complex datatype columns and insert into table and apply filters
  test("test Complex_DataType-006") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>) STORED AS carbondata")
    sql("insert into test values(1, named_struct('b', 2, 'c', 3))")
    sql("insert into test values(3, named_struct('b', 5, 'c', 3))")
    sql("insert into test values(2, named_struct('b', 4, 'c', 5))")
    checkAnswer(sql("select a.b from test where id=3"), Seq(Row(5)))
    checkAnswer(sql("select a.b from test where a.c!=3"), Seq(Row(4)))
    checkAnswer(sql("select a.b from test where a.c=3"), Seq(Row(5), Row(2)))
    checkAnswer(sql("select a.b from test where id=1 or !a.c=3"), Seq(Row(4), Row(2)))
    checkAnswer(sql("select a.b from test where id=3 or a.c=3"), Seq(Row(5), Row(2)))
  }

  // check create table with complex datatype columns and perform insertoverwrite
  test("test Complex_DataType-007") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata")
    sql(
      s"LOAD DATA local inpath '$filePath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    checkAnswer(sql("select count(*) from complexcarbontable"), Seq(Row(100)))
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata")
    sql("insert overwrite table test select * from complexcarbontable")
    checkAnswer(sql("select count(*) from test"), Seq(Row(100)))
  }

  // check create complex table and insert null values
  test("test Complex_DataType-008") {
    sql("drop table if exists complexcarbontable")
    sql(
      "create table complexcarbontable(roll int, student struct<id:int,name:string," +
      "marks:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into complexcarbontable values(1, named_struct('id', 1, 'name', 'abc', 'marks', array(1,null,null)))")
    checkAnswer(sql("select * from complexcarbontable"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(1, null, null))))))
  }

  //check create table with complex double and insert bigger value and check
  test("test Complex_DataType-009") {
    sql("Drop table if exists complexcarbontable")
    sql(
      "create table complexcarbontable(struct_dbl struct<double1:double,double2:double," +
      "double3:double>) " +
      "STORED AS carbondata")
    sql("insert into complexcarbontable values(named_struct('double1', 10000000, 'double2', 300000, 'double3', 3000))")
    checkExistence(sql("select * from complexcarbontable"), true, "1.0E7,300000.0,3000.0")
    sql("Drop table if exists complexcarbontable")
    sql(
      "create table complexcarbontable(struct_arr struct<array_db1:array<double>>) " +
      "STORED AS carbondata")
    sql("insert into complexcarbontable values(named_struct('array_db1', array(5555555.9559,12345678991234567,3444.999)))")
    checkExistence(sql("select * from complexcarbontable"),
      true,
      "5555555.9559, 1.2345678991234568E16, 3444.999")
  }

  // check create table with complex data type through SDK
  test("test Complex_DataType-010") {
    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |   "name" :"my_address",
        |   "type" :{
        |							"name": "my_address",
        |							"type": "record",
        |							"fields": [
        |               {
        |									"name": "Temperaturetest",
        |									"type": "double"
        |								}
        |							]
        |       }
        |			}
        |	]
        |} """.stripMargin

    val jsonvalue =
      """{
        |"name" :"abcde",
        |"age" :34,
        |"my_address" :{ "Temperaturetest" :100 }
        |}
      """.stripMargin
    val pschema = org.apache.avro.Schema.parse(mySchema)
    val records = jsonToAvro(jsonvalue, mySchema)
    val writer = CarbonWriter.builder().outputPath(writerPath).withAvroInput(pschema)
      .writtenBy("ComplexDataTypeTestCase").build()
    writer.write(records)
    writer.close()
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("abcde", 34, Row(100.0))))
  }

  def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
    var input: InputStream = null
    var writer: DataFileWriter[GenericRecord] = null
    var output: ByteArrayOutputStream = null
    try {
      val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      input = new ByteArrayInputStream(json.getBytes())
      output = new ByteArrayOutputStream()
      val din = new DataInputStream(input)
      writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
      writer.create(schema, output)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      var datum: GenericRecord = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }
}
