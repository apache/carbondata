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

package org.apache.carbondata.spark.testsuite.createTable

import java.io.File
import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.CarbonWriter

/**
 * Test class for Avro supported data types through SDK
 */
class TestNonTransactionalCarbonTableWithAvroDataType extends QueryTest with BeforeAndAfterAll {


  val badRecordAction = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION)

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath

  writerPath = writerPath.replace("\\", "/")

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "force")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, badRecordAction)
  }

  test("test enum") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        | "fields":
        | [{
        | "name": "id",
        |						"type": {
        |                    "type": "enum",
        |                    "name": "Suit",
        |                    "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |                    }
        |                }
        |            ]
        |}""".stripMargin

    val json1 =
      """{"id":"HEARTS"}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("HEARTS")))
  }

  test("test enum with struct type") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val mySchema =
      "{ " +
      "  \"name\": \"address\", " +
      "  \"type\": \"record\", " +
      "  \"fields\": [ " +
      "    { " +
      "      \"name\": \"name\", " +
      "      \"type\": \"string\" " +
      "    }, " +
      "    { " +
      "      \"name\": \"age\", " +
      "      \"type\": \"int\" " +
      "    }, " +
      "    { " +
      "       \"name\": \"address\",  \"type\": {" +
      "        \"type\" : \"record\",  \"name\" : \"my_address\"," +
      "         \"fields\" : [" +
      "         {\"name\": \"enumRec\", " +
      "           \"type\": { " +
      "            \"type\": \"enum\", " +
      "             \"name\": \"card\", " +
      "             \"symbols\": [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"] " +
      "      } " +
      "}]}" +
      "    } " +
      "  ] " +
      "} "

    val json1 = "{\"name\":\"bob\", \"age\":10, \"address\": {\"enumRec\":\"SPADES\"}}"

    val nn = new org.apache.avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json1, mySchema)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("bob", 10, Row("SPADES"))))
  }

  test("test enum with Array type") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val mySchema =
      """ {
        |      "name": "address",
        |      "type": "record",
        |      "fields": [
        |      {
        |      "name": "name",
        |      "type": "string"
        |      },
        |      {
        |      "name": "age",
        |      "type": "int"
        |      },
        |      {
        |      "name": "address",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |                    "name": "Suit",
        |                    "type": "enum",
        |                    "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |      }}}]
        |  }
      """.stripMargin

    val json: String = """ {"name": "bob","age": 10,"address": ["SPADES", "DIAMONDS"]} """

    val nn = new org.apache.avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json, mySchema)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row("bob", 10, mutable.WrappedArray.make(Array("SPADES", "DIAMONDS")))))
  }

  test("test union type long") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["string", "int", "long"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"long":10345}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(null, null, 10345))))
  }

  test("test union type boolean") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["boolean", "int", "long"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"boolean":true}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(true, null, null))))
  }

  test("test union type string") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["string", "int", "long"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"string":"abc"}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row("abc", null, null))))
  }

  test("test union type int") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["string", "int", "long"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"int":10}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(null, 10, null))))
  }

  test("test union type with null") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["null", "int"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"null":null}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(null))))
  }

  test("test union type with only type null") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |       { "name": "first", "type": ["null"] }
        |     ]
        |}""".stripMargin

    val json1 =
      """{"first":{"null":null}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)
    val exception1 = intercept[UnsupportedOperationException] {
      val writer = CarbonWriter.builder
        .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
      writer.write(record)
      writer.close()
    }
    assert(exception1.getMessage
      .contains("Carbon do not support Avro UNION with only null type"))
  }

  test("test union type with Enum") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |      {
        |		  "name": "enum_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "EnumField",
        |          "type": "enum",
        |          "symbols": [
        |				"VAL_0",
        |				"VAL_1"
        |			]
        |        },"null"], "default": null
        |	}]
        |}""".stripMargin

    val json1 =
      """{"enum_field":{"org.example.avro.EnumField":"VAL_0"}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row("VAL_0"))))
  }

  test("test union type with Map") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |      {
        |		  "name": "map_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "mapField",
        |          "type": "map",
        |          "values":"string"
        |        },"int"], "default": null
        |	}]
        |}""".stripMargin

    val json1 =
      """{"map_field":{"map":{"street": "k-lane", "city": "bangalore"}}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql("select * from sdkOutputTable").show(false)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row(Row(Map("city" -> "bangalore", "street" -> "k-lane"), null))))
  }

  test("test union type") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |      {
        |		  "name": "struct_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "structField",
        |          "type": "array",
        |          "items": { "name" : "name0", "type":"string"}
        |        },"int"], "default": null
        |	}]
        |}""".stripMargin

    val json1 =
      """{"struct_field":{"int":12}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(null)), 12))))
  }

  test("test Struct of Union") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """
        |{"name": "address",
        | "type": "record",
        | "fields": [
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "city", "type": ["string","int"]}]}}
        |]}
      """.stripMargin

    val json1 =
      """{"address":{"city":{"int":1}}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql("describe formatted sdkOutputTable").show(false)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(Row(null, 1)))))
    sql("insert into sdkOutputTable values('abc:12')")
    sql("select address.city.city0 from sdkOutputTable").show(false)
  }

  test("test Union with struct of array") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """
        |{"name": "address",
        | "type": "record",
        | "fields": [
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "city", "type": ["string",  {
        |                "type": "array",
        |                "name": "abc_name_0",
        |                "items": {
        |                  "name": "_name_0",
        |                  "type": "record",
        |                  "fields": [
        |                    {
        |                      "name": "app_id",
        |                      "type": [
        |                        "null",
        |                        "string"
        |                      ]
        |                    }
        |                    ]
        |                    }
        |                    }
        |                    ]}]}}
        |]}
      """.stripMargin

    val json1 =
      """{
        |"address":{"city":
        |{"array":[
        |        {
        |          "app_id": {
        |            "string": "abc"
        |          }}]
        |          }}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(Row(Row(null, mutable.WrappedArray.make(Array(Row(Row("abc")))))))))
  }

  test("test union type with Array and Struct") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |  "type": "record",
        |  "namespace": "example.avro",
        |  "name": "array_union",
        |  "fields": [
        |    {
        |      "name": "body",
        |      "type": {
        |        "name": "body",
        |        "type": "record",
        |        "fields": [
        |          {
        |            "name": "abc",
        |            "type": [
        |              "int",
        |              {
        |                "type": "array",
        |                "name": "abc_name_0",
        |                "items": {
        |                  "name": "_name_0",
        |                  "type": "record",
        |                  "fields": [
        |                    {
        |                      "name": "app_id",
        |                      "type": [
        |                        "null",
        |                        "string"
        |                      ]
        |                    },
        |                    {
        |                      "name": "app_name",
        |                      "type": [
        |                        "int",
        |                        "float",
        |                        "string"
        |                      ]
        |                    },
        |                    {
        |                      "name": "app_key",
        |                      "type": [
        |                        "null",
        |                        "string"
        |                      ]
        |                    }
        |                  ]
        |                }
        |              }
        |            ]
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

    val json1 =
      """{
        |  "body": {
        |    "abc": {
        |      "array": [
        |        {
        |          "app_id": {
        |            "string": "abc"
        |          },
        |          "app_name": {
        |            "string": "bcd"
        |          },
        |          "app_key": {
        |            "string": "cde"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql("describe formatted sdkOutputTable").show(false)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(Row(Row(null,
        mutable.WrappedArray.make(Array(Row(Row("abc"), Row(null, null, "bcd"), Row("cde")))))))))
  }

  test("test union type with Decimal") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |      {
        |		  "name": "enum_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "dec",
        |          "type": "bytes",
        |         "logicalType": "decimal",
        |                     "precision": 10,
        |                     "scale": 2
        |        },"null"]
        |	}]
        |}""".stripMargin

    val json1 =
      """{"enum_field":{"bytes":"1010"}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkExistence(sql("select * from sdkOutputTable"), true, "1010.00")
  }

  test("test logical type decimal with struct") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """
        |{"name": "address",
        | "type": "record",
        | "fields": [
        |  { "name": "name", "type": "string"},
        |  { "name": "age", "type": "float"},
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "street", "type": "string"},
        |    {"name": "city", "type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 4,
        |                     "scale": 2
        |                    }}]}}
        |]}
      """.stripMargin

    val json1 = """ {"name":"bob", "age":10.24, "address" : {"street":"abc", "city":"32"}} """

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkExistence(sql("select * from sdkOutputTable"), true, "32.00")
  }

  test("test logical type decimal with Array") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """ {
        |      "name": "address",
        |      "type": "record",
        |      "fields": [
        |      {
        |      "name": "name",
        |      "type": "string"
        |      },
        |      {
        |      "name": "age",
        |      "type": "int"
        |      },
        |      {
        |      "name": "address",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "bytes",
        |      "logicalType": "decimal",
        |      "precision": 4,
        |      "scale": 1
        |      }}}]
        |  }
      """.stripMargin

    val json1: String = """ {"name": "bob","age": 10,"address": ["32", "42"]} """

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput(nn)
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkExistence(sql("select * from sdkOutputTable"), true, "32.0")
  }

}
