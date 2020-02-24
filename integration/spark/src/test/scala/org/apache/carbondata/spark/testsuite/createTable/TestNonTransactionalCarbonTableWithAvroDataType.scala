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
import java.nio.ByteBuffer
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.GenericData
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

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath

  writerPath = writerPath.replace("\\", "/")

  val decimalConversion = new DecimalConversion

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "force")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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
      val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    sql("describe formatted sdkOutputTable").show(false)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(Row(null, 1)))))
    sql("insert into sdkOutputTable values(named_struct('city', named_struct('city0', 'abc', 'city1', 12)))")
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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
        |		  "name": "union_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "dec",
        |          "type": "bytes",
        |         "logicalType": "decimal",
        |                     "precision": 10,
        |                     "scale": 2
        |        },"null"]
        |	}]
        |}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val decimalConversion = new DecimalConversion
    val logicalType = LogicalTypes.decimal(10, 2)
    val decimal = new java.math.BigDecimal("1010").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("union_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"union_field":{"bytes":"$data"}}""".stripMargin
    val record = testUtil.jsonToAvro(json1, schema1)
    val data1 = new String(record.get(0).asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytes1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(data1))
    val avroRec = new GenericData. Record(nn)
    avroRec.put("union_field", bytes1)


    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkExistence(sql("select * from sdkOutputTable"), true, "1010.00")
  }

  test("test logical type decimal with struct") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """
        |{"name": "struct_field",
        | "type": "record",
        | "fields": [
        |  { "name": "record1", "type": "string"},
        |  { "name": "record2", "type": "float"},
        |  { "name": "struct_field_decimal",  "type": {
        |    "type" : "record",  "name" : "my_record",
        |        "fields" : [
        |    {"name": "record3", "type": "string"},
        |    {"name": "record4", "type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 4,
        |                     "scale": 2
        |                    }}]}}
        |]}
      """.stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)

    val logicalType = LogicalTypes.decimal(4, 2)
    val decimal1 = new java.math.BigDecimal("32").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal1, nn.getField("struct_field_decimal").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 = s""" {"record1":"bob", "record2":10.24, "struct_field_decimal" : {"record3":"abc", "record4":"$data"}} """
    val record = testUtil.jsonToAvro(json1, schema1)

    val jsonData = new String(record.get(2).asInstanceOf[GenericData.Record].get(1)
      .asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytesValue = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(jsonData))
    val mySchema =
      """
        |{"name": "struct_field_decimal",
        | "type": "record",
        | "fields": [
        |  { "name": "record3", "type": "string"},
        |  { "name": "record4", "type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 4,
        |                     "scale": 2
        |                    }}
        |]}
      """.stripMargin
    val schema = new org.apache.avro.Schema.Parser().parse(mySchema)
    val genericByteArray = new GenericData.Record(schema)
    genericByteArray.put("record3", "abc")
    genericByteArray.put("record4", bytesValue)
    val avroRec = new GenericData.Record(nn)
    avroRec.put("record1", "bob")
    avroRec.put("record2", 10.24)
    avroRec.put("struct_field_decimal", genericByteArray)

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
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
        |      "name": "dec_fields",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "dec_field",
        |      "type": "bytes",
        |      "logicalType": "decimal",
        |      "precision": 4,
        |      "scale": 1
        |      }}}]
        |  }
      """.stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(4, 1)
    val decimal1 = new java.math.BigDecimal("32").setScale(1)
    val decimal2 = new java.math.BigDecimal("42").setScale(1)
    //get unscaled 2's complement bytearray
    val bytes1 =
      decimalConversion.toBytes(decimal1, nn.getField("dec_fields").schema, logicalType)
    val bytes2 =
      decimalConversion.toBytes(decimal2, nn.getField("dec_fields").schema, logicalType)
    val data1 = DatatypeConverter.printBase64Binary(bytes1.array())
    val data2 = DatatypeConverter.printBase64Binary(bytes2.array())
    val json1: String = s""" {"name": "bob","age": 10,"dec_fields":["$data1","$data2"]} """
    val record = testUtil.jsonToAvro(json1, schema1)

    val jsonData1 = new String(record.get(2).asInstanceOf[GenericData.Array[ByteBuffer]].get(0)
      .array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val jsonData2 = new String(record.get(2).asInstanceOf[GenericData.Array[ByteBuffer]].get(1)
      .array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytesValue1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(jsonData1))
    val bytesValue2 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(jsonData2))
    val genericByteArray = new GenericData.Array[ByteBuffer](2,
      Schema.createArray(Schema.create(Schema.Type.BYTES)))
    genericByteArray.add(bytesValue1)
    genericByteArray.add(bytesValue2)
    val avroRec = new GenericData.Record(nn)
    avroRec.put("name", "bob")
    avroRec.put("age", 10)
    avroRec.put("dec_fields", genericByteArray)

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkExistence(sql("select * from sdkOutputTable"), true, "32.0")
  }

  test("test logical type time-millis") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "int", "logicalType": "time-millis"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "int", "logicalType": "time-millis"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 172800,"course_details": { "course_struct_course_time":172800}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)


    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(172800, Row(172800))))
  }

  test("test logical type time-micros") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "long", "logicalType": "time-micros"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "long", "logicalType": "time-micros"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 1728000,"course_details": { "course_struct_course_time":1728000}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)


    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(1728000, Row(1728000))))
  }

  test("test logical type decimal through Json") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 5,
        |                     "scale": 2
        |                    }
        |}
        |	]
        |}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(5, 2)
    val decimal = new java.math.BigDecimal("12.8").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("id").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"id":"$data"}""".stripMargin
    val record = testUtil.jsonToAvro(json1, schema1)
    val data1 = new String(record.get(0).asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytes1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(data1))
    val avroRec = new GenericData. Record(nn)
    avroRec.put("id", bytes1)
    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(decimal)))
  }

  test("test logical type decimal through Json with big decimal value") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "dec_field",
        |						"type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 30,
        |                     "scale": 10
        |                    }
        |}
        |	]
        |}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(30, 10)
    val decimal = new java.math.BigDecimal("12672346879023.845789").setScale(10)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("dec_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"dec_field":"$data"}""".stripMargin
    val record = testUtil.jsonToAvro(json1, schema1)
    val data1 = new String(record.get(0).asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytes1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(data1))
    val avroRec = new GenericData. Record(nn)
    avroRec.put("dec_field", bytes1)
    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(decimal)))
  }

  test("test logical type decimal through Json with negative decimal value") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "dec_field",
        |						"type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 30,
        |                     "scale": 6
        |                    }
        |}
        |	]
        |}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(30, 6)
    val decimal = new java.math.BigDecimal("-12672346879023.845").setScale(6)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("dec_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"dec_field":"$data"}""".stripMargin
    val record = testUtil.jsonToAvro(json1, schema1)
    val data1 = new String(record.get(0).asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytes1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(data1))
    val avroRec = new GenericData. Record(nn)
    avroRec.put("dec_field", bytes1)
    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(decimal)))
  }

  test("test logical type decimal through Avro") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "dec_field",
        |						"type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 5,
        |                     "scale": 2
        |                    }
        |}
        |	]
        |}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(5, 2)
    val decimal = new java.math.BigDecimal("12.8").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("dec_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"dec_field":"$data"}""".stripMargin
    val avroRec = new GenericData. Record(nn)
    avroRec.put("dec_field", bytes)
    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(decimal)))
  }

  test("test logical type decimal with data having greater precision than specified precision") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "dec_field",
        |						"type": {"type" : "bytes",
        |                     "logicalType": "decimal",
        |                     "precision": 5,
        |                     "scale": 2
        |                    }
        |}
        |	]
        |}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val logicalType = LogicalTypes.decimal(5, 2)
    val decimal = new java.math.BigDecimal("1218").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("dec_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"dec_field":"$data"}""".stripMargin
    val avroRec = new GenericData. Record(nn)
    avroRec.put("dec_field", bytes)
    val exception1 = intercept[Exception] {
    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    }
    assert(exception1.getMessage
      .contains("Data Loading failed as value Precision 6 is greater than specified Precision 5 in Avro Schema"))
  }

  test("test union with multiple record type") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "test.avro",
        |	"type": "record",
        |	"name": "NewCar2",
        |	"fields": [
        |      {
        |		  "name": "optionalExtra",
        |    "type": ["null",{
        |       "type":"record",
        |       "name":"Stereo",
        |       "fields" :[{
        |       "name":"make",
        |       "type":"string"
        |       },
        |       {
        |       "name":"speakers",
        |       "type":"int"
        |       }]
        |       },{
        |       "type":"record",
        |       "name":"LeatherTrim",
        |       "fields":[{
        |       "name":"colour",
        |       "type":"string"
        |       }]
        |       }],
        |       "default":null
        |       }]
        |
        |}""".stripMargin

    val json1 =
      """{"optionalExtra":{"test.avro.LeatherTrim":{"colour":"ab"}}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(Row(null,null),Row("ab")))))
  }

  test("test union with multiple Enum type") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "test.avro",
        |	"type": "record",
        |	"name": "Union_data3",
        |	"fields": [
        |      {
        |		  "name": "enum_record",
        |    "type":
        |    ["long","null","string",
        |    {"type":"enum","name":"t1","symbols":["red","blue","yellow"]},
        |    {"type":"enum","name":"t2","symbols":["sun","mon","tue","wed","thu","fri","sat"]},
        |    "int"
        |    ]}]
        |}""".stripMargin

    val json1 =
      """{"enum_record":{"test.avro.t2":"sun"}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         |LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(null,null,null,"sun",null))))
  }

  test("test spark file format") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |      {
        |		  "name": "union_field", "type": [{
        |          "namespace": "org.example.avro",
        |          "name": "dec",
        |          "type": "bytes",
        |         "logicalType": "decimal",
        |                     "precision": 10,
        |                     "scale": 2
        |        },"int"]
        |	}]
        |}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val decimalConversion = new DecimalConversion
    val logicalType = LogicalTypes.decimal(10, 2)
    val decimal = new java.math.BigDecimal("1010").setScale(2)
    //get unscaled 2's complement bytearray
    val bytes =
      decimalConversion.toBytes(decimal, nn.getField("union_field").schema, logicalType)
    val data = DatatypeConverter.printBase64Binary(bytes.array())
    val json1 =
      s"""{"union_field":{"bytes":"$data"}}""".stripMargin
    val record = testUtil.jsonToAvro(json1, schema1)
    val data1 = new String(record.get(0).asInstanceOf[ByteBuffer].array(),
      CarbonCommonConstants.DEFAULT_CHARSET_CLASS)
    val bytes1 = ByteBuffer.wrap(DatatypeConverter.parseBase64Binary(data1))
    val avroRec = new GenericData. Record(nn)
    avroRec.put("union_field", bytes1)


    val writer = CarbonWriter.builder.outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithAvroDataType").build()
    writer.write(avroRec)
    writer.close()
    sql(s"create table sdkOutputTable(union_field struct<union_field0:decimal(10,2),union_field1:int>) " +
        s"using carbon options(path='$writerPath')")
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Row(decimal,null))))
  }

}
