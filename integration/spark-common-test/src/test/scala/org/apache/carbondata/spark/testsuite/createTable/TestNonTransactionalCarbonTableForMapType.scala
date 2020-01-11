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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.sdk.file.CarbonReader

/**
 * test cases for SDK complex map data type support
 */
class TestNonTransactionalCarbonTableForMapType extends QueryTest with BeforeAndAfterAll {

  private val conf: Configuration = new Configuration(false)

  private val nonTransactionalCarbonTable = new TestNonTransactionalCarbonTable
  private val writerPath = nonTransactionalCarbonTable.writerPath

  private def deleteDirectory(path: String): Unit = {
    FileUtils.deleteDirectory(new File(path))
  }

  def buildMapSchema(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "mapRecord",
        |      "type": {
        |        "type": "map",
        |        "values": "string"
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json =
      """ {"name":"bob", "age":10, "mapRecord": {"street": "k-lane", "city": "bangalore"}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildMapSchemaWith2Levels(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "mapRecord",
        |      "type": {
        |        "type": "map",
        |        "values": {
        |            "type": "map",
        |            "values": "string"
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "mapRecord": {"details": {"street": "k-lane", "city": "bangalore"}}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildMapSchemaWith6Levels(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "mapRecord",
        |      "type": {
        |        "type": "map",
        |        "values": {
        |          "type": "map",
        |          "values": {
        |            "type": "map",
        |            "values": {
        |              "type": "map",
        |              "values": {
        |                "type": "map",
        |                "values": {
        |                  "type": "map",
        |                  "values": {
        |                    "type": "map",
        |                    "values": "string"
        |                  }
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json =
      """ {"name":"bob", "age":10, "mapRecord": {"topLevel": {"level1": {"level2": {"level3":
        |{"level4": {"level5": {"street": "k-lane", "city": "bangalore"}}}}}}}} """
        .stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildMapSchemaWithArrayTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        {
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "mapRecord",
        |      "type": {
        |        "type": "map",
        |        "values": {
        |          "type": "array",
        |          "items": {
        |            "name": "street",
        |            "type": "string"
        |          }
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "mapRecord": {"city": ["city1","city2"]}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildMapSchemaWithStructTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "mapRecord",
        |      "type": {
        |        "type": "map",
        |        "values": {
        |          "type": "record",
        |          "name": "my_address",
        |          "fields": [
        |            {
        |              "name": "street",
        |              "type": "string"
        |            },
        |            {
        |              "name": "city",
        |              "type": "string"
        |            }
        |          ]
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "mapRecord": {"details": {"street":"street1", "city":"bang"}}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildStructSchemaWithMapTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "structRecord",
        |      "type": {
        |        "type": "record",
        |        "name": "my_address",
        |        "fields": [
        |          {
        |            "name": "street",
        |            "type": "string"
        |          },
        |          {
        |            "name": "houseDetails",
        |            "type": {
        |               "type": "map",
        |               "values": "string"
        |             }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "structRecord": {"street":"street1", "houseDetails": {"101": "Rahul", "102": "Pawan"}}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildStructSchemaWithNestedArrayOfMapTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "structRecord",
        |      "type": {
        |        "type": "record",
        |        "name": "my_address",
        |        "fields": [
        |          {
        |            "name": "street",
        |            "type": "string"
        |          },
        |          {
        |            "name": "houseDetails",
        |            "type": {
        |               "type": "array",
        |               "items": {
        |                   "name": "memberDetails",
        |                   "type": "map",
        |                   "values": "string"
        |                }
        |             }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "structRecord": {"street":"street1", "houseDetails": [{"101": "Rahul", "102": "Pawan"}]}} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildArraySchemaWithMapTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "arrayRecord",
        |      "type": {
        |        "type": "array",
        |        "items": {
        |           "name": "houseDetails",
        |           "type": "map",
        |           "values": "string"
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "arrayRecord": [{"101": "Rahul", "102": "Pawan"}]} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildArraySchemaWithNestedArrayOfMapTypeAsValue(rows: Int): Unit = {
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "arrayRecord",
        |      "type": {
        |        "type": "array",
        |        "items": {
        |           "name": "FloorNum",
        |           "type": "array",
        |           "items": {
        |             "name": "houseDetails",
        |             "type": "map",
        |             "values": "string"
        |           }
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "arrayRecord": [[{"101": "Rahul", "102": "Pawan"}]]} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  private def dropSchema: Unit = {
    deleteDirectory(writerPath)
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
  }

  override def beforeAll(): Unit = {
    dropSchema
  }

  test("SDK Reader Without Projection Columns"){
    deleteDirectory(writerPath)
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "arrayRecord",
        |      "type": {
        |        "type": "array",
        |        "items": {
        |           "name": "houseDetails",
        |           "type": "map",
        |           "values": "string"
        |        }
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json = """ {"name":"bob", "age":10, "arrayRecord": [{"101": "Rahul", "102": "Pawan"}]} """.stripMargin
    nonTransactionalCarbonTable.WriteFilesWithAvroWriter(2, mySchema, json)

    val reader = CarbonReader.builder(writerPath, "_temp").build()
    reader.close()
    val exception1 = intercept[Exception] {
      val reader1 = CarbonReader.builder(writerPath, "_temp")
        .projection(Array[String] { "arrayRecord.houseDetails" })
        .build()
      reader1.close()
    }
    assert(exception1.getMessage
      .contains(
        "Complex child columns projection NOT supported through CarbonReader"))
    println("Done test")
  }


  test("Read sdk writer Avro output Map Type") {
    buildMapSchema(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select count(*) from sdkMapOutputTable where maprecord['street']='abc'"),
      Row(0))
    checkAnswer(sql("select count(*) from sdkMapOutputTable where maprecord['street']='k-lane'"),
      Row(3))
    checkAnswer(sql("select  maprecord['street'] from sdkMapOutputTable"),
      Seq(Row("k-lane"), Row("k-lane"), Row("k-lane")))
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Map("city" -> "bangalore", "street" -> "k-lane")),
      Row("bob", 10, Map("city" -> "bangalore", "street" -> "k-lane")),
      Row("bob", 10, Map("city" -> "bangalore", "street" -> "k-lane"))))
  }

  test("Read sdk writer Avro output Map Type with nested 2 levels") {
    buildMapSchemaWith2Levels(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select maprecord['details'] from sdkMapOutputTable"), Seq(
      Row(Map("city" -> "bangalore", "street" -> "k-lane")),
      Row(Map("city" -> "bangalore", "street" -> "k-lane")),
      Row(Map("city" -> "bangalore", "street" -> "k-lane"))))
  }

  test("Read sdk writer Avro output Map Type with nested 6 levels") {
    buildMapSchemaWith6Levels(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select count(*) from sdkMapOutputTable"), Row(3))
  }

  test("Read sdk writer Avro output Map Type with array type as value") {
    buildMapSchemaWithArrayTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Map("city" -> Seq("city1", "city2"))),
      Row("bob", 10, Map("city" -> Seq("city1", "city2"))),
      Row("bob", 10, Map("city" -> Seq("city1", "city2")))))
  }

  test("Read sdk writer Avro output Map Type with struct type as value") {
    buildMapSchemaWithStructTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Map("details" -> Row("street1", "bang"))),
      Row("bob", 10, Map("details" -> Row("street1", "bang"))),
      Row("bob", 10, Map("details" -> Row("street1", "bang")))))
  }

  test("Read sdk writer Avro output Map Type with map type as child to struct type") {
    buildStructSchemaWithMapTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Row("street1", Map("101" -> "Rahul", "102" -> "Pawan"))),
      Row("bob", 10, Row("street1", Map("101" -> "Rahul", "102" -> "Pawan"))),
      Row("bob", 10, Row("street1", Map("101" -> "Rahul", "102" -> "Pawan")))))
  }

  test("Read sdk writer Avro output Map Type with map type as child to struct<array> type") {
    buildStructSchemaWithNestedArrayOfMapTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    sql("desc formatted sdkMapOutputTable").show(1000, false)
    sql("select * from sdkMapOutputTable").show(false)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Row("street1", Seq(Map("101" -> "Rahul", "102" -> "Pawan")))),
      Row("bob", 10, Row("street1", Seq(Map("101" -> "Rahul", "102" -> "Pawan")))),
      Row("bob", 10, Row("street1", Seq(Map("101" -> "Rahul", "102" -> "Pawan"))))))
  }

  test("Read sdk writer Avro output Map Type with map type as child to array type") {
    buildArraySchemaWithMapTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Seq(Map("101" -> "Rahul", "102" -> "Pawan"))),
      Row("bob", 10, Seq(Map("101" -> "Rahul", "102" -> "Pawan"))),
      Row("bob", 10, Seq(Map("101" -> "Rahul", "102" -> "Pawan")))))
  }

  test("Read sdk writer Avro output Map Type with map type as child to array<array> type") {
    buildArraySchemaWithNestedArrayOfMapTypeAsValue(3)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkMapOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkMapOutputTable STORED AS carbondata LOCATION
          |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkMapOutputTable"), Seq(
      Row("bob", 10, Seq(Seq(Map("101" -> "Rahul", "102" -> "Pawan")))),
      Row("bob", 10, Seq(Seq(Map("101" -> "Rahul", "102" -> "Pawan")))),
      Row("bob", 10, Seq(Seq(Map("101" -> "Rahul", "102" -> "Pawan"))))))
  }

  override def afterAll(): Unit = {
    dropSchema
  }

}
