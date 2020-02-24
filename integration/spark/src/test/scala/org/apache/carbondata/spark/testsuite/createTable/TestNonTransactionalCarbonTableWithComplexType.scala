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
import java.util

import org.apache.avro
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.CharEncoding
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.CarbonWriter

class TestNonTransactionalCarbonTableWithComplexType extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")


  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
  }

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  private def WriteFilesWithAvroWriter(rows: Int, mySchema: String, json: String, isLocalDictionary: Boolean): Unit = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json, mySchema)
    try {
      val writer = if (isLocalDictionary) {
        CarbonWriter.builder
          .outputPath(writerPath).enableLocalDictionary(true)
          .localDictionaryThreshold(2000)
          .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithComplexType").build()
      } else {
        CarbonWriter.builder
          .outputPath(writerPath)
          .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTableWithComplexType").build()
      }
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  // test multi level -- 4 levels [array of array of array of struct]
  def buildAvroTestDataMultiLevel4(rows: Int, options: util.Map[String, String], isLocalDictionary: Boolean): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =  """ {
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
                      |			"name": "BuildNum",
                      |			"type": {
                      |				"type": "array",
                      |				"items": {
                      |					"name": "FloorNum",
                      |					"type": "array",
                      |					"items": {
                      |						"name": "doorNum",
                      |						"type": "array",
                      |						"items": {
                      |							"name": "my_address",
                      |							"type": "record",
                      |							"fields": [
                      |								{
                      |									"name": "street",
                      |									"type": "string"
                      |								},
                      |								{
                      |									"name": "city",
                      |									"type": "string"
                      |								},
                      |               {
                      |									"name": "Temperature",
                      |									"type": "double"
                      |								},
                      |               {
                      |									"name": "WindSpeed",
                      |									"type": "string"
                      |								},
                      |               {
                      |									"name": "year",
                      |									"type": "string"
                      |								}
                      |							]
                      |						}
                      |					}
                      |				}
                      |			}
                      |		}
                      |	]
                      |} """.stripMargin

    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"BuildNum": [
        |		[
        |			[
        |				{"street":"abc", "city":"city1", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				{"street":"def", "city":"city2", "Temperature":13.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				{"street":"cfg", "city":"city3", "Temperature":14.6, "WindSpeed":"1234.56", "year":"2018-05-10"}
        |			],
        |			[
        |				 {"street":"abc1", "city":"city3", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"def1", "city":"city4", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"cfg1", "city":"city5", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"}
        |			]
        |		],
        |		[
        |			[
        |				 {"street":"abc2", "city":"cityx", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"abc3", "city":"cityy", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"abc4", "city":"cityz", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"}
        |			],
        |			[
        |				 {"street":"a1bc", "city":"cityA", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"a1bc", "city":"cityB", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"},
        |				 {"street":"a1bc", "city":"cityc", "Temperature":12.6, "WindSpeed":"1234.56", "year":"2018-05-10"}
        |			]
        |		]
        |	]
        |} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json, isLocalDictionary)
  }

  def buildAvroTestDataMultiLevel4Type(isLocalDictionary: Boolean): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel4(3, null, isLocalDictionary)
  }

  // test multi level -- 4 levels [array of array of array of struct]
  test("test multi level support : array of array of array of struct") {
    buildAvroTestDataMultiLevel4Type(false)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  test("test local dictionary for complex datatype") {
    buildAvroTestDataMultiLevel4Type(true)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS localComplex")
    sql(
      s"""CREATE EXTERNAL TABLE localComplex STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("describe formatted localComplex").show(30, false)
    val descLoc = sql("describe formatted localComplex").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }

    // TODO: Add a validation

    sql("DROP TABLE localComplex")
    // drop table should not delete the files
    cleanTestData()
  }

  test("test multi level support : array of array of array of with Double data type") {
    cleanTestData()
    val mySchema =  """ {
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

    val jsonvalue=
      """{
        |"name" :"babu",
        |"age" :12,
        |"my_address" :{ "Temperaturetest" :123 }
        |}
      """.stripMargin
    val pschema= org.apache.avro.Schema.parse(mySchema)
    val records = testUtil.jsonToAvro(jsonvalue, mySchema)
    val writer = CarbonWriter.builder().outputPath(writerPath).withAvroInput(pschema).writtenBy("TestNonTransactionalCarbonTableWithComplexType").build()
    writer.write(records)
    writer.close()
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  // test multi level -- 4 levels [array of array of array of struct]
  test("test ComplexDataType projection for array of array of array of struct") {
    buildAvroTestDataMultiLevel4Type(false)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select BuildNum[0][0][0].street from sdkOutputTable"),
      Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select BuildNum[1][0][0].street from sdkOutputTable"),
      Seq(Row("abc2"), Row("abc2"), Row("abc2")))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  def buildAvroTestDataMultiLevel6Type(isLocalDictionary: Boolean): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel6(1, null, isLocalDictionary)
  }

  // test multi level -- 6 levels
  def buildAvroTestDataMultiLevel6(rows: Int, options: util.Map[String, String], isLocalDictionary: Boolean): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |"type": "record",
        |	"name": "UserInfo",
        |	"namespace": "com.apache.schema.schemalevel6_struct",
        |	"fields": [
        |		{
        |			"name": "username",
        |			"type": "string",
        |			"default": "NONE"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int",
        |			"default": -1
        |		},
        |		{
        |			"name": "phone",
        |			"type": "string",
        |			"default": "NONE"
        |		},
        |		{
        |			"name": "housenum",
        |			"type": "string",
        |			"default": "NONE"
        |		},
        |		{
        |			"name": "address",
        |			"type": {
        |				"type": "record",
        |				"name": "Mailing_Address",
        |				"fields": [
        |					{
        |						"name": "Address_Detail",
        |						"type": {
        |							"type": "record",
        |							"name": "Address_Detail",
        |							"fields": [
        |								{
        |									"name": "Building_Detail",
        |									"type": {
        |										"type": "record",
        |										"name": "Building_Address",
        |										"fields": [
        |											{
        |												"name": "Society_name",
        |												"type": "string"
        |											},
        |											{
        |												"name": "building_no",
        |												"type": "string"
        |											},
        |											{
        |												"name": "house_no",
        |												"type": "int"
        |											},
        |											{
        |												"name": "Building_Type",
        |												"type": {
        |													"type": "record",
        |													"name": "Building_Type",
        |													"fields": [
        |														{
        |															"name":"Buildingname",
        |															"type":"string"
        |														},
        |														{
        |															"name":"buildingArea",
        |															"type":"int"
        |														},
        |														{
        |															"name":"Building_Criteria",
        |															"type":{
        |																"type":"record",
        |																"name":"BuildDet",
        |																"fields":[
        |																	{
        |																		"name":"f1",
        |																		"type":"int"
        |																	},
        |																	{
        |																		"name":"f2",
        |																		"type":"string"
        |																	},
        |																	{
        |																		"name":"BuildDetInner",
        |																		"type":
        |																			{
        |																				"type":"record",
        |																				"name":"BuildInner",
        |																				"fields":[
        |																						{
        |																							"name": "duplex",
        |																							"type": "boolean"
        |																						},
        |																						{
        |																							"name": "Price",
        |																							"type": "int"
        |																						},
        |																						{
        |																							"name": "TotalCost",
        |																							"type": "int"
        |																						},
        |																						{
        |																							"name": "Floor",
        |																							"type": "int"
        |																						},
        |																						{
        |																							"name": "PhoneNo",
        |																							"type": "long"
        |																						},
        |																						{
        |																							"name": "value",
        |																							"type": "string"
        |																						}
        |																				]
        |																			}
        |																	}
        |																]
        |															}
        |														}
        |													]
        |												}
        |											}
        |										]
        |									}
        |								}
        |							]
        |						}
        |					}
        |				]
        |			}
        |		}
        |	]
        |} """.stripMargin

    val json =
      """ {
        |"username": "DON",
        |"age": 21,
        |"phone": "9888",
        |"housenum": "44",
        |"address": {
        |"Address_Detail": {
        |"Building_Detail": {
        |"Society_name": "TTTT",
        |"building_no": "5",
        |"house_no": 78,
        |"Building_Type": {
        |"Buildingname": "Amaranthus",
        |"buildingArea": 34,
        |"Building_Criteria": {
        |"f1": 23,
        |"f2": "RRR",
        |"BuildDetInner": {
        |"duplex": true,
        |"Price": 3434,
        |"TotalCost": 7777,
        |"Floor": 4,
        |"PhoneNo": 5656,
        |"value":"Value"
        |}
        |}
        |}
        |}
        |}
        |}
        |} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json, isLocalDictionary)
  }


  test("test ComplexDataType projection for struct of struct -6 levels") {
    buildAvroTestDataMultiLevel6Type(false)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row("DON", 21, "9888", "44", Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34,
        Row(23, "RRR", Row(true, 3434, 7777, 4, 5656,  "Value")))))))))
    checkAnswer(sql("select address from sdkOutputTable"),
      Seq(Row(Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value")))))))))
    checkAnswer(sql("select address.Address_Detail from sdkOutputTable"),
      Seq(Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))))))
    checkAnswer(sql("select address.Address_Detail.Building_Detail from sdkOutputTable"),
      Seq(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value")))))))
    checkAnswer(sql("select address.Address_Detail.Building_Detail.Building_Type from sdkOutputTable"),
      Seq(Row(Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria from " +
      "sdkOutputTable"), Seq(Row(Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value")))))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.duplex from sdkOutputTable"), Seq(Row(true)))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.price from sdkOutputTable"), Seq(Row(3434)))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.totalcost from sdkOutputTable"), Seq(Row(7777)))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.floor from sdkOutputTable"), Seq(Row(4)))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.phoneNo from sdkOutputTable"), Seq(Row(5656)))
    checkAnswer(sql(
      "select address.Address_Detail.Building_Detail.Building_Type.Building_Criteria" +
      ".BuildDetInner.value from sdkOutputTable"), Seq(Row("Value")))
    checkAnswer(sql("select address,address.Address_Detail from sdkOutputTable"),
      Seq(Row(Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))))
      , Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))))))
    checkAnswer(sql("select address.Address_Detail.Building_Detail.Building_Type,address.Address_Detail.Building_Detail.Building_Type.Building_Criteria from sdkOutputTable"), Seq(Row(Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))),Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value")))))
    checkAnswer(sql("select address.Address_Detail,address.Address_Detail.Building_Detail.Building_Type.Building_Criteria from sdkOutputTable"),Seq(Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))),Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value")))))
    checkAnswer(sql("select address.Address_Detail,address.Address_Detail.Building_Detail.Society_name,address.Address_Detail.Building_Detail.Building_Type.Building_Criteria.f1 from sdkOutputTable"),
      Seq(Row(Row(Row("TTTT", "5", 78, Row("Amaranthus", 34, Row(23, "RRR", Row(true, 3434, 7777, 4, 5656, "Value"))))),"TTTT",23)))
    checkAnswer(sql("select address.Address_Detail.Building_Detail.Society_name,address.Address_Detail.Building_Detail.building_no from sdkOutputTable"),Seq(Row("TTTT","5")))
    sql("select address.Address_Detail.Building_Detail.Society_name,address.Address_Detail.Building_Detail.building_no from sdkOutputTable where address.Address_Detail.Building_Detail.Society_name ='TTTT'").show(false)
    sql("DROP TABLE sdkOutputTable")
    cleanTestData()
  }
}
