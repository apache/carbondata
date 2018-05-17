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
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataTypes, Field, StructField}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}

class TestNonTransactionalCarbonTableWithComplexType extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
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

  private def WriteFilesWithAvroWriter(rows: Int,
      mySchema: String,
      json: String,
      fields: Array[Field]) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json.getBytes(CharEncoding.UTF_8), nn)

    try {
      val writer = CarbonWriter.builder.withSchema(new Schema(fields))
        .outputPath(writerPath).isTransactionalTable(false)
        .uniqueIdentifier(System.currentTimeMillis()).buildWriterForAvroInput(nn)
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
  def buildAvroTestDataMultiLevel4(rows: Int, options: util.Map[String, String]): Any = {
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

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val subFld = new util.ArrayList[Field]
    subFld.add(new StructField("EachDoorNum", DataTypes.INT))

    val address = new util.ArrayList[Field]
    address.add(new StructField("street", DataTypes.STRING))
    address.add(new StructField("city", DataTypes.STRING))
    address.add(new StructField("Temperature", DataTypes.DOUBLE))
    address.add(new StructField("WindSpeed", DataTypes.createDecimalType(6,2)))
    address.add(new StructField("year", DataTypes.DATE))

    val fld = new util.ArrayList[Field]
    fld.add(new StructField("DoorNum",
      DataTypes.createArrayType(DataTypes.createStructType(address)),
      subFld))
    // array of struct of struct
    val doorNum = new util.ArrayList[Field]
    doorNum.add(new StructField("FloorNum",
      DataTypes.createArrayType(
        DataTypes.createArrayType(DataTypes.createStructType(address))), fld))
    fields(2) = new Field("BuildNum", "array", doorNum)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataMultiLevel4Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel4(3, null)
  }

  // test multi level -- 4 levels [array of array of array of struct]
  test("test multi level support : array of array of array of struct") {
    buildAvroTestDataMultiLevel4Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
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

    val records=new JsonAvroConverter().convertToGenericDataRecord(jsonvalue.getBytes(CharEncoding.UTF_8),pschema)

    val fieds = new Array[Field](3)
    fieds(0)=new Field("name",DataTypes.STRING);
    fieds(1)=new Field("age",DataTypes.INT)

    val fld = new util.ArrayList[Field]
    fld.add(new StructField("Temperature", DataTypes.DOUBLE))
    fieds(2) = new Field("my_address", "struct", fld)


    val writer=CarbonWriter.builder().withSchema(new Schema(fieds)).outputPath(writerPath).buildWriterForAvroInput(pschema)
    writer.write(records)
    writer.close()
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }
}
