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
package org.apache.spark.sql

import scala.collection.mutable.Map

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.command.Field

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
  * Stub class for calling the CarbonSqlParser
  */
private class TestCarbonSqlParserStub extends CarbonSqlParser {

  def extractDimAndMsrFieldsTest(fields: Seq[Field],
      tableProperties: Map[String, String]): (Seq[Field], Seq[Field], Seq[String], Seq[String]) = {
    extractDimAndMsrFields(fields, tableProperties)
  }


}

/**
  * Test class to test Carbon Sql Parser
  */
class TestCarbonSqlParser extends QueryTest {

  /**
    * load all test fields
    * @return
    */
  def loadAllFields: Seq[Field] = {
    var fields: Seq[Field] = Seq[Field]()

    var col1 = Field("col1", Option("String"), Option("col1"), None, null, Some("columnar"))
    var col2 = Field("col2", Option("String"), Option("col2"), None, null, Some("columnar"))
    var col3 = Field("col3", Option("String"), Option("col3"), None, null, Some("columnar"))
    var col4 = Field("col4", Option("int"), Option("col4"), None, null, Some("columnar"))
    var col5 = Field("col5", Option("String"), Option("col5"), None, null, Some("columnar"))
    var col6 = Field("col6", Option("String"), Option("col6"), None, null, Some("columnar"))
    var col7 = Field("col7", Option("String"), Option("col7"), None, null, Some("columnar"))
    var col8 = Field("col8", Option("String"), Option("col8"), None, null, Some("columnar"))

    fields :+= col1
    fields :+= col2
    fields :+= col3
    fields :+= col4
    fields :+= col5
    fields :+= col6
    fields :+= col7
    fields :+= col8
    fields
  }

  // Testing the extracting of Dims and no Dictionary
  test("Test-extractDimColsAndNoDictionaryFields") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col2", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col4")
    val fields: Seq[Field] = loadAllFields

    val stub = new TestCarbonSqlParserStub()
    val (dimCols, _, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    // testing col

    //All dimension fields should be available in dimensions list
    assert(dimCols.size == 8)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(3).get.column.equalsIgnoreCase("col4"))

    //No dictionary column names will be available in noDictionary list
    assert(noDictionary.size == 7)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(6).get.equalsIgnoreCase("col8"))

  }

  test("Test-DimAndMsrColsWithNoDictionaryFields1") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col1")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 7)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(6).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields2") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_INCLUDE -> "col1")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields3") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col1", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col4")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields,
      tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 8)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(3).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 7)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(6).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 0)
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields4") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col3", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col2")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields5") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col1", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col2")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields6") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col2", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col1")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col3"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields7") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col2 ,col1  ",
      CarbonCommonConstants.DICTIONARY_INCLUDE -> "col3 ,col4 "
    )
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 8)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(3).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 0)
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields8") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE-> "col2", CarbonCommonConstants.DICTIONARY_INCLUDE -> "col3")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (dimCols, msrCols, noDictionary, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 7)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 6)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(2).get.equalsIgnoreCase("col5"))
    assert(noDictionary.lift(3).get.equalsIgnoreCase("col6"))
    assert(noDictionary.lift(4).get.equalsIgnoreCase("col7"))
    assert(noDictionary.lift(5).get.equalsIgnoreCase("col8"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  // Testing the extracting of measures
  test("Test-extractMsrColsFromFields") {
    val tableProperties = Map(CarbonCommonConstants.DICTIONARY_EXCLUDE -> "col2",
      CarbonCommonConstants.DICTIONARY_INCLUDE -> "col1")
    val fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    val (_, msrCols, _, _) = stub.extractDimAndMsrFieldsTest(fields, tableProperties)

    // testing col
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))

  }

}


