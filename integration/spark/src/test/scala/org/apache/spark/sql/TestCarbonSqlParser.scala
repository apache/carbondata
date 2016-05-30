package org.apache.spark.sql

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.command.Field

/**
  * Stub class for calling the CarbonSqlParser
  */
private class TestCarbonSqlParserStub extends CarbonSqlParser {

  //val parser:CarbonSqlDDLParser = new CarbonSqlDDLParser()

  def updateColumnGroupsInFieldTest(tableProperties: Map[String, String]): Seq[String] = {

    updateColumnGroupsInField(tableProperties)
  }

  def extractDimColsAndNoDictionaryFieldsTest(fields: Seq[Field], tableProperties: Map[String, String]): (Seq[Field],
    Seq[String]) = {

    extractDimColsAndNoDictionaryFields(fields, tableProperties)
  }

  def extractMsrColsFromFieldsTest(fields: Seq[Field], tableProperties: Map[String, String]): (Seq[Field]) = {

    extractMsrColsFromFields(fields, tableProperties)
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

    var col1 = Field("col1", Option("Int"), Option("col1"), None, null, Some("columnar"))
    var col2 = Field("col2", Option("String"), Option("col2"), None, null, Some("columnar"))
    var col3 = Field("col3", Option("String"), Option("col3"), None, null, Some("columnar"))
    var col4 = Field("col4", Option("Int"), Option("col4"), None, null, Some("columnar"))

    fields :+= col1
    fields :+= col2
    fields :+= col3
    fields :+= col4
    fields
  }

  // Testing the column group Splitting method.
  test("Test-updateColumnGroupsInField") {
    val colGroupStr = "(aaa,bbb,ccc),(ddd,eee),(fff),(ggg)"
    val tableProperties = Map("COLUMN_GROUPS" -> colGroupStr)
    val stub = new TestCarbonSqlParserStub()
    val colgrps = stub.updateColumnGroupsInFieldTest(tableProperties)
    assert(colgrps.lift(0).get.equalsIgnoreCase("aaa,bbb,ccc"))
    assert(colgrps.lift(1).get.equalsIgnoreCase("ddd,eee"))
    assert(colgrps.lift(2).get.equalsIgnoreCase("fff"))
    assert(colgrps.lift(3).get.equalsIgnoreCase("ggg"))

  }
  // Testing the column group Splitting method with empty table properties so null will be returned.
  test("Test-Empty-updateColumnGroupsInField") {
    val tableProperties = Map("" -> "")
    val stub = new TestCarbonSqlParserStub()
    val colgrps = stub.updateColumnGroupsInFieldTest(Map())
    //assert( rtn === 1)
    assert(null == colgrps)
  }

  // Testing the extracting of Dims and no Dictionary
  test("Test-extractDimColsAndNoDictionaryFields") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2", "DICTIONARY_INCLUDE" -> "col4")
    var fields: Seq[Field] = loadAllFields

    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub.extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)

    // testing col

    //All dimension fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col4"))

    //No dictionary column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields1") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col1")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields2") {
    val tableProperties = Map("DICTIONARY_INCLUDE" -> "col1")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 0)

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields3") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col1", "DICTIONARY_INCLUDE" -> "col4")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 4)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(3).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))

    //check msr
    assert(msrCols.size == 0)
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields4") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col3", "DICTIONARY_INCLUDE" -> "col2")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 2)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col3"))

    //check msr
    assert(msrCols.size == 2)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(msrCols.lift(1).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields5") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col4", "DICTIONARY_INCLUDE" -> "col2")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col4"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col1"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields6") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2", "DICTIONARY_INCLUDE" -> "col1")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 1)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col4"))
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields7") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2 ,col1  ",
      "DICTIONARY_INCLUDE" -> "col3 ,col4 "
    )
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 4)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col1"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(3).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 2)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col1"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col2"))

    //check msr
    assert(msrCols.size == 0)
  }

  test("Test-DimAndMsrColsWithNoDictionaryFields8") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2,col4", "DICTIONARY_INCLUDE" -> "col3")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub
      .extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    //below dimension fields should be available in dimensions list
    assert(dimCols.size == 3)
    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col2"))
    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col3"))
    assert(dimCols.lift(2).get.column.equalsIgnoreCase("col4"))

    //below column names will be available in noDictionary list
    assert(noDictionary.size == 2)
    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))
    assert(noDictionary.lift(1).get.equalsIgnoreCase("col4"))

    //check msr
    assert(msrCols.size == 1)
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col1"))
  }

  // Testing the extracting of measures
  test("Test-extractMsrColsFromFields") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2", "DICTIONARY_INCLUDE" -> "col4")
    var fields: Seq[Field] = loadAllFields
    val stub = new TestCarbonSqlParserStub()
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    // testing col
    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col1"))

  }


}


