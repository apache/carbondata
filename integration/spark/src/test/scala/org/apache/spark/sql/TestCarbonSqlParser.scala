package org.apache.spark.sql

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.cubemodel.Field

/**
  * Stub class for calling the CarbonSqlParser
  */
private class TestCarbonSqlParserStub extends CarbonSqlDDLParser {

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
    var fields: Seq[Field] = Seq[Field]()

    var col1 = Field("col1", Option("Int"), Option("col1"), None, null, Some("columnar"))
    var col2 = Field("col2", Option("String"), Option("col2"), None, null, Some("columnar"))
    var col3 = Field("col3", Option("String"), Option("col3"), None, null, Some("columnar"))
    var col4 = Field("col4", Option("Int"), Option("col4"), None, null, Some("columnar"))

    fields :+= col1
    fields :+= col2
    fields :+= col3
    fields :+= col4

    val stub = new TestCarbonSqlParserStub()
    var (dimCols, noDictionary) = stub.extractDimColsAndNoDictionaryFieldsTest(fields, tableProperties)

    // testing col

    assert(dimCols.lift(0).get.column.equalsIgnoreCase("col3"))

    assert(dimCols.lift(1).get.column.equalsIgnoreCase("col4"))

    assert(noDictionary.lift(0).get.equalsIgnoreCase("col2"))
  }


  // Testing the extracting of measures
  test("Test-extractMsrColsFromFields") {
    val tableProperties = Map("DICTIONARY_EXCLUDE" -> "col2", "DICTIONARY_INCLUDE" -> "col4")
    var fields: Seq[Field] = Seq[Field]()

    var col1 = Field("col1", Option("Int"), Option("col1"), None, null, Some("columnar"))
    var col2 = Field("col2", Option("String"), Option("col2"), None, null, Some("columnar"))
    var col3 = Field("col3", Option("String"), Option("col3"), None, null, Some("columnar"))
    var col4 = Field("col4", Option("Int"), Option("col4"), None, null, Some("columnar"))

    fields :+= col1
    fields :+= col2
    fields :+= col3
    fields :+= col4

    val stub = new TestCarbonSqlParserStub()
    var msrCols = stub.extractMsrColsFromFieldsTest(fields, tableProperties)

    // testing col

    assert(msrCols.lift(0).get.column.equalsIgnoreCase("col1"))

  }


}


