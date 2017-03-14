package org.apache.carbondata.utils

import org.scalatest.FunSuite

import org.apache.carbondata.exception.InvalidParameterException

class ArgumentParserTest extends FunSuite with ArgumentParser {

  val test = "'inputpath'='/opt/csv.csv','fileheader'='p,k,q'"

  test("Parse argument with empty arguments") {
    intercept[InvalidParameterException] {
      getProperties("")
    }
  }

  test("Parse argument with only input path") {
    val arguments = "'inputpath'='../test/test.csv'"
    assert(getProperties(arguments) == LoadProperties("../test/test.csv"))
  }

  test("Parse argument with all properties") {
    val arguments = "'inputpath'='../test/test.csv', 'fileheader'='id,name,location'," +
                    "'quotechar'='$','badrecordaction'='FORCED','storelocation'='../test/test'," +
                    "'delimiter'=';'"
    assert(getProperties(arguments) == LoadProperties("../test/test.csv",
      Some(List("id", "name", "location")),
      ";",
      "$",
      "FORCED",
      "../test/test"))
  }

}
