package org.apache.carbondata

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.FunSuite

import org.apache.carbondata.utils.{CsvHeaderSchema, DataFrameUtil, LoadProperties}


class DataFrameUtilTest extends FunSuite with DataFrameUtil {

  import TestHelper.sparkSession.implicits._

  val commandLineArguments = LoadProperties("", Some(List("name", "age")))

  val employeeRecord = List(EmployeeRecord("sangeeta", 22),
    EmployeeRecord("geetika", 24),
    EmployeeRecord("pallavi", 26),
    EmployeeRecord("prabhat", 35),
    EmployeeRecord("sangeeta", 22),
    EmployeeRecord("geetika", 22),
    EmployeeRecord("sangeeta", 22),
    EmployeeRecord("prabhat", 35))
  val employeeDataframe = TestHelper.sparkSession.sparkContext.parallelize(employeeRecord)
    .toDF("name", "age")

  test("get column names") {
    val columnList = getColumnNames(employeeDataframe)
    assert(columnList === List("name", "age"))
  }

  test("get column name from file header : When headers are present") {
    val columnList = getColumnNameFromFileHeader(commandLineArguments, 2)
    assert(columnList === List("name", "age"))
  }

  test("get Column name form file header : When headers are not present") {
    val commandLineArguments = LoadProperties("", None)
    val columnList = getColumnNameFromFileHeader(commandLineArguments, 2)
    assert(columnList === List("_c0", "_c1"))
  }

  test("get column Data Types") {
    val result: List[CsvHeaderSchema] = getColumnDataTypes(employeeDataframe)
    assert(result === List(CsvHeaderSchema("name", StringType, true),
      CsvHeaderSchema("age", IntegerType, true)))
  }

  test("get column Header Count") {
    val headerCount = getHeaderCount(employeeDataframe)
    assert(headerCount === 2)
  }
}
