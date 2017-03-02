package org.apache.carbondata

import org.apache.carbondata.cardinality.{CardinalityMatrix, CardinalityProcessor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.FunSuite

case class EmployeeRecord(name: String, age: Int)

class CardinalityProcessorTest extends FunSuite with CardinalityProcessor{

  import TestHelper.sparkSession.implicits._

  val uniqueNameList = List("sangeeta", "geetika", "pallavi", "prabhat")
  val nameList = List("sangeeta", "geetika", "pallavi", "prabhat", "sangeeta", "geetika", "sangeeta")
  val ageList = List(22,24,26,35,22,22,22,24)

  val uniqueNameDataFrame: DataFrame = TestHelper.sparkSession.sparkContext.parallelize(uniqueNameList).toDF("name")
  val nameDataFrame: DataFrame = TestHelper.sparkSession.sparkContext.parallelize(nameList).toDF("name")
  val ageDataFrame: DataFrame = TestHelper.sparkSession.sparkContext.parallelize(ageList).toDF("age")


  test("calculate column cardinality for empty dataframe "){
    val dataFrame: DataFrame = TestHelper.sparkSession.emptyDataFrame
    val cardinality: Double = computeCardinality("Name", dataFrame)
    assert(cardinality === 0.0)
  }

  test("calculate column cardinality for dataframe containing unique data"){
    val cardinality: Double = computeCardinality("Name", uniqueNameDataFrame)
    assert(cardinality === 1.0)
  }

  test("calculate column cardinality for dataframe containing duplicate data"){
    val cardinality: Double = computeCardinality("Name", nameDataFrame)
    assert(cardinality === 0.5714285714285714)
  }

  test("set data type with cardinality"){
    val inputFileSchemaList = List(CsvHeaderSchema("name", StringType, isNullable = false), CsvHeaderSchema("age",IntegerType, isNullable = false))
    val cardinalityMatrixList: List[CardinalityMatrix] = List(CardinalityMatrix("name",0.5714285714285714,nameDataFrame),
      CardinalityMatrix("age", 0.5, ageDataFrame))

    val updatedCardinalityMatrixList = setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchemaList)
    assert(updatedCardinalityMatrixList === List(CardinalityMatrix("name", 0.5714285714285714,nameDataFrame, StringType, ""), CardinalityMatrix("age", 0.5, ageDataFrame, IntegerType, "")))
  }

  test("set data type with cardinality : column mismatch case") {
    val inputFileSchemaList = List(CsvHeaderSchema("name1", StringType, isNullable = false), CsvHeaderSchema("age", IntegerType, isNullable = false))
    val cardinalityMatrixList = List(CardinalityMatrix("name", 0.5714285714285714, nameDataFrame), CardinalityMatrix("age", 0.5, ageDataFrame))

    intercept[java.lang.IllegalArgumentException] {
      setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchemaList)
    }
  }

  test("get cardinality matrix : when file headers are present"){
    val commandLineArguments = CommandLineArguments("", Some(List("name","age")))
    val employeeRecord = List(EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika", 24),EmployeeRecord("pallavi", 26),EmployeeRecord("prabhat", 35),EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika",22),EmployeeRecord("sangeeta",22),EmployeeRecord("prabhat", 35))
    val employeeDataframe = TestHelper.sparkSession.sparkContext.parallelize(employeeRecord).toDF("name", "age")

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
    val empNameDataFrame = employeeDataframe.select("name")
    val empAgeDataFrame = employeeDataframe.select("age")



    assert(cardinalityMatrixList.head.columnName.contains("name") && cardinalityMatrixList.last.columnName.contains("age"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("name") && cardinalityMatrixList.last.inputColumnName.contains("age"))
  }

  test("get cardinality matrix : when file headers are not provided from command line"){
    val commandLineArguments = CommandLineArguments("", None)
    val employeeRecord = List(EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika", 24),EmployeeRecord("pallavi", 26),EmployeeRecord("prabhat", 35),EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika",22),EmployeeRecord("sangeeta",22),EmployeeRecord("prabhat", 35))
    val employeeDataframe = TestHelper.sparkSession.sparkContext.parallelize(employeeRecord).toDF("name", "age")

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
    val empNameDataFrame = employeeDataframe.select("name")
    val empAgeDataFrame = employeeDataframe.select("age")

    assert(cardinalityMatrixList.head.columnName.contains("name") && cardinalityMatrixList.last.columnName.contains("age"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.equals("_c0") && cardinalityMatrixList.last.inputColumnName.equals("_c1"))
  }

  test("get cardinality matrix : when file headers are not present in input file but present in command line arguments and header set to false"){
    val commandLineArguments = CommandLineArguments("", Some(List("name","age")))
    val employeeDataframe: DataFrame = TestHelper.sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("../tools/src/test/resources/input.csv")

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
    val empNameDataFrame = employeeDataframe.select("_c0")
    val empAgeDataFrame = employeeDataframe.select("_c1")

    assert(cardinalityMatrixList.head.columnName.contains("_c0") && cardinalityMatrixList.last.columnName.contains("_c1"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("name") && cardinalityMatrixList.last.inputColumnName.contains("age"))
  }

  //TODO: This scenario needs to be handled
  test("get cardinality matrix : when file headers are not present in input file but present in command line arguments and header set to true"){
    pending
    val commandLineArguments = CommandLineArguments("", Some(List("name","age")))
    val employeeDataframe: DataFrame = TestHelper.sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("../tools/src/test/resources/input.csv")

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
    val empNameDataFrame = employeeDataframe.select("_c0")
    val empAgeDataFrame = employeeDataframe.select("_c1")

    assert(cardinalityMatrixList.head.columnName.contains("_c0") && cardinalityMatrixList.last.columnName.contains("_c1"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("name") && cardinalityMatrixList.last.inputColumnName.contains("age"))
  }

}
