package org.apache.carbondata

import org.apache.carbondata.cardinality.{CardinalityMatrix, CardinalityProcessor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

case class EmployeeRecord(name: String, age: Int)

class CardinalityProcessorTest extends FunSuite with MockitoSugar with CardinalityProcessor {

  import TestHelper.sparkSession.implicits._

  val dataFrameUtil: DataFrameUtil = mock[DataFrameUtil]

  val uniqueNameList = List("sangeeta", "geetika", "pallavi", "prabhat")
  val nameList = List("sangeeta", "geetika", "pallavi", "prabhat", "sangeeta", "geetika", "sangeeta")
  val ageList = List(22,24,26,35,22,22,22,24)

  val uniqueNameDataFrame = TestHelper.sparkSession.sparkContext.parallelize(uniqueNameList).toDF("name")
  val nameDataFrame = TestHelper.sparkSession.sparkContext.parallelize(nameList).toDF("name")
  val ageDataFrame = TestHelper.sparkSession.sparkContext.parallelize(ageList).toDF("age")


  test("calculate column cardinality for empty dataframe "){
    val dataFrame = TestHelper.sparkSession.emptyDataFrame
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
    assert(updatedCardinalityMatrixList === List(CardinalityMatrix("name", 0.5714285714285714,nameDataFrame, StringType, None), CardinalityMatrix("age", 0.5, ageDataFrame, IntegerType, None)))
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
    val empNameDataFrame = employeeDataframe.select("name")
    val empAgeDataFrame = employeeDataframe.select("age")
    val partialCardinalityMatrix = List(CardinalityMatrix("name",0.5, empNameDataFrame),CardinalityMatrix("age", 0.5, empAgeDataFrame))
    val inputFileSchemaList = List(CsvHeaderSchema("name", StringType, false), CsvHeaderSchema("age", IntegerType, false))

    when(dataFrameUtil.getColumnNames(employeeDataframe)).thenReturn (List("name", "age"))
    when(dataFrameUtil.getColumnDataTypes(employeeDataframe)).thenReturn(List(CsvHeaderSchema("name",StringType,false), CsvHeaderSchema("age", IntegerType, false)))
    when(dataFrameUtil.getColumnNameFromFileHeader(commandLineArguments, employeeDataframe.schema.fields.length)).thenReturn(List("name","age"))

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
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
    val empNameDataFrame = employeeDataframe.select("name")
    val empAgeDataFrame = employeeDataframe.select("age")
    val partialCardinalityMatrix = List(CardinalityMatrix("name",0.5, empNameDataFrame),CardinalityMatrix("age", 0.5, empAgeDataFrame))
    val inputFileSchemaList = List(CsvHeaderSchema("name", StringType, false), CsvHeaderSchema("age", IntegerType, false))

    when(dataFrameUtil.getColumnNames(employeeDataframe)).thenReturn (List("name", "age"))
    when(dataFrameUtil.getColumnDataTypes(employeeDataframe)).thenReturn(List(CsvHeaderSchema("name",StringType,false), CsvHeaderSchema("age", IntegerType, false)))
    when(dataFrameUtil.getColumnNameFromFileHeader(commandLineArguments, employeeDataframe.schema.fields.length)).thenReturn(List("_c0","_c1"))

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)

    assert(cardinalityMatrixList.head.columnName.contains("name") && cardinalityMatrixList.last.columnName.contains("age"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("_c0") && cardinalityMatrixList.last.inputColumnName.contains("_c1"))
  }

  test("Headers Test Case 2 : when file headers are not present in input file but present in command line arguments and header set to false"){
    val commandLineArguments = CommandLineArguments("", Some(List("name","age")))
    val isHeadersPresent = commandLineArguments.fileHeaders.isDefined
    val employeeDataframe: DataFrame = TestHelper.sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", (!isHeadersPresent).toString)
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .load("../tools/src/test/resources/input.csv")

    val empNameDataFrame = employeeDataframe.select("_c0")
    val empAgeDataFrame = employeeDataframe.select("_c1")
    when(dataFrameUtil.getColumnNames(employeeDataframe)).thenReturn (List("_c0", "_c1"))
    when(dataFrameUtil.getColumnDataTypes(employeeDataframe)).thenReturn(List(CsvHeaderSchema("_c0",StringType,false), CsvHeaderSchema("_c1", IntegerType, false)))
    when(dataFrameUtil.getColumnNameFromFileHeader(commandLineArguments, employeeDataframe.schema.fields.length)).thenReturn(List("name","age"))

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)
    assert(cardinalityMatrixList.head.columnName.contains("_c0") && cardinalityMatrixList.last.columnName.contains("_c1"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("name") && cardinalityMatrixList.last.inputColumnName.contains("age"))
  }

  test("Headers Test Case 3: File Headers provided with input and no command line headers are present(file)"){
    val commandLineArguments = CommandLineArguments("", None)
    val isHeadersPresent = commandLineArguments.fileHeaders.isDefined
    val employeeDataframe: DataFrame = TestHelper.sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", (!isHeadersPresent).toString)
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .load("../tools/src/test/resources/input-with-header.csv")
    val empAgeDataFrame = employeeDataframe.select("age")
    val empNameDataFrame = employeeDataframe.select("name")

    when(dataFrameUtil.getColumnNames(employeeDataframe)).thenReturn (List("name", "age"))
    when(dataFrameUtil.getColumnDataTypes(employeeDataframe)).thenReturn(List(CsvHeaderSchema("name",StringType,false), CsvHeaderSchema("age", IntegerType, false)))
    when(dataFrameUtil.getColumnNameFromFileHeader(commandLineArguments, employeeDataframe.schema.fields.length)).thenReturn(List("_c0","_c1"))

    val cardinalityMatrixList = getCardinalityMatrix(employeeDataframe, commandLineArguments)

    assert(cardinalityMatrixList.head.columnName.contains("name") && cardinalityMatrixList.last.columnName.contains("age"))
    assert(cardinalityMatrixList.head.cardinality.equals(0.5) && cardinalityMatrixList.last.cardinality.equals(0.5))
    assert(cardinalityMatrixList.head.columnDataframe.collect().toList == empNameDataFrame.collect().toList )
    assert(cardinalityMatrixList.last.columnDataframe.collect().toList == empAgeDataFrame.collect().toList )
    assert(cardinalityMatrixList.head.inputColumnName.contains("_c0") && cardinalityMatrixList.last.inputColumnName.contains("_c1"))
  }

  //TODO: This scenario needs to be handled
  test("Headers Test Case 1 : when file headers are present in input source and commandline both"){
    pending
    val commandLineArguments = CommandLineArguments("", Some(List("name","age")))
    val employeeDataframe: DataFrame = TestHelper.sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("../tools/src/test/resources/input-with-header.csv")

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
  test("Headers Test Case 4 : when file headers are neither present in input source nor commandline"){
    pending
    val commandLineArguments = CommandLineArguments("", None)
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
