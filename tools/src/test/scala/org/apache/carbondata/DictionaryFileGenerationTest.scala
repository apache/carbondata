package org.apache.carbondata

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.carbondata.dictionary.CarbonTableUtil

import org.apache.carbondata.cardinality.CardinalityProcessor
import org.apache.carbondata.utils.LoadProperties

class DictionaryFileGenerationTest
  extends FunSuite with MockitoSugar with DictionaryFileGeneration {


  import TestHelper.sparkSession.implicits._

  val uniqueNameList = List("Prabhat", "Sangeeta")

  val dataFrames: DataFrame = TestHelper.sparkSession.sparkContext.parallelize(uniqueNameList)
    .toDF("name")

  val dataReader: DataReader = mock[DataReader]
  val cardinalityProcessor: CardinalityProcessor = mock[CardinalityProcessor]
  val carbonTableUtil: CarbonTableUtil = mock[CarbonTableUtil]

  val commandLineArguments = LoadProperties("file_path")
  val arguments = Array("inputpath=file_path")


  test("Start Process with valid arguments") {
    when(dataReader.getDataFrameAndArguments(arguments)) thenReturn
    ((dataFrames, commandLineArguments))
    when(cardinalityProcessor.getCardinalityMatrix(dataFrames, commandLineArguments)) thenReturn Nil
    assert(startGeneration(arguments) === Nil)
  }

  test("Start Process without File Path") {
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array.empty[String]
      startGeneration(arguments)
    }
  }

  test("Start Process with extra arguments") {
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array("filePath", "File Header", "Delimiter",
        "QuoteCharacter", "Bad Record Action", "Extra Argument")
      startGeneration(arguments)
    }
  }

}
