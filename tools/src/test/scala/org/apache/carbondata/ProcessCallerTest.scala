package org.apache.carbondata

import org.apache.carbondata.cardinality.CardinalityProcessor
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

class ProcessCallerTest extends FunSuite with MockitoSugar with ProcessCaller {


  import TestHelper.sparkSession.implicits._

  val uniqueNameList = List("Prabhat", "Sangeeta")

  val dataFrames: DataFrame = TestHelper.sparkSession.sparkContext.parallelize(uniqueNameList).toDF("name")

  val loadHandler: LoadHandler = mock[LoadHandler]
  val cardinalityProcessor: CardinalityProcessor = mock[CardinalityProcessor]

  val commandLineArguments = CommandLineArguments("file_path")
  val arguments = Array("filePath", "File Header", "Delimiter", "QuoteCharacter", "Bad Record Action")


  test("Start Process with valid arguments") {
    when(loadHandler.getDataFrameAndArguments(arguments)) thenReturn ((dataFrames, commandLineArguments))
    when(cardinalityProcessor.getCardinalityMatrix(dataFrames, commandLineArguments)) thenReturn Nil
    assert(startProcess(arguments) === Nil)
  }

  test("Start Process without File Path") {
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array.empty[String]
      startProcess(arguments)
    }
  }

  test("Start Process with extra arguments") {
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array("filePath", "File Header", "Delimiter",
        "QuoteCharacter", "Bad Record Action", "Extra Argument")
      startProcess(arguments)
    }
  }

}
