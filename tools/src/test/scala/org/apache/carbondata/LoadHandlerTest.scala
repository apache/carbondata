package org.apache.carbondata

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class LoadHandlerTest extends FunSuite with LoadHandler {

  test("Get Data Frames with invalid file path") {
    intercept[org.apache.spark.sql.AnalysisException] {
      val arguments = Array("filePath")
      getDataFrameAndArguments(arguments)
    }
  }

  test("Get Data Frames with valid file path") {
    val arguments = Array("../tools/src/test/resources/test.csv")
    val (dataFrame: DataFrame, _: CommandLineArguments) = getDataFrameAndArguments(arguments)
    assert(dataFrame.count() === 5)
  }

  test("Get Data Frames with valid text file path") {
    val arguments = Array("../tools/src/test/resources/test.txt")
    val (dataFrame: DataFrame, _: CommandLineArguments) = getDataFrameAndArguments(arguments)
    assert(dataFrame.count() === 5)
  }

}
