package org.apache.carbondata

import org.apache.carbondata.utils.{ArgumentParser, LoadProperties}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class DataReaderTest extends FunSuite with DataReader with MockitoSugar{

  val argumentParser: ArgumentParser = mock[ArgumentParser]

  test("Get Data Frames with invalid file path") {
    intercept[java.io.FileNotFoundException] {
      val arguments = Array("filePath")
      getDataFrameAndArguments(arguments)
    }
  }

  test("Get Data Frames with valid file path") {
    val arguments = Array("../tools/src/test/resources/test.csv")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    assert(dataFrame.count() === 5)
  }

  test("Get Data Frames with valid text file path") {
    val arguments = Array("../tools/src/test/resources/test.txt")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    assert(dataFrame.count() === 5)
  }

  test("load data from a folder") {
    val arguments = Array("../tools/src/test/resources/test_multi_load_with_header")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    assert(dataFrame.count() === 10)
  }

  test("load data from folder when headers are present in both files and in command-line-arguments") {
    val arguments = Array("../tools/src/test/resources/test_multi_load_with_header", "name,occupation,salary,age,dob")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 10)
  }

  test("load data from folder when headers are present in command-line-arguments but not file ") {
    val arguments = Array("../tools/src/test/resources/test_multi_load_without_header", "name,occupation,salary,age,dob")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 10)
  }

  test("load data when headers are present in command-line-arguments but some input files have headers and some do not have headers") {
    val arguments = Array("../tools/src/test/resources/test_multi_load_for_header", "name,occupation,salary,age,dob")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 10)
  }

  test("load data when headers are not present in command-line-arguments but some input files have headers and some donot have headers") {
    val arguments = Array("../tools/src/test/resources/test_multi_load_for_header")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 10)
  }

  test("load data from folder with mixed csv's with different headers") {
    val arguments = Array("../tools/src/test/resources")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 10)
  }

  test("load empty csv without command line arguments") {
    val arguments = Array("../tools/src/test/resources/empty.csv")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 0)
  }

  test("load empty csv with header and no command line arguments") {
    val arguments = Array("../tools/src/test/resources/empty-with-header.csv")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 0)
  }

  test("load empty csv with command line arguments"){
    val arguments = Array("../tools/src/test/resources/empty.csv", "name,occupation,salary,age,dob")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 0)
  }

  test("load empty csv with headers when command line arguments are also present") {
    val arguments = Array("../tools/src/test/resources/empty-with-header.csv", "name,occupation,salary,age,dob")
    val (dataFrame: DataFrame, _: LoadProperties) = getDataFrameAndArguments(arguments)
    dataFrame.show()
    assert(dataFrame.count() === 0)
  }

}
