package org.apache.carbondata

import org.scalatest.FunSuite

class LaunchTest extends FunSuite {

  test("Carbon Tool Launch without File Path") {
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array.empty[String]
      Launch.main(arguments)
    }
  }

  test("Carbon Tool Launch with extra arguments"){
    intercept[org.apache.carbondata.exception.InvalidParameterException] {
      val arguments = Array("filePath", "File Header" , "Delimiter",
        "QuoteCharacter", "Bad Record Action", "Extra Argument")
      Launch.main(arguments)
    }
  }

}
