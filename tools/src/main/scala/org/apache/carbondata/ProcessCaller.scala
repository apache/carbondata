package org.apache.carbondata

import org.apache.carbondata.cardinality.{CardinalityMatrix, CardinalityProcessor}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.dictionary.CarbonTableUtil
import org.apache.carbondata.exception.InvalidParameterException

trait ProcessCaller {

  val loadHandler: LoadHandler
  val cardinalityProcessor: CardinalityProcessor
  val carbonTableUtil: CarbonTableUtil

  def startProcess(args: Array[String]): List[CardinalityMatrix] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    if (args.length > 5 || args.length < 1) {
      LOGGER.error("Invalid input parameters.")
      LOGGER.error("[Usage]: <Path> <File Header(Comma-separated)>[Optional] <Delimiter>[Optional] <Quote Character>[Optional] <Bad Record Action>[Optional]")
      throw InvalidParameterException("Invalid Parameter Exception")
    } else {
      val (dataFrame, arguments) = loadHandler.getDataFrameAndArguments(args)

      cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val cardinalityMatrix = cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val dictionaryPath: String =carbonTableUtil.createDictionary(cardinalityMatrix, dataFrame)
      LOGGER.info(s"Dictionary created successfully.\n Location : $dictionaryPath")
      cardinalityMatrix
    }
  }

}

object ProcessCaller extends ProcessCaller {
  val loadHandler: LoadHandler = LoadHandler
  val cardinalityProcessor: CardinalityProcessor = CardinalityProcessor
  val carbonTableUtil: CarbonTableUtil = CarbonTableUtil
}
