package org.apache.carbondata

import org.apache.carbondata.cardinality.{CardinalityMatrix, CardinalityProcessor}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.exception.InvalidParameterException

trait ProcessCaller {

  val loadHandler: LoadHandler
  val cardinalityProcessor: CardinalityProcessor

  def startProcess(args: Array[String]): List[CardinalityMatrix] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    if (args.length > 5 || args.length < 1) {
      LOGGER.error("Invalid input parameters.")
      LOGGER.error("[Usage]: <Path> <File Header(Comma-separated)>[Optional] <Delimiter>[Optional] <Quote Character>[Optional] <Bad Record Action>[Optional]")
      throw InvalidParameterException("Invalid Parameter Exception")
    } else {
      val (dataFrame, arguments) = loadHandler.getDataFrameAndArguments(args)
      cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
    }
  }

}

object ProcessCaller extends ProcessCaller{
  val loadHandler: LoadHandler = LoadHandler
  val cardinalityProcessor: CardinalityProcessor = CardinalityProcessor
}
