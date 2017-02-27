package org.apache.carbondata

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.exception.InvalidParameterException

object Launch {

  def main(args: Array[String]) {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    if (args.length > 5 || args.length < 2) {
      LOGGER.error("Invalid input parameters.")
      LOGGER.error("[Usage]: <Path> <File Header(Comma-separated)>[Optional] <Delimiter>[Optional] <Quote Character>[Optional] <Bad Record Action>[Optional]")
      throw InvalidParameterException("Invalid Parameter Exception")
    } else {
      val dataFrameHandler = new DataFrameHandler
      dataFrameHandler.startProcess(args)
    }
  }

}
