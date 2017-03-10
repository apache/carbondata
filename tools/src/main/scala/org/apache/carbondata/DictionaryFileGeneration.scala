package org.apache.carbondata

import org.apache.carbondata.cardinality.{CardinalityMatrix, CardinalityProcessor}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.dictionary.CarbonTableUtil
import org.apache.carbondata.exception.InvalidParameterException

trait DictionaryFileGeneration {

  val dataReader: DataReader
  val cardinalityProcessor: CardinalityProcessor
  val carbonTableUtil: CarbonTableUtil

  /**
   *
   * @param args Command Line Arguments accepting "path_to_input_source, fileheader(optional),
   *             delimiter(optional), quotecharacter(optional)"
   *             ,badrecordaction(optional)
   * @return
   */
  def startGeneration(args: Array[String]): List[CardinalityMatrix] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    if (args.length != 1) {
      LOGGER.error("Invalid input parameters.")
      LOGGER
        .error(
          "[Usage]: \"inputpath=<Path>, fileheader=<File Header(Comma-separated)>[Optional], " +
          "delimiter=<Delimiter>[Optional], quotecharacter=<Quote Character>[Optional], " +
          "badrecordaction=<Bad Record Action>[Optional]\"")

      throw InvalidParameterException("Invalid Parameter Exception")
    } else {
      val (dataFrame, arguments) = dataReader.getDataFrameAndArguments(args)

      cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val cardinalityMatrix = cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val dictionaryPath: String =carbonTableUtil.createDictionary(cardinalityMatrix, dataFrame)
      LOGGER.info(s"Dictionary created successfully.\n Location : $dictionaryPath")
      cardinalityMatrix
    }
  }

}

object DictionaryFileGeneration extends DictionaryFileGeneration {
  val dataReader: DataReader = DataReader
  val cardinalityProcessor: CardinalityProcessor = CardinalityProcessor
  val carbonTableUtil: CarbonTableUtil = CarbonTableUtil
}
