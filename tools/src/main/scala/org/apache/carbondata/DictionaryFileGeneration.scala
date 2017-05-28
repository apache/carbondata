/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
          "[Usage]: \"'inputpath'='<Path>', 'fileheader'='<File Header(Comma-separated)" +
          ">'[Optional], 'delimiter'='<Delimiter>'[Optional], 'quotecharacter'='<Quote " +
          "Character>'[Optional], 'badrecordaction'='<Bad Record Action>'[Optional]\"")

      throw InvalidParameterException("Invalid Parameter Exception")
    } else {
      val (dataFrame, arguments) = dataReader.getDataFrameAndArguments(args)

      cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val cardinalityMatrix = cardinalityProcessor.getCardinalityMatrix(dataFrame, arguments)
      val dictionaryPath: String = carbonTableUtil.createDictionary(cardinalityMatrix, dataFrame)
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
