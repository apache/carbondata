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

import java.io.{BufferedReader, File, FileNotFoundException, FileReader}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.exception.{EmptyFileException, InvalidHeaderException}
import org.apache.carbondata.utils.{ArgumentParser, LoadProperties}

trait DataReader {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val argumentParser: ArgumentParser

  /**
   * This method reads and return dataframe and command line arguments
   *
   * @param args
   * @return
   */
  def getDataFrameAndArguments(args: Array[String]): (DataFrame, LoadProperties) = {
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val arguments = argumentParser.getProperties(args.head)
    val headerExist: Boolean = arguments.fileHeaders.fold(true) { _ => false }
    checkCSVHeader(arguments.inputPath, arguments.delimiter, headerExist)
    val df: DataFrame = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", headerExist.toString)
      .option("inferSchema", "true")
      .option("delimiter", arguments.delimiter)
      .option("quote", arguments.quoteCharacter)
      .load(arguments.inputPath)
    (df, arguments)
  }

  private def checkCSVHeader(csvFile: String,
      delimiter: String,
      isCommandLineHeaderExist: Boolean) = {
    val path = new File(csvFile)
    if (!path.exists()) {
      throw new FileNotFoundException("File not found : " + path.getAbsolutePath)
    }
    if (path.isFile) {

      if (path.length == 0) {
        throw EmptyFileException("Input File is Empty : " + path.getAbsolutePath)
      } else {
        val file: BufferedReader = new BufferedReader(new FileReader(path))
        val fileHeaders = file.readLine().split(delimiter).toList
        if (fileHeaders.distinct.size != fileHeaders.size) {
          throw InvalidHeaderException("Duplicate Header")
        }

      }

    } else {

      if (isCommandLineHeaderExist) {
        val listOfHeaders: Array[List[String]] = path.listFiles().map { file =>
          val bufferedReader: BufferedReader = new BufferedReader(new FileReader(file))
          val headers = bufferedReader.readLine().split(delimiter).toList
          bufferedReader.close()
          headers
        }

        if (listOfHeaders.distinct.length != 1) {
          throw InvalidHeaderException("Headers of CSV files provided are not same.")
        } else {
          val headOfList: List[String] = listOfHeaders.head
          if (headOfList.distinct.length != headOfList.length) {
            throw InvalidHeaderException("CSV files contains duplicate headers")
          }
        }
      }
    }
  }

}

object DataReader extends DataReader {
  val argumentParser: ArgumentParser = ArgumentParser
}
