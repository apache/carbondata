package org.apache.carbondata

import java.io.{BufferedReader, File, FileNotFoundException, FileReader}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.exception.InvalidHeaderException
import org.apache.carbondata.utils.{ArgumentParser, LoadProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataReader {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val argumentParser: ArgumentParser

  def getDataFrameAndArguments(args: Array[String]): (DataFrame, LoadProperties) = {
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val arguments = argumentParser.getProperties(args.head)
    val headerExist: String = arguments.fileHeaders.fold(true) { _ => false }.toString
    checkCSVHeader(arguments.inputPath, arguments.delimiter)
    val df: DataFrame = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", headerExist)
      .option("inferSchema", "true")
      .option("delimiter", arguments.delimiter)
      .option("quote", arguments.quoteCharacter)
      .load(arguments.inputPath)
    (df, arguments)
  }

  private def checkCSVHeader(csvFile: String, delimiter: String) = {
    val path = new File(csvFile)
    if (path.exists()) {
      throw new FileNotFoundException("File not found : " + path.getAbsolutePath)
    }
    if (path.isFile) {
      val file = new BufferedReader(new FileReader(path))
      val fileHeaders = file.readLine().split(delimiter).toList
      if (fileHeaders.distinct.size != fileHeaders.size) {
        throw InvalidHeaderException("Duplicate Header")
      } else {
        val listOfHeaders = path.listFiles().map { file =>
          val bufferedReader: BufferedReader = new BufferedReader(new FileReader(file))
          val headers = bufferedReader.readLine().split(delimiter).toList
          bufferedReader.close()
          headers
        }
        if (listOfHeaders.distinct.length != listOfHeaders.length) {
          throw InvalidHeaderException("Headers of CSV files provided are not same.")
        } else {
          val headOfList = listOfHeaders.head
          if (headOfList.distinct.length != headOfList.length) {
            throw InvalidHeaderException("CSV files contains duplicate headers")
          }
        }
      }
      file.close()
    }
  }

}

object DataReader extends DataReader {
  val argumentParser: ArgumentParser = ArgumentParser
}
