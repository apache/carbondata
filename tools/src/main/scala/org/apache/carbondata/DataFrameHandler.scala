package org.apache.carbondata

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CommandLineArguments(inputPath: String, fileHeaders: Option[List[String]] = None, delimiter: String = ",", quoteCharacter: String = " \"",
                                badRecordAction: String = "IGNORE")

object DataFrameHandler {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def main(args: Array[String]) {
    if (args.length > 5 || args.length < 1) {
        LOGGER.error("Invalid input parameters.")
        LOGGER.error("[Usage]: <Path> <File Header(Comma-separated)>[Optional] <Delimiter>[Optional] <Quote Character>[Optional] <Bad Record Action>[Optional]")
      } else {
        processDataFrame(getArguments(args))
    }
  }

  def getArguments(args: Array[String]): CommandLineArguments = {
    args.length match {
      case 1 => val inputPath: String = args(0)
        CommandLineArguments(inputPath)
      case 2 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        CommandLineArguments(inputPath, fileHeaders)
      case 3 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        CommandLineArguments(inputPath, fileHeaders, delimiter)
      case 4 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        val quoteChar = args(3)
        CommandLineArguments(inputPath, fileHeaders, delimiter, quoteChar)
      case 5 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        val quoteChar = args(3)
        val badRecordAction = args(4)
        CommandLineArguments(inputPath, fileHeaders, delimiter, quoteChar, badRecordAction)
    }
  }

  def processDataFrame(parameters: CommandLineArguments): Unit = {
    val isHeaderExist = parameters.fileHeaders.isDefined
    val dataFrame = loadData(parameters.inputPath, isHeaderExist)
    val cardinalityProcessor = new CardinalityProcessor
    println("Cardinality Matrix is : " + cardinalityProcessor.getCardinalityMatrix(dataFrame, parameters))
  }

  def loadData(filePath: String, isHeaderExist: Boolean): DataFrame = {
    LOGGER.info("Starting with the demo project")
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

//    sparkSession.sparkContext.setLogLevel("WARN")

    val df: DataFrame = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
    df.printSchema()
    df
  }


}
