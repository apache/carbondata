package org.apache.carbondata

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CommandLineArguments(inputPath: String, fileHeaders: Option[List[String]] = None, delimiter: String = ",", quoteCharacter: String = "\"",
                                badRecordAction: String = "IGNORE")

trait LoadHandler {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def getDataFrameAndArguments(args: Array[String]): (DataFrame, CommandLineArguments) = {
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val arguments = getArguments(args)
    val headerExistInCSV = arguments.fileHeaders.fold(true) { _ => false }

    val df: DataFrame = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", headerExistInCSV.toString)
      .option("inferSchema", "true")
      .option("delimiter", arguments.delimiter)
      .option("quote", arguments.quoteCharacter)
      .load(arguments.inputPath)
    df.printSchema()
    (df, arguments)
  }

  private def getArguments(args: Array[String]): CommandLineArguments = {
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

}

object LoadHandler extends LoadHandler
