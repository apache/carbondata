package org.apache.carbondata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}

case class CsvHeaderSchema(columnName: String, dataType: DataType, isNullable: Boolean)

class DataFrameUtil {

  def ifHeaderExists(dataFrame: DataFrame): DataFrame = {
    //TODO: write code to test if header exists
    dataFrame
  }

  def getColumnNames(dataFrame: DataFrame): List[String] = dataFrame.columns.toList

  def getColumnNameFromFileHeader(parameters: CommandLineArguments): List[String] ={
    parameters.fileHeaders match {
      case Some(columnList) => columnList
      case _ => throw new Exception("Headers not present !!")
    }
  }

  def getColumnDataTypes(dataFrame: DataFrame): List[CsvHeaderSchema] = {
    dataFrame.schema.toList map { colStructure => CsvHeaderSchema(colStructure.name,colStructure.dataType, colStructure.nullable) }
  }

  def getColumnHeaderCount(dataFrame: DataFrame): Int = dataFrame.columns.length

}
