package org.apache.carbondata.utils

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

case class CsvHeaderSchema(columnName: String, dataType: DataType, isNullable: Boolean)

trait DataFrameUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def getColumnNames(dataFrame: DataFrame): List[String] = dataFrame.columns.toList

  def getColumnNameFromFileHeader(properties: LoadProperties, header_count: Int): List[String] = {
    properties.fileHeaders match {
      case Some(columnList) => columnList
      case _ => LOGGER.info("Headers not present !!")
        (0 until header_count).toList map { count => "_c" + count }
    }
  }

  def getColumnDataTypes(dataFrame: DataFrame): List[CsvHeaderSchema] = {
    dataFrame.schema.toList map { colStructure => CsvHeaderSchema(colStructure.name, colStructure.dataType, colStructure.nullable) }
  }

  def getColumnHeaderCount(dataFrame: DataFrame): Int = dataFrame.columns.length

}

object DataFrameUtil extends DataFrameUtil
