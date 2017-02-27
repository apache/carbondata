package org.apache.carbondata

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}

case class CardinalityMatrix(columnName: String, cardinality: Double, columnDataframe: DataFrame, dataType: DataType = StringType, inputColumnName: String = "")

class CardinalityProcessor{

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val dataFrameUtil = new DataFrameUtil

  def computeCardinality(colName: String, columnDataFrame: DataFrame): Double = {
    LOGGER.info(s"Computing Cardinality for Column $columnDataFrame")
    val uniqueData = columnDataFrame.distinct
    uniqueData.count.toDouble / columnDataFrame.count
  }

  //TODO: Validation needs to be added so that no two columns in csv have same name
  def setDataTypeWithCardinality(cardinalityMatrixList: List[CardinalityMatrix], inputFileSchemaList: List[CsvHeaderSchema]): List[CardinalityMatrix]= {
    cardinalityMatrixList map { cardinalityMatrix =>
      val filteredColumnHeader: Option[CsvHeaderSchema] = inputFileSchemaList.find { inputFileSchema =>
        inputFileSchema.columnName == cardinalityMatrix.columnName
      }

      filteredColumnHeader match {
        case Some(columnHeader) => cardinalityMatrix.copy(dataType = columnHeader.dataType)
        case _ => throw new Exception("Column Mismatch occurred !!")
      }
    }
  }

  /**
    * This method returns the list of cardinality of each column
    * @param dataFrame
    * @return
    */
  def getCardinalityMatrix(dataFrame: DataFrame, parameters: CommandLineArguments): List[CardinalityMatrix] = {
    val cardinalityProcessor = new CardinalityProcessor()
    val cardinalityMatrixList = dataFrameUtil.getColumnNames(dataFrame) map { columnName =>
      val columnDataFrame = dataFrame.select(columnName)
      val cardinality = cardinalityProcessor.computeCardinality(columnName, dataFrame.select(columnName))
      CardinalityMatrix(columnName, cardinality, columnDataFrame)
    }
    val inputFileSchema = dataFrameUtil.getColumnDataTypes(dataFrame)
    val columnList = dataFrameUtil.getColumnNameFromFileHeader(parameters)
    val cardinalityMatrix = cardinalityProcessor.setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchema)
    columnList.zip(cardinalityMatrix) map {case (column, matrix) => matrix.copy(inputColumnName = column)}
  }

}
