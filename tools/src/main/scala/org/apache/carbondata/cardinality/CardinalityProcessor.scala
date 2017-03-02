package org.apache.carbondata.cardinality

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.{CommandLineArguments, CsvHeaderSchema, DataFrameUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}


/**
  * @param columnName : ColumnName As stored in dataframe
  * @param cardinality : Cardinality for a particular column
  * @param columnDataframe : DataFrame of the column
  * @param dataType : datatype for the column
  * @param inputColumnName : Name of column as given in FileHeaders(command line arguments)
  */
case class CardinalityMatrix(columnName: String, cardinality: Double, columnDataframe: DataFrame, dataType: DataType = StringType, inputColumnName: String = "")

trait CardinalityProcessor{

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val dataFrameUtil = new DataFrameUtil

  def computeCardinality(columnName: String, columnDataFrame: DataFrame): Double = {
    LOGGER.info(s"Computing Cardinality for Column $columnName")
    if(columnDataFrame.count == 0){
      LOGGER.warn(s"Computing Cardinality for column with no data")
      0
    } else {
      val uniqueData = columnDataFrame.distinct
      uniqueData.count.toDouble / columnDataFrame.count
    }
  }

  //TODO: Validation needs to be added so that no two columns in csv have same name
  def setDataTypeWithCardinality(cardinalityMatrixList: List[CardinalityMatrix], inputFileSchemaList: List[CsvHeaderSchema]): List[CardinalityMatrix]= {
    cardinalityMatrixList map { cardinalityMatrix =>
      val filteredColumnHeader: Option[CsvHeaderSchema] = inputFileSchemaList.find { inputFileSchema =>
        inputFileSchema.columnName == cardinalityMatrix.columnName
      }

      filteredColumnHeader match {
        case Some(columnHeader) => cardinalityMatrix.copy(dataType = columnHeader.dataType)
        case _ => throw new IllegalArgumentException("Column Mismatch occurred !!")
      }
    }
  }

  /**
    * This method returns the list of cardinality of each column
    * @param dataFrame
    * @return
    */
  def getCardinalityMatrix(dataFrame: DataFrame, parameters: CommandLineArguments): List[CardinalityMatrix] = {
    val cardinalityMatrixList = dataFrameUtil.getColumnNames(dataFrame) map { columnName =>
      val columnDataFrame = dataFrame.select(columnName)
      val cardinality = computeCardinality(columnName, dataFrame.select(columnName))
      CardinalityMatrix(columnName, cardinality, columnDataFrame)
    }
    val inputFileSchema = dataFrameUtil.getColumnDataTypes(dataFrame)
    val columnList = dataFrameUtil.getColumnNameFromFileHeader(parameters, dataFrame.schema.fields.length)
    val cardinalityMatrix = setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchema)
    columnList.zip(cardinalityMatrix) map { case (column, matrix) => matrix.copy(inputColumnName = column)}
  }

}

object CardinalityProcessor extends CardinalityProcessor
