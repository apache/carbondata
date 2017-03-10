package org.apache.carbondata.cardinality

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.utils.{CsvHeaderSchema, DataFrameUtil, LoadProperties}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}


/**
  * @param columnName : ColumnName As stored in dataframe
  * @param cardinality : Cardinality for a particular column
  * @param columnDataframe : DataFrame of the column
  * @param dataType : datatype for the column
  * @param inputColumnName : Name of column as given in FileHeaders(command line arguments)
  */
case class CardinalityMatrix(columnName: String, cardinality: Double, columnDataframe: DataFrame, dataType: DataType = StringType, inputColumnName: Option[String] = None)

trait CardinalityProcessor{

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val dataFrameUtil: DataFrameUtil

  /**
    * This method computes Cardinality for a column
    * @param columnName Name of the column whose cardinality needs to be computed
    * @param columnDataFrame Dataframe Of Column records
    * @return Cardinality Value
    */
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

  /**
    * This method adds datatype for each column
    *
    * @param cardinalityMatrixList
    * @param inputFileSchemaList
    * @return
    */
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
    *
    * This method returns the list of cardinality of each column
    * @param dataFrame
    * @return
    */
  def getCardinalityMatrix(dataFrame: DataFrame, properties: LoadProperties): List[CardinalityMatrix] = {
    val cardinalityMatrixList = dataFrameUtil.getColumnNames(dataFrame) map { columnName =>
      val columnDataFrame = dataFrame.select(columnName)
      val cardinality = computeCardinality(columnName, dataFrame.select(columnName))
      CardinalityMatrix(columnName, cardinality, columnDataFrame)
    }

    val inputFileSchema = dataFrameUtil.getColumnDataTypes(dataFrame)
    val columnList = dataFrameUtil.getColumnNameFromFileHeader(properties, dataFrame.schema.fields.length)
    val cardinalityMatrix = setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchema)
    columnList.zip(cardinalityMatrix) map { case (column, matrix) => matrix.copy(inputColumnName = Some(column))}
  }

}

object CardinalityProcessor extends CardinalityProcessor{
  val dataFrameUtil:DataFrameUtil = DataFrameUtil
}
