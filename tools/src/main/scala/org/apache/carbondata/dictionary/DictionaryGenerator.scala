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

package org.apache.carbondata.dictionary

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.common.CarbonToolConstants
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, CarbonMeasure}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriterImpl,
CarbonDictionarySortInfoPreparator}

trait DictionaryGenerator {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * This method initiates dictionary creation process
   *
   * @param carbonTable
   * @param cardinalityMatrix
   * @param absoluteTableIdentifier
   */
  def writeDictionary(carbonTable: CarbonTable,
      cardinalityMatrix: List[CardinalityMatrix],
      absoluteTableIdentifier: AbsoluteTableIdentifier): Unit = {

    val dimensions: util.List[CarbonDimension] = carbonTable
      .getDimensionByTableName(carbonTable.getFactTableName)

    // TODO List of Columns of type measures
    val measures: util.List[CarbonMeasure] = carbonTable
      .getMeasureByTableName(carbonTable.getFactTableName)
    val (dimArrSet: Array[Set[String]], dictColumnNames) = identifyDictionaryColumns(
      cardinalityMatrix,
      dimensions)
    writeDictionaryToFile(absoluteTableIdentifier, dimArrSet, dimensions, dictColumnNames)
  }

  /**
   * This method returns the Column values for Dictionary Creation
   *
   * @param cardinalityMatrix
   * @param dimensions
   * @return
   */
  private def identifyDictionaryColumns(cardinalityMatrix: List[CardinalityMatrix],
      dimensions: util.List[CarbonDimension]): (Array[Set[String]], List[String]) = {
    val dimArrSet: Array[Set[String]] = new Array[Set[String]](dimensions.size())
    val dictColumnNames: List[String] = cardinalityMatrix
      .filter(cardMatrix => isDictionaryColumn(cardMatrix.cardinality, cardMatrix.dataType))
      .zipWithIndex
      .map { case (columnCardinality, index) =>
        dimArrSet(index) = Set[String]()
        columnCardinality.columnDataframe.distinct.collect().map { (elem: Row) =>
          dimArrSet(index) += elem.get(CarbonToolConstants.Zero).toString
        }
        columnCardinality.columnName
      }
    (dimArrSet, dictColumnNames)
  }

  /**
   * This method returns the Dictionary Store location
   *
   * @param storePath
   * @return
   */
  def getStorePath(storePath: String = CarbonToolConstants.DefaultStorePath): String = {
    storePath
  }

  /**
   * This method returns the dictionary file path for a specific column
   *
   * @param dictColumnName
   * @return
   */
  private def getDictFilePath(dictColumnName: String): String = {
    getStorePath() + CarbonToolConstants.DictionaryFolder + dictColumnName +
    CarbonToolConstants.DictionaryExtension
  }

  /**
   * This method writes the dictionary to the File Path
   *
   * @param absoluteTableIdentifier
   * @param dimArrSet
   * @param dimensions
   * @param dictColumnNames
   */
  private def writeDictionaryToFile(absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimArrSet: Array[Set[String]],
      dimensions: util.List[CarbonDimension],
      dictColumnNames: List[String]): Unit = {
    val dictCache: Cache[java.lang.Object, Dictionary] = CacheProvider.getInstance()
      .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath)
    LOGGER.info("Creating Dictionary")
    dimArrSet.zipWithIndex.foreach { case (dimSet, index) =>
      val columnIdentifier: ColumnIdentifier = new ColumnIdentifier(dimensions.get(index)
        .getColumnId, null, null)
      val writer: CarbonDictionaryWriterImpl = getDictionaryWriter(columnIdentifier,
        absoluteTableIdentifier,
        dimensions,
        index)
      val dictFilePath = getDictFilePath(dictColumnNames(index))
      val fileType = FileFactory.getFileType(dictFilePath)

      val dict: Dictionary = dictCache
        .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier,
          columnIdentifier,
          dimensions.get(index).getDataType))

      val dimensionSet: Set[String] = collection.immutable.SortedSet[String]() ++ dimSet

      // Dictionary Generation from Source Data Files
      if (!FileFactory.isFileExist(dictFilePath, fileType)) {
        val distinctVal: List[String] = generateDictionaryForInitialLoad(writer, dimensionSet)
        sortIndexWriter(dict,
          columnIdentifier,
          absoluteTableIdentifier,
          dimensions,
          index,
          distinctVal)
      }
      else {
        //Dictionary Generation For Incremental Data Load
        val distinctVal: List[String] = generateDictionaryForIncrementalLoad(writer,
          dimensionSet,
          dict)
        sortIndexWriter(dict,
          columnIdentifier,
          absoluteTableIdentifier,
          dimensions,
          index,
          distinctVal)
      }
    }
    LOGGER.info("Dictionary Created Successfully.")
  }

  /**
   * This method identifies the distinct values for a particular dimension
   *
   * @param dimensionValue
   * @param dict
   * @return
   */
  private def checkDistinctValue(dimensionValue: String, dict: Dictionary): Boolean = {
    dimensionValue != null &&
    dict.getSurrogateKey(dimensionValue) == CarbonToolConstants.InvalidSurrogateKey
  }

  /**
   * This method writes dictionary for Initial Loading
   *
   * @param writer
   * @param dimensionSet
   * @return
   */
  private def generateDictionaryForInitialLoad(writer: CarbonDictionaryWriterImpl,
      dimensionSet: Set[String]): List[String] = {
    writer.write(CarbonToolConstants.MemberDefaultVal)
    dimensionSet.map(elem => writer.write(elem))
    writer.close()
    writer.commit()
    (dimensionSet + CarbonToolConstants.MemberDefaultVal).toList
  }

  /**
   * This method writes dictionary for Incremental Loading
   *
   * @param writer
   * @param dimensionSet
   * @param dict
   * @return
   */
  private def generateDictionaryForIncrementalLoad(writer: CarbonDictionaryWriterImpl,
      dimensionSet: Set[String],
      dict: Dictionary): List[String] = {
    val distinctVal: Set[String] = dimensionSet.filter(dimVal => checkDistinctValue(dimVal, dict))
      .map { dimensionValue =>
        writer.write(dimensionValue)
        dimensionValue
      }
    writer.close()
    writer.commit()
    distinctVal.toList
  }

  /**
   * This method returns Dictionary writer
   *
   * @param columnIdentifier
   * @param absoluteTableIdentifier
   * @param dimensions
   * @param index
   * @return
   */
  private def getDictionaryWriter(columnIdentifier: ColumnIdentifier,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimensions: util.List[CarbonDimension],
      index: Int): CarbonDictionaryWriterImpl = {
    val writer: CarbonDictionaryWriterImpl = new CarbonDictionaryWriterImpl(absoluteTableIdentifier
      .getStorePath,
      absoluteTableIdentifier.getCarbonTableIdentifier,
      columnIdentifier)
    writer
  }

  /**
   * This method writes the Sort Index file
   *
   * @param dict
   * @param columnIdentifier
   * @param absoluteTableIdentifier
   * @param dimensions
   * @param index
   * @param distinctValues
   */
  private def sortIndexWriter(dict: Dictionary,
      columnIdentifier: ColumnIdentifier,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimensions: util.List[CarbonDimension],
      index: Int, distinctValues: List[String]): Unit = {

    import scala.collection.JavaConverters._

    LOGGER.info("Writing sort index file")
    val dictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator()
    val carbonDictionarySortInfo = dictionarySortInfoPreparator
      .getDictionarySortInfo(distinctValues.asJava, dict, dimensions.get(index).getDataType)

    val carbonDictionarySortIndexWriter = new CarbonDictionarySortIndexWriterImpl(
      absoluteTableIdentifier.getCarbonTableIdentifier,
      columnIdentifier,
      absoluteTableIdentifier.getStorePath)
    try {
      carbonDictionarySortIndexWriter.writeSortIndex(carbonDictionarySortInfo.getSortIndex())
      carbonDictionarySortIndexWriter
        .writeInvertedSortIndex(carbonDictionarySortInfo.getSortIndexInverted())
    } finally {
      carbonDictionarySortIndexWriter.close()
    }
  }

  import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}

  /**
   * This method identifies Dictionary Columns
   *
   * @param cardinality
   * @param dataType
   * @return
   */
  def isDictionaryColumn(cardinality: Double,
      dataType: org.apache.spark.sql.types.DataType): Boolean = {
    if (parseDataType(dataType) == CarbonDataType.TIMESTAMP ||
        parseDataType(dataType) == CarbonDataType.DATE) {
      false
    } else {
      cardinality <= CarbonToolConstants.CardinalityThreshold
    }
  }

  /**
   * This method parses Spark data type to CarbonData Type
   *
   * @param dataType
   * @return
   */
  def parseDataType(dataType: org.apache.spark.sql.types.DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.STRING
      case FloatType => CarbonDataType.FLOAT
      case IntegerType => CarbonDataType.INT
      case ByteType => CarbonDataType.SHORT
      case ShortType => CarbonDataType.SHORT
      case DoubleType => CarbonDataType.DOUBLE
      case LongType => CarbonDataType.LONG
      case BooleanType => CarbonDataType.BOOLEAN
      case DateType => CarbonDataType.DATE
      case DecimalType.USER_DEFAULT => CarbonDataType.DECIMAL
      case TimestampType => CarbonDataType.TIMESTAMP
      case _ => CarbonDataType.STRING
    }
  }
}

object DictionaryGenerator extends DictionaryGenerator
