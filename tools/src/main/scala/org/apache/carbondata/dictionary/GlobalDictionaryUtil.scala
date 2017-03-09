package org.apache.carbondata.dictionary

import java.util

import org.apache.spark.sql.Row

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, CarbonMeasure}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriterImpl,
CarbonDictionarySortInfoPreparator}

trait GlobalDictionaryUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

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

  private def identifyDictionaryColumns(cardinalityMatrix: List[CardinalityMatrix],
      dimensions: util.List[CarbonDimension]): (Array[Set[String]], List[String]) = {
    val dimArrSet: Array[Set[String]] = new Array[Set[String]](dimensions.size())
    val dictColumnNames: List[String] = cardinalityMatrix
      .filter(cardMatrix => isDictionaryColumn(cardMatrix.cardinality)).zipWithIndex
      .map { case (columnCardinality, index) =>
        dimArrSet(index) = Set[String]()
        columnCardinality.columnDataframe.distinct.collect().map { (elem: Row) =>
          dimArrSet(index) += elem.get(0).toString
        }
        columnCardinality.columnName
      }
    (dimArrSet, dictColumnNames)
  }

  def getStorePath(storePath: String = "./target/store/T1"): String = {
    storePath
  }

  private def getDictFilePath(dictColumnName: String): String = {
    getStorePath() + "/Metadata/" + dictColumnName + ".dict"
  }

  private def writeDictionaryToFile(absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimArrSet: Array[Set[String]],
      dimensions: util.List[CarbonDimension],
      dictColumnNames: List[String]): Unit = {
    val dictCache: Cache[java.lang.Object, Dictionary] = CacheProvider.getInstance()
      .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath)
    dimArrSet.zipWithIndex.foreach { case (dimSet, index) =>
      val columnIdentifier: ColumnIdentifier = new ColumnIdentifier(dimensions.get(index)
        .getColumnId, null, null)
      val writer: CarbonDictionaryWriterImpl = dictionaryWriter(columnIdentifier,
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
        LOGGER.info("Creating Dictionary from source files")
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
        LOGGER.info("Dictionary Generation for incremental load")
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
  }

  private def checkDistinctValue(dimensionValue: String, dict: Dictionary): Boolean = {
    dimensionValue != null &&
    dict.getSurrogateKey(dimensionValue) == CarbonCommonConstants.INVALID_SURROGATE_KEY
  }

  private def generateDictionaryForInitialLoad(writer: CarbonDictionaryWriterImpl,
      dimensionSet: Set[String]): List[String] = {
    writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
    dimensionSet.map(elem => writer.write(elem))
    writer.close()
    writer.commit()
    (dimensionSet + CarbonCommonConstants.MEMBER_DEFAULT_VAL).toList
  }

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

  private def dictionaryWriter(columnIdentifier: ColumnIdentifier,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      dimensions: util.List[CarbonDimension],
      index: Int): CarbonDictionaryWriterImpl = {
    val writer: CarbonDictionaryWriterImpl = new CarbonDictionaryWriterImpl(absoluteTableIdentifier
      .getStorePath,
      absoluteTableIdentifier.getCarbonTableIdentifier,
      columnIdentifier)
    writer
  }

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

  def isDictionaryColumn(cardinality: Double): Boolean = {
    val cardinalityThreshold = 0.8
    if (cardinality > cardinalityThreshold) {
      false
    } else {
      true
    }
  }
}

object GlobalDictionaryUtil extends GlobalDictionaryUtil
