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

package org.carbondata.spark.util

import java.io.IOException
import java.nio.charset.Charset
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.language.implicitConversions
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark.Logging
import org.apache.spark.sql.{CarbonEnv, CarbonRelation, DataFrame, SQLContext}
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.util.FileUtils

import org.carbondata.core.cache.dictionary.Dictionary
import org.carbondata.core.carbon.CarbonDataLoadSchema
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.reader.{CarbonDictionaryReader, CarbonDictionaryReaderImpl, ThriftReader}
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.util.CarbonUtil
import org.carbondata.core.writer.{CarbonDictionaryWriter, CarbonDictionaryWriterImpl}
import org.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriter, CarbonDictionarySortIndexWriterImpl, CarbonDictionarySortInfo, CarbonDictionarySortInfoPreparator}
import org.carbondata.spark.load.CarbonLoaderUtil
import org.carbondata.spark.load.CarbonLoadModel
import org.carbondata.spark.partition.reader.CSVWriter
import org.carbondata.spark.rdd.{ArrayParser, CarbonBlockDistinctValuesCombineRDD, CarbonDataRDDFactory, CarbonGlobalDictionaryGenerateRDD, ColumnPartitioner, DataFormat, DictionaryLoadModel, GenericParser, PrimitiveParser, StructParser}

/**
 * A object which provide a method to generate global dictionary from CSV files.
 */
object GlobalDictionaryUtil extends Logging {

  /**
   * find columns which need to generate global dictionary.
   *
   * @param dimensions dimension list of schema
   * @param columns    column list of csv file
   * @return java.lang.String[]
   */
  def pruneDimensions(dimensions: Array[CarbonDimension],
      headers: Array[String],
      columns: Array[String]): (Array[CarbonDimension], Array[String]) = {
    val dimensionBuffer = new ArrayBuffer[CarbonDimension]
    val columnNameBuffer = new ArrayBuffer[String]
    val dimensionsWithDict = dimensions.filter(hasEncoding(_, Encoding.DICTIONARY,
      Encoding.DIRECT_DICTIONARY))
    dimensionsWithDict.foreach { dim =>
      breakable {
        headers.zipWithIndex.foreach { h =>
          if (dim.getColName.equalsIgnoreCase(h._1)) {
            dimensionBuffer += dim
            columnNameBuffer += columns(h._2)
            break
          }
        }
      }
    }
    (dimensionBuffer.toArray, columnNameBuffer.toArray)
  }

  /**
   * use this method to judge whether CarbonDimension use some encoding or not
   */
  def hasEncoding(dimension: CarbonDimension,
      encoding: Encoding,
      excludeEncoding: Encoding): Boolean = {
    if (dimension.isComplex()) {
      var has = false
      val children = dimension.getListOfChildDimensions
      children.asScala.exists(hasEncoding(_, encoding, excludeEncoding))
    } else {
      dimension.hasEncoding(encoding) &&
      (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding))
    }
  }

  def gatherDimensionByEncoding(dimension: CarbonDimension,
      encoding: Encoding,
      excludeEncoding: Encoding,
      dimensionsWithEncoding: ArrayBuffer[CarbonDimension]) {
    if (dimension.isComplex()) {
      val children = dimension.getListOfChildDimensions.asScala
      children.foreach { c =>
        gatherDimensionByEncoding(c, encoding, excludeEncoding, dimensionsWithEncoding)
      }
    } else {
      if (dimension.hasEncoding(encoding) &&
          (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding))) {
        dimensionsWithEncoding += dimension
      }
    }
  }

  def getPrimDimensionWithDict(dimension: CarbonDimension): Array[CarbonDimension] = {
    val dimensionsWithDict = new ArrayBuffer[CarbonDimension]
    gatherDimensionByEncoding(dimension, Encoding.DICTIONARY, Encoding.DIRECT_DICTIONARY,
      dimensionsWithDict)
    dimensionsWithDict.toArray
  }

  /**
   * invoke CarbonDictionaryWriter to write dictionary to file.
   *
   * @param model       instance of DictionaryLoadModel
   * @param columnIndex the index of current column in column list
   * @param iter        distinct value list of dictionary
   */
  def writeGlobalDictionaryToFile(model: DictionaryLoadModel,
      columnIndex: Int,
      iter: Iterator[String]): Unit = {
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
      model.hdfsLocation, model.table,
      model.primDimensions(columnIndex).getColumnId)
    try {
      while (iter.hasNext) {
        writer.write(iter.next)
      }
    } finally {
      writer.close()
    }
  }

  /**
   * invokes the CarbonDictionarySortIndexWriter to write column sort info
   * sortIndex and sortIndexInverted data to sortinsex file.
   *
   */
  def writeGlobalDictionaryColumnSortInfo(model: DictionaryLoadModel,
      index: Int,
      dictionary: Dictionary): Unit = {
    val preparator: CarbonDictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator
    val dictionarySortInfo: CarbonDictionarySortInfo =
      preparator.getDictionarySortInfo(dictionary,
        model.primDimensions(index).getDataType)
    val carbonDictionaryWriter: CarbonDictionarySortIndexWriter =
      new CarbonDictionarySortIndexWriterImpl(model.table,
        model.primDimensions(index).getColumnId, model.hdfsLocation)
    try {
      carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex)
      carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted)
    } finally {
      carbonDictionaryWriter.close()
    }
  }

  /**
   * read global dictionary from cache
   */
  def readGlobalDictionaryFromCache(model: DictionaryLoadModel): HashMap[String, Dictionary] = {
    val dictMap = new HashMap[String, Dictionary]
    model.primDimensions.zipWithIndex.filter(f => model.dictFileExists(f._2)).foreach { m =>
      val dict = CarbonLoaderUtil.getDictionary(model.table,
        m._1.getColumnId, model.hdfsLocation,
        m._1.getDataType
      )
      dictMap.put(m._1.getColumnId, dict)
    }
    dictMap
  }

  /**
   * invoke CarbonDictionaryReader to read dictionary from files.
   */
  def readGlobalDictionaryFromFile(model: DictionaryLoadModel): HashMap[String, HashSet[String]] = {
    val dictMap = new HashMap[String, HashSet[String]]
    for (i <- model.primDimensions.indices) {
      val set = new HashSet[String]
      if (model.dictFileExists(i)) {
        val reader: CarbonDictionaryReader = new CarbonDictionaryReaderImpl(
          model.hdfsLocation, model.table, model.primDimensions(i).getColumnId)
        val values = reader.read
        if (values != null) {
          for (j <- 0 until values.size) {
            set.add(new String(values.get(j),
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)))
          }
        }
      }
      dictMap.put(model.primDimensions(i).getColumnId, set)
    }
    dictMap
  }

  def generateParserForChildrenDimension(dim: CarbonDimension,
      format: DataFormat,
      mapColumnValuesWithId:
      HashMap[String, HashSet[String]],
      generic: GenericParser): Unit = {
    val children = dim.getListOfChildDimensions.asScala
    for (i <- children.indices) {
      generateParserForDimension(Some(children(i)), format.cloneAndIncreaseIndex,
        mapColumnValuesWithId) match {
        case Some(childDim) =>
          generic.addChild(childDim)
        case None =>
      }
    }
  }

  def generateParserForDimension(dimension: Option[CarbonDimension],
      format: DataFormat,
      mapColumnValuesWithId: HashMap[String, HashSet[String]]): Option[GenericParser] = {
    dimension match {
      case None =>
        None
      case Some(dim) =>
        dim.getDataType match {
          case DataType.ARRAY =>
            val arrDim = ArrayParser(dim, format)
            generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, arrDim)
            Some(arrDim)
          case DataType.STRUCT =>
            val stuDim = StructParser(dim, format)
            generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, stuDim)
            Some(stuDim)
          case _ =>
            Some(PrimitiveParser(dim, mapColumnValuesWithId.get(dim.getColumnId)))
        }
    }
  }

  def createDataFormat(delimiters: Array[String]): DataFormat = {
    if (ArrayUtils.isNotEmpty(delimiters)) {
      val patterns = delimiters.map { d =>
        Pattern.compile(if (d == null) {
          ""
        } else {
          d
        })
      }
      DataFormat(delimiters, 0, patterns)
    } else {
      null
    }
  }

  def isHighCardinalityColumn(columnCardinality: Int,
                              rowCount: Long,
                              model: DictionaryLoadModel): Boolean = {
    (columnCardinality > model.highCardThreshold) && (rowCount > 0) &&
      (columnCardinality.toDouble / rowCount * 100 > model.rowCountPercentage)
  }

  /**
   * create a instance of DictionaryLoadModel
   *
   * @param table          CarbonTableIdentifier
   * @param dimensions    column list
   * @param hdfsLocation   store location in HDFS
   * @param dictfolderPath path of dictionary folder
   * @return: org.carbondata.spark.rdd.DictionaryLoadModel
   */
  def createDictionaryLoadModel(carbonLoadModel: CarbonLoadModel,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension],
      hdfsLocation: String,
      dictfolderPath: String): DictionaryLoadModel = {
    val primDimensionsBuffer = new ArrayBuffer[CarbonDimension]
    val isComplexes = new ArrayBuffer[Boolean]
    for (i <- dimensions.indices) {
      val dims = getPrimDimensionWithDict(dimensions(i))
      for (j <- dims.indices) {
        primDimensionsBuffer += dims(j)
        isComplexes += dimensions(i).isComplex
      }
    }
    val primDimensions = primDimensionsBuffer.map { x => x }.toArray
    // list dictionary file path
    val dictFilePaths = new Array[String](primDimensions.length)
    val dictFileExists = new Array[Boolean](primDimensions.length)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(hdfsLocation, table)
    primDimensions.zipWithIndex.foreach{f =>
      dictFilePaths(f._2) = carbonTablePath.getDictionaryFilePath(f._1.getColumnId)
      dictFileExists(f._2) = CarbonUtil.isFileExists(dictFilePaths(f._2))
    }

    // load high cardinality identify configure
    val highCardIdentifyEnable = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE,
        CarbonCommonConstants.HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT).toBoolean
    val highCardThreshold = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD,
        CarbonCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT).toInt
    val rowCountPercentage = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE,
        CarbonCommonConstants.HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT).toDouble

    // get load count
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CarbonDataRDDFactory.readLoadMetadataDetails(carbonLoadModel, hdfsLocation)
    }
    new DictionaryLoadModel(table,
      dimensions,
      hdfsLocation,
      dictfolderPath,
      dictFilePaths,
      dictFileExists,
      isComplexes.toArray,
      primDimensions,
      carbonLoadModel.getDelimiters,
      highCardIdentifyEnable,
      highCardThreshold,
      rowCountPercentage,
      carbonLoadModel.getLoadMetadataDetails.size() == 0)
  }

  /**
   * load CSV files to DataFrame by using datasource "com.databricks.spark.csv"
   *
   * @param sqlContext SQLContext
   * @param carbonLoadModel   CarbonLoadModel
   * @return: org.apache.spark.sql.DataFrame
   */
  def loadDataFrame(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel): DataFrame = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv.newapi")
      .option("header", {
        if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) {
          "true"
        }
        else {
          "false"
        }
      })
      .option("delimiter", {
        if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter)) {
          "" + CSVWriter.DEFAULT_SEPARATOR
        }
        else {
          carbonLoadModel.getCsvDelimiter
        }
      })
      .option("parserLib", "univocity")
      .option("escape", carbonLoadModel.getEscapeChar)
      .load(carbonLoadModel.getFactFilePath)
    df
  }

  private def updateTableMetadata(carbonLoadModel: CarbonLoadModel,
                                  sqlContext: SQLContext,
                                  model: DictionaryLoadModel,
                                  noDictDimension: Array[CarbonDimension]): Unit = {

    val carbonTablePath = CarbonStorePath.getCarbonTablePath(model.hdfsLocation,
      model.table)
    val schemaFilePath = carbonTablePath.getSchemaFilePath

    // read TableInfo
    val tableInfo = CarbonMetastoreCatalog.readSchemaFileToThriftTable(schemaFilePath)

    // modify TableInfo
    val columns = tableInfo.getFact_table.getTable_columns
    for (i <- 0 until columns.size) {
      if (noDictDimension.exists(x => columns.get(i).getColumn_id.equals(x.getColumnId))) {
        columns.get(i).encoders.remove(org.carbondata.format.Encoding.DICTIONARY)
      }
    }

    // write TableInfo
    CarbonMetastoreCatalog.writeThriftTableToSchemaFile(schemaFilePath, tableInfo)

    // update Metadata
    val catalog = CarbonEnv.getInstance(sqlContext).carbonCatalog
    catalog.updateMetadataByThriftTable(schemaFilePath, tableInfo,
        model.table.getDatabaseName, model.table.getTableName, carbonLoadModel.getStorePath)

    // update CarbonDataLoadSchema
    val carbonTable = catalog.lookupRelation1(Option(model.table.getDatabaseName),
          model.table.getTableName, None)(sqlContext)
        .asInstanceOf[CarbonRelation].cubeMeta.carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))

  }

  /**
   * check whether global dictionary have been generated successfully or not.
   *
   * @param status Array[(String, String)]
   * @return: void
   */
  private def checkStatus(carbonLoadModel: CarbonLoadModel,
                          sqlContext: SQLContext,
                          model: DictionaryLoadModel,
                          status: Array[(Int, String, Boolean)]) = {
    var result = false
    val noDictionaryColumns = new ArrayBuffer[CarbonDimension]
    val tableName = model.table.getTableName
    status.foreach { x =>
      val columnName = model.primDimensions(x._1).getColName
      if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(x._2)) {
        result = true
        logError(s"table:$tableName column:$columnName generate global dictionary file failed")
      }
      if (x._3) noDictionaryColumns +=  model.primDimensions(x._1)
    }
    if (noDictionaryColumns.nonEmpty) {
      updateTableMetadata(carbonLoadModel, sqlContext, model, noDictionaryColumns.toArray)
    }
    if (result) {
      logError("generate global dictionary files failed")
      throw new Exception("Failed to generate global dictionary files")
    } else {
      logInfo("generate global dictionary successfully")
    }
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   *
   */
  def generateGlobalDictionary(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      hdfsLocation: String): Unit = {
    try {
      val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier

      // create dictionary folder if not exists
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(hdfsLocation, table)
      val dictfolderPath = carbonTablePath.getMetadataDirectoryPath
      val created = CarbonUtil.checkAndCreateFolder(dictfolderPath)
      if (!created) {
        logError("Dictionary Folder creation status :: " + created)
        throw new IOException("Failed to created dictionary folder")
      }
      // load data by using dataSource com.databricks.spark.csv
      var df = loadDataFrame(sqlContext, carbonLoadModel)
      // columns which need to generate global dictionary file
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val dimensions = carbonTable.getDimensionByTableName(
        carbonTable.getFactTableName).asScala.toArray
      val headers = if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) {
        df.columns
      }
      else {
        carbonLoadModel.getCsvHeader.split("" + CSVWriter.DEFAULT_SEPARATOR)
      }
      val (requireDimension, requireColumnNames) = pruneDimensions(dimensions, headers, df.columns)
      if (requireDimension.nonEmpty) {
        // select column to push down pruning
        df = df.select(requireColumnNames.head, requireColumnNames.tail: _*)
        val model = createDictionaryLoadModel(carbonLoadModel, table, requireDimension,
          hdfsLocation, dictfolderPath)
        // combine distinct value in a block and partition by column
        val inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, model)
          .partitionBy(new ColumnPartitioner(model.primDimensions.length))
        // generate global dictionary files
        val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
        // check result status
        checkStatus(carbonLoadModel, sqlContext, model, statusList)
      } else {
        logInfo("have no column need to generate global dictionary")
      }
      // generate global dict from dimension file
      if (carbonLoadModel.getDimFolderPath != null) {
        val fileMapArray = carbonLoadModel.getDimFolderPath.split(",")
        for (fileMap <- fileMapArray) {
          val dimTableName = fileMap.split(":")(0)
          var dimDataframe = loadDataFrame(sqlContext, carbonLoadModel)
          val (requireDimensionForDim, requireColumnNamesForDim) =
            pruneDimensions(dimensions, dimDataframe.columns, dimDataframe.columns)
          if (requireDimensionForDim.length >= 1) {
            dimDataframe = dimDataframe.select(requireColumnNamesForDim.head,
              requireColumnNamesForDim.tail: _*)
            val modelforDim = createDictionaryLoadModel(carbonLoadModel, table,
              requireDimensionForDim, hdfsLocation, dictfolderPath)
            val inputRDDforDim = new CarbonBlockDistinctValuesCombineRDD(
              dimDataframe.rdd, modelforDim)
              .partitionBy(new ColumnPartitioner(modelforDim.primDimensions.length))
            val statusListforDim = new CarbonGlobalDictionaryGenerateRDD(
              inputRDDforDim, modelforDim).collect()
            checkStatus(carbonLoadModel, sqlContext, modelforDim, statusListforDim)
          } else {
            logInfo(s"No columns in dimension table $dimTableName to generate global dictionary")
          }
        }
      }
    } catch {
      case ex: Exception =>
        logError("generate global dictionary failed")
        throw ex
    }
  }

  def generateAndWriteNewDistinctValueList(valuesBuffer: mutable.HashSet[String],
      dictionary: Dictionary,
      model: DictionaryLoadModel, columnIndex: Int): Int = {
    val values = valuesBuffer.toArray
    java.util.Arrays.sort(values, Ordering[String])
    var distinctValueCount: Int = 0
    val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
      model.hdfsLocation, model.table,
      model.primDimensions(columnIndex).getColumnId)
    try {
      if (!model.dictFileExists(columnIndex)) {
        writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
        distinctValueCount += 1
      }

      if (values.length >= 1) {
        var preValue = values(0)
        if (model.dictFileExists(columnIndex)) {
          if (dictionary.getSurrogateKey(values(0)) == CarbonCommonConstants
            .INVALID_SURROGATE_KEY) {
            writer.write(values(0))
            distinctValueCount += 1
          }
          for (i <- 1 until values.length) {
            if (preValue != values(i)) {
              if (dictionary.getSurrogateKey(values(i)) ==
                  CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                writer.write(values(i))
                preValue = values(i)
                distinctValueCount += 1
              }
            }
          }

        } else {
          writer.write(values(0))
          distinctValueCount += 1
          for (i <- 1 until values.length) {
            if (preValue != values(i)) {
              writer.write(values(i))
              preValue = values(i)
              distinctValueCount += 1
            }
          }
        }
      }
    } finally {
      writer.close()
    }
    distinctValueCount
  }
}
