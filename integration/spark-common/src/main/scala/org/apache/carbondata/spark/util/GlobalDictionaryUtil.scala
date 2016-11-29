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

package org.apache.carbondata.spark.util

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.Charset
import java.util.regex.Pattern

import org.apache.carbondata.common.factory.CarbonCommonFactory
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.reader.CarbonDictionaryReader
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.writer.CarbonDictionaryWriter
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd._
import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.util.FileUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.language.implicitConversions
import scala.util.control.Breaks.{break, breakable}

/**
 * A object which provide a method to generate global dictionary from CSV files.
 */
object GlobalDictionaryUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * The default separator to use if none is supplied to the constructor.
   */
  val DEFAULT_SEPARATOR: Char = ','
  /**
   * The default quote character to use if none is supplied to the
   * constructor.
   */
  val DEFAULT_QUOTE_CHARACTER: Char = '"'

  /**
   * find columns which need to generate global dictionary.
   *
   * @param dimensions dimension list of schema
   * @param headers    column headers
   * @param columns    column list of csv file
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
   *
   * @param dimension       carbonDimension
   * @param encoding        the coding way of dimension
   * @param excludeEncoding the coding way to exclude
   */
  def hasEncoding(dimension: CarbonDimension,
      encoding: Encoding,
      excludeEncoding: Encoding): Boolean = {
    if (dimension.isComplex()) {
      val children = dimension.getListOfChildDimensions
      children.asScala.exists(hasEncoding(_, encoding, excludeEncoding))
    } else {
      dimension.hasEncoding(encoding) &&
      (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding))
    }
  }

  def gatherDimensionByEncoding(carbonLoadModel: CarbonLoadModel,
      dimension: CarbonDimension,
      encoding: Encoding,
      excludeEncoding: Encoding,
      dimensionsWithEncoding: ArrayBuffer[CarbonDimension],
      forPreDefDict: Boolean) {
    if (dimension.isComplex) {
      val children = dimension.getListOfChildDimensions.asScala
      children.foreach { c =>
        gatherDimensionByEncoding(carbonLoadModel, c, encoding, excludeEncoding,
          dimensionsWithEncoding, forPreDefDict)
      }
    } else {
      if (dimension.hasEncoding(encoding) &&
          (excludeEncoding == null || !dimension.hasEncoding(excludeEncoding))) {
        if ((forPreDefDict && carbonLoadModel.getPredefDictFilePath(dimension) != null) ||
            (!forPreDefDict && carbonLoadModel.getPredefDictFilePath(dimension) == null)) {
          dimensionsWithEncoding += dimension
        }
      }
    }
  }

  def getPrimDimensionWithDict(carbonLoadModel: CarbonLoadModel,
      dimension: CarbonDimension,
      forPreDefDict: Boolean): Array[CarbonDimension] = {
    val dimensionsWithDict = new ArrayBuffer[CarbonDimension]
    gatherDimensionByEncoding(carbonLoadModel, dimension, Encoding.DICTIONARY,
      Encoding.DIRECT_DICTIONARY,
      dimensionsWithDict, forPreDefDict)
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
    val dictService = CarbonCommonFactory.getDictionaryService
    val writer: CarbonDictionaryWriter = dictService.getDictionaryWriter(
      model.table,
      model.columnIdentifier(columnIndex),
      model.hdfsLocation
    )
    try {
      while (iter.hasNext) {
        writer.write(iter.next)
      }
    } finally {
      writer.close()
    }
  }

  /**
   * read global dictionary from cache
   */
  def readGlobalDictionaryFromCache(model: DictionaryLoadModel): HashMap[String, Dictionary] = {
    val dictMap = new HashMap[String, Dictionary]
    model.primDimensions.zipWithIndex.filter(f => model.dictFileExists(f._2)).foreach { m =>
      val dict = CarbonLoaderUtil.getDictionary(model.table,
        m._1.getColumnIdentifier, model.hdfsLocation,
        m._1.getDataType
      )
      dictMap.put(m._1.getColumnId, dict)
    }
    dictMap
  }

  /**
   * invoke CarbonDictionaryReader to read dictionary from files.
   *
   * @param model carbon dictionary load model
   */
  def readGlobalDictionaryFromFile(model: DictionaryLoadModel): HashMap[String, HashSet[String]] = {
    val dictMap = new HashMap[String, HashSet[String]]
    val dictService = CarbonCommonFactory.getDictionaryService
    for (i <- model.primDimensions.indices) {
      val set = new HashSet[String]
      if (model.dictFileExists(i)) {
        val reader: CarbonDictionaryReader = dictService.getDictionaryReader(model.table,
          model.columnIdentifier(i), model.hdfsLocation
        )
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
    (columnCardinality > model.highCardThreshold) &&
    (rowCount > 0) &&
    (columnCardinality.toDouble / rowCount * 100 > model.rowCountPercentage)
  }

  /**
   * create a instance of DictionaryLoadModel
   *
   * @param carbonLoadModel carbon load model
   * @param table           CarbonTableIdentifier
   * @param dimensions      column list
   * @param hdfsLocation    store location in HDFS
   * @param dictfolderPath  path of dictionary folder
   */
  def createDictionaryLoadModel(carbonLoadModel: CarbonLoadModel,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension],
      hdfsLocation: String,
      dictfolderPath: String,
      forPreDefDict: Boolean): DictionaryLoadModel = {
    val primDimensionsBuffer = new ArrayBuffer[CarbonDimension]
    val isComplexes = new ArrayBuffer[Boolean]
    for (i <- dimensions.indices) {
      val dims = getPrimDimensionWithDict(carbonLoadModel, dimensions(i), forPreDefDict)
      for (j <- dims.indices) {
        primDimensionsBuffer += dims(j)
        isComplexes += dimensions(i).isComplex
      }
    }
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(hdfsLocation, table)
    val primDimensions = primDimensionsBuffer.map { x => x }.toArray
    val dictDetail = CarbonSparkFactory.getDictionaryDetailService().
      getDictionaryDetail(dictfolderPath, primDimensions, table, hdfsLocation)
    val dictFilePaths = dictDetail.dictFilePaths
    val dictFileExists = dictDetail.dictFileExists
    val columnIdentifier = dictDetail.columnIdentifiers
    val hdfsTempLocation = CarbonProperties.getInstance.
      getProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION, System.getProperty("java.io.tmpdir"))
    val lockType = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    val zookeeperUrl = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.ZOOKEEPER_URL)
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

    val serializationNullFormat =
      carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
    // get load count
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel, hdfsLocation)
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
      columnIdentifier,
      carbonLoadModel.getLoadMetadataDetails.size() == 0,
      hdfsTempLocation,
      lockType,
      zookeeperUrl,
      serializationNullFormat)
  }

  /**
   * load CSV files to DataFrame by using datasource "com.databricks.spark.csv"
   *
   * @param sqlContext      SQLContext
   * @param carbonLoadModel carbon data load model
   */
  def loadDataFrame(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel): DataFrame = {
    val df = sqlContext.read
      .format("com.databricks.spark.csv.newapi")
      .option("header", {
        if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) {
          "true"
        } else {
          "false"
        }
      })
      .option("delimiter", {
        if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter)) {
          "" + DEFAULT_SEPARATOR
        } else {
          carbonLoadModel.getCsvDelimiter
        }
      })
      .option("parserLib", "univocity")
      .option("escape", carbonLoadModel.getEscapeChar)
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .option("codec", "gzip")
      .option("quote", {
        if (StringUtils.isEmpty(carbonLoadModel.getQuoteChar)) {
          "" + DEFAULT_QUOTE_CHARACTER
        } else {
          carbonLoadModel.getQuoteChar
        }
      })
      .option("comment", carbonLoadModel.getCommentChar)
      .load(carbonLoadModel.getFactFilePath)
    df
  }

  // Hack for spark2 integration
  var updateTableMetadataFunc: (CarbonLoadModel, SQLContext, DictionaryLoadModel,
      Array[CarbonDimension]) => Unit = _

  /**
   * check whether global dictionary have been generated successfully or not
   *
   * @param status checking whether the generating is  successful
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
        LOGGER.error(s"table:$tableName column:$columnName generate global dictionary file failed")
      }
      if (x._3) {
        noDictionaryColumns += model.primDimensions(x._1)
      }
    }
    if (noDictionaryColumns.nonEmpty) {
      updateTableMetadataFunc(carbonLoadModel, sqlContext, model, noDictionaryColumns.toArray)
    }
    if (result) {
      LOGGER.error("generate global dictionary files failed")
      throw new Exception("Failed to generate global dictionary files")
    } else {
      LOGGER.info("generate global dictionary successfully")
    }
  }

  /**
   * get external columns and whose dictionary file path
   *
   * @param colDictFilePath external column dict file path
   * @param table           table identifier
   * @param dimensions      dimension columns
   */
  private def setPredefinedColumnDictPath(carbonLoadModel: CarbonLoadModel,
      colDictFilePath: String,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension]) = {
    val colFileMapArray = colDictFilePath.split(",")
    for (colPathMap <- colFileMapArray) {
      val colPathMapTrim = colPathMap.trim
      val colNameWithPath = colPathMapTrim.split(":")
      if (colNameWithPath.length == 1) {
        LOGGER.error("the format of external column dictionary should be " +
                     "columnName:columnPath, please check")
        throw new DataLoadingException("the format of predefined column dictionary" +
                                       " should be columnName:columnPath, please check")
      }
      setPredefineDict(carbonLoadModel, dimensions, table, colNameWithPath(0),
        FileUtils.getPaths(colPathMapTrim.substring(colNameWithPath(0).length + 1)))
    }
  }

  /**
   * set pre defined dictionary for dimension
   *
   * @param dimensions    all the dimensions
   * @param table         carbon table identifier
   * @param colName       user specified  column name for predefined dict
   * @param colDictPath   column dictionary file path
   * @param parentDimName parent dimenion for complex type
   */
  def setPredefineDict(carbonLoadModel: CarbonLoadModel,
      dimensions: Array[CarbonDimension],
      table: CarbonTableIdentifier,
      colName: String,
      colDictPath: String,
      parentDimName: String = "") {
    val middleDimName = colName.split("\\.")(0)
    val dimParent = parentDimName + {
      colName match {
        case "" => colName
        case _ =>
          if (parentDimName.isEmpty) middleDimName else "." + middleDimName
      }
    }
    // judge whether the column is exists
    val preDictDimensionOption = dimensions.filter(
      _.getColName.equalsIgnoreCase(dimParent))
    if (preDictDimensionOption.length == 0) {
      LOGGER.error(s"Column $dimParent is not a key column " +
                   s"in ${ table.getDatabaseName }.${ table.getTableName }")
      throw new DataLoadingException(s"Column $dimParent is not a key column. " +
                                     s"Only key column can be part of dictionary " +
                                     s"and used in COLUMNDICT option.")
    }
    val preDictDimension = preDictDimensionOption(0)
    if (preDictDimension.isComplex) {
      val children = preDictDimension.getListOfChildDimensions.asScala.toArray
      // for Array, user set ArrayFiled: path, while ArrayField has a child Array.val
      val currentColName = {
        preDictDimension.getDataType match {
          case DataType.ARRAY =>
            if (children(0).isComplex) {
              "val." + colName.substring(middleDimName.length + 1)
            } else {
              "val"
            }
          case _ => colName.substring(middleDimName.length + 1)
        }
      }
      setPredefineDict(carbonLoadModel, children, table, currentColName,
        colDictPath, dimParent)
    } else {
      carbonLoadModel.setPredefDictMap(preDictDimension, colDictPath)
    }
  }

  /**
   * use external dimension column to generate global dictionary
   *
   * @param colDictFilePath external column dict file path
   * @param table           table identifier
   * @param dimensions      dimension column
   * @param carbonLoadModel carbon load model
   * @param sqlContext      spark sql context
   * @param hdfsLocation    store location on hdfs
   * @param dictFolderPath  generated global dict file path
   */
  private def generatePredefinedColDictionary(colDictFilePath: String,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension],
      carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      hdfsLocation: String,
      dictFolderPath: String) = {
    // set pre defined dictionary column
    setPredefinedColumnDictPath(carbonLoadModel, colDictFilePath, table, dimensions)
    val dictLoadModel = createDictionaryLoadModel(carbonLoadModel, table, dimensions,
      hdfsLocation, dictFolderPath, forPreDefDict = true)
    // new RDD to achieve distributed column dict generation
    val extInputRDD = new CarbonColumnDictGenerateRDD(carbonLoadModel, dictLoadModel,
      sqlContext.sparkContext, table, dimensions, hdfsLocation, dictFolderPath)
      .partitionBy(new ColumnPartitioner(dictLoadModel.primDimensions.length))
    val statusList = new CarbonGlobalDictionaryGenerateRDD(extInputRDD, dictLoadModel).collect()
    // check result status
    checkStatus(carbonLoadModel, sqlContext, dictLoadModel, statusList)
  }

  /* generate Dimension Parsers
   *
   * @param model
   * @param distinctValuesList
   * @return dimensionParsers
   */
  def createDimensionParsers(model: DictionaryLoadModel,
      distinctValuesList: ArrayBuffer[(Int, HashSet[String])]): Array[GenericParser] = {
    // local combine set
    val dimNum = model.dimensions.length
    val primDimNum = model.primDimensions.length
    val columnValues = new Array[HashSet[String]](primDimNum)
    val mapColumnValuesWithId = new HashMap[String, HashSet[String]]
    for (i <- 0 until primDimNum) {
      columnValues(i) = new HashSet[String]
      distinctValuesList += ((i, columnValues(i)))
      mapColumnValuesWithId.put(model.primDimensions(i).getColumnId, columnValues(i))
    }
    val dimensionParsers = new Array[GenericParser](dimNum)
    for (j <- 0 until dimNum) {
      dimensionParsers(j) = GlobalDictionaryUtil.generateParserForDimension(
        Some(model.dimensions(j)),
        GlobalDictionaryUtil.createDataFormat(model.delimiters),
        mapColumnValuesWithId).get
    }
    dimensionParsers
  }

  /**
   * parse records in dictionary file and validate record
   *
   * @param x
   * @param accum
   * @param csvFileColumns
   */
  private def parseRecord(x: String, accum: Accumulator[Int],
      csvFileColumns: Array[String]): (String, String) = {
    val tokens = x.split("" + DEFAULT_SEPARATOR)
    var columnName: String = ""
    var value: String = ""
    // such as "," , "", throw ex
    if (tokens.isEmpty) {
      LOGGER.error("Read a bad dictionary record: " + x)
      accum += 1
    } else if (tokens.size == 1) {
      // such as "1", "jone", throw ex
      if (!x.contains(",")) {
        accum += 1
      } else {
        try {
          columnName = csvFileColumns(tokens(0).toInt)
        } catch {
          case ex: Exception =>
            LOGGER.error("Read a bad dictionary record: " + x)
            accum += 1
        }
      }
    } else {
      try {
        columnName = csvFileColumns(tokens(0).toInt)
        value = tokens(1)
      } catch {
        case ex: Exception =>
          LOGGER.error("Read a bad dictionary record: " + x)
          accum += 1
      }
    }
    (columnName, value)
  }

  /**
   * read local dictionary and prune column
   *
   * @param sqlContext
   * @param csvFileColumns
   * @param requireColumns
   * @param allDictionaryPath
   * @return allDictionaryRdd
   */
  private def readAllDictionaryFiles(sqlContext: SQLContext,
      csvFileColumns: Array[String],
      requireColumns: Array[String],
      allDictionaryPath: String,
      accumulator: Accumulator[Int]) = {
    var allDictionaryRdd: RDD[(String, Iterable[String])] = null
    try {
      // read local dictionary file, and spilt (columnIndex, columnValue)
      val basicRdd = sqlContext.sparkContext.textFile(allDictionaryPath)
        .map(x => parseRecord(x, accumulator, csvFileColumns)).persist()

      // group by column index, and filter required columns
      val requireColumnsList = requireColumns.toList
      allDictionaryRdd = basicRdd
        .groupByKey()
        .filter(x => requireColumnsList.contains(x._1))
    } catch {
      case ex: Exception =>
        LOGGER.error("Read dictionary files failed. Caused by: " + ex.getMessage)
        throw ex
    }
    allDictionaryRdd
  }

  /**
   * validate local dictionary files
   *
   * @param allDictionaryPath
   * @return (isNonempty, isDirectory)
   */
  private def validateAllDictionaryPath(allDictionaryPath: String): Boolean = {
    val fileType = FileFactory.getFileType(allDictionaryPath)
    val filePath = FileFactory.getCarbonFile(allDictionaryPath, fileType)
    // filepath regex, look like "/path/*.dictionary"
    if (filePath.getName.startsWith("*")) {
      val dictExt = filePath.getName.substring(1)
      if (filePath.getParentFile.exists()) {
        val listFiles = filePath.getParentFile.listFiles()
        if (listFiles.exists(file =>
          file.getName.endsWith(dictExt) && file.getSize > 0)) {
          true
        } else {
          LOGGER.warn("No dictionary files found or empty dictionary files! " +
                      "Won't generate new dictionary.")
          false
        }
      } else {
        throw new FileNotFoundException(
          "The given dictionary file path is not found!")
      }
    } else {
      if (filePath.exists()) {
        if (filePath.getSize > 0) {
          true
        } else {
          LOGGER.warn("No dictionary files found or empty dictionary files! " +
                      "Won't generate new dictionary.")
          false
        }
      } else {
        throw new FileNotFoundException(
          "The given dictionary file path is not found!")
      }
    }
  }

  /**
   * get file headers from fact file
   *
   * @param carbonLoadModel
   * @return headers
   */
  private def getHeaderFormFactFile(carbonLoadModel: CarbonLoadModel): Array[String] = {
    var headers: Array[String] = null
    val factFile: String = carbonLoadModel.getFactFilePath.split(",")(0)
    val readLine = CarbonUtil.readHeader(factFile)

    if (null != readLine) {
      val delimiter = if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter)) {
        "" + DEFAULT_SEPARATOR
      } else {
        carbonLoadModel.getCsvDelimiter
      }
      headers = readLine.toLowerCase().split(delimiter)
    } else {
      LOGGER.error("Not found file header! Please set fileheader")
      throw new IOException("Failed to get file header")
    }
    headers
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   *
   * @param sqlContext      sql context
   * @param carbonLoadModel carbon load model
   */
  def generateGlobalDictionary(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      dataFrame: Option[DataFrame] = None): Unit = {
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
      // create dictionary folder if not exists
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
      val dictfolderPath = carbonTablePath.getMetadataDirectoryPath
      // columns which need to generate global dictionary file
      val dimensions = carbonTable.getDimensionByTableName(
        carbonTable.getFactTableName).asScala.toArray
      // generate global dict from pre defined column dict file
      carbonLoadModel.initPredefDictMap()

      val allDictionaryPath = carbonLoadModel.getAllDictPath
      if (StringUtils.isEmpty(allDictionaryPath)) {
        LOGGER.info("Generate global dictionary from source data files!")
        // load data by using dataSource com.databricks.spark.csv
        var df = dataFrame.getOrElse(loadDataFrame(sqlContext, carbonLoadModel))
        var headers = if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) {
          df.columns
        } else {
          carbonLoadModel.getCsvHeader.split("" + DEFAULT_SEPARATOR)
        }
        headers = headers.map(headerName => headerName.trim)
        val colDictFilePath = carbonLoadModel.getColDictFilePath
        if (colDictFilePath != null) {
          // generate predefined dictionary
          generatePredefinedColDictionary(colDictFilePath, carbonTableIdentifier,
            dimensions, carbonLoadModel, sqlContext, storePath, dictfolderPath)
        }
        if (headers.length > df.columns.length) {
          val msg = "The number of columns in the file header do not match the " +
                    "number of columns in the data file; Either delimiter " +
                    "or fileheader provided is not correct"
          LOGGER.error(msg)
          throw new DataLoadingException(msg)
        }
        // use fact file to generate global dict
        val (requireDimension, requireColumnNames) = pruneDimensions(dimensions,
          headers, df.columns)
        if (requireDimension.nonEmpty) {
          // select column to push down pruning
          df = df.select(requireColumnNames.head, requireColumnNames.tail: _*)
          val model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier,
            requireDimension, storePath, dictfolderPath, false)
          // combine distinct value in a block and partition by column
          val inputRDD = new CarbonBlockDistinctValuesCombineRDD(df.rdd, model)
            .partitionBy(new ColumnPartitioner(model.primDimensions.length))
          // generate global dictionary files
          val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
          // check result status
          checkStatus(carbonLoadModel, sqlContext, model, statusList)
        } else {
          LOGGER.info("No column found for generating global dictionary in source data files")
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
              val modelforDim = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier,
                requireDimensionForDim, storePath, dictfolderPath, false)
              val inputRDDforDim = new CarbonBlockDistinctValuesCombineRDD(
                dimDataframe.rdd, modelforDim)
                .partitionBy(new ColumnPartitioner(modelforDim.primDimensions.length))
              val statusListforDim = new CarbonGlobalDictionaryGenerateRDD(
                inputRDDforDim, modelforDim).collect()
              checkStatus(carbonLoadModel, sqlContext, modelforDim, statusListforDim)
            } else {
              LOGGER.info(s"No columns in dimension table $dimTableName " +
                          "to generate global dictionary")
            }
          }
        }
      } else {
        LOGGER.info("Generate global dictionary from dictionary files!")
        val isNonempty = validateAllDictionaryPath(allDictionaryPath)
        if (isNonempty) {
          var headers = if (StringUtils.isEmpty(carbonLoadModel.getCsvHeader)) {
            getHeaderFormFactFile(carbonLoadModel)
          } else {
            carbonLoadModel.getCsvHeader.toLowerCase.split("" + DEFAULT_SEPARATOR)
          }
          headers = headers.map(headerName => headerName.trim)
          // prune columns according to the CSV file header, dimension columns
          val (requireDimension, requireColumnNames) = pruneDimensions(dimensions, headers, headers)
          if (requireDimension.nonEmpty) {
            val model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier,
              requireDimension, storePath, dictfolderPath, false)
            // check if dictionary files contains bad record
            val accumulator = sqlContext.sparkContext.accumulator(0)
            // read local dictionary file, and group by key
            val allDictionaryRdd = readAllDictionaryFiles(sqlContext, headers,
              requireColumnNames, allDictionaryPath, accumulator)
            // read exist dictionary and combine
            val inputRDD = new CarbonAllDictionaryCombineRDD(allDictionaryRdd, model)
              .partitionBy(new ColumnPartitioner(model.primDimensions.length))
            // generate global dictionary files
            val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
            // check result status
            checkStatus(carbonLoadModel, sqlContext, model, statusList)
            // if the dictionary contains wrong format record, throw ex
            if (accumulator.value > 0) {
              throw new DataLoadingException("Data Loading failure, dictionary values are " +
                                             "not in correct format!")
            }
          } else {
            LOGGER.info("have no column need to generate global dictionary")
          }
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(ex, "generate global dictionary failed")
        throw ex
    }
  }
}
