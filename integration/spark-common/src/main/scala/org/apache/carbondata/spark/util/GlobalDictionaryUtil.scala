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

import java.nio.charset.Charset
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.language.implicitConversions
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.{Accumulator, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.sql._
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.reader.CarbonDictionaryReader
import org.apache.carbondata.core.service.CarbonCommonFactory
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonDictionaryWriter
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.csvinput.{CSVInputFormat, StringArrayWritable}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.rdd._
import org.apache.carbondata.spark.tasks.{DictionaryWriterTask, SortIndexWriterTask}

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
        if (DataTypes.isArrayType(dim.getDataType)) {
          val arrDim = ArrayParser(dim, format)
          generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, arrDim)
          Some(arrDim)
        } else if (DataTypes.isStructType(dim.getDataType)) {
          val stuDim = StructParser(dim, format)
          generateParserForChildrenDimension(dim, format, mapColumnValuesWithId, stuDim)
          Some(stuDim)
        } else {
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
          CarbonUtil.delimiterConverter(d)
        })
      }
      DataFormat(delimiters.map(CarbonUtil.delimiterConverter(_)), 0, patterns)
    } else {
      null
    }
  }

  /**
   * create a instance of DictionaryLoadModel
   *
   * @param carbonLoadModel carbon load model
   * @param table           CarbonTableIdentifier
   * @param dimensions      column list
   * @param dictFolderPath  path of dictionary folder
   */
  def createDictionaryLoadModel(
      carbonLoadModel: CarbonLoadModel,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension],
      dictFolderPath: String,
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
    val primDimensions = primDimensionsBuffer.map { x => x }.toArray
    val dictDetail = CarbonSparkFactory.getDictionaryDetailService.
      getDictionaryDetail(dictFolderPath, primDimensions, carbonLoadModel.getTablePath)
    val dictFilePaths = dictDetail.dictFilePaths
    val dictFileExists = dictDetail.dictFileExists
    val columnIdentifier = dictDetail.columnIdentifiers
    val hdfsTempLocation = CarbonProperties.getInstance.
      getProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION, System.getProperty("java.io.tmpdir"))
    val lockType = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    val zookeeperUrl = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.ZOOKEEPER_URL)
    val serializationNullFormat =
      carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
    val absoluteTableIdentifier = AbsoluteTableIdentifier.from(carbonLoadModel.getTablePath, table)
    DictionaryLoadModel(
      absoluteTableIdentifier,
      dimensions,
      carbonLoadModel.getTablePath,
      dictFolderPath,
      dictFilePaths,
      dictFileExists,
      isComplexes.toArray,
      primDimensions,
      carbonLoadModel.getDelimiters,
      columnIdentifier,
      hdfsTempLocation,
      lockType,
      zookeeperUrl,
      serializationNullFormat,
      carbonLoadModel.getDefaultTimestampFormat,
      carbonLoadModel.getDefaultDateFormat)
  }

  /**
   * load and prune dictionary Rdd from csv file or input dataframe
   *
   * @param sqlContext sqlContext
   * @param carbonLoadModel carbonLoadModel
   * @param inputDF input dataframe
   * @param requiredCols names of dictionary column
   * @param hadoopConf hadoop configuration
   * @return rdd that contains only dictionary columns
   */
  private def loadInputDataAsDictRdd(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      inputDF: Option[DataFrame],
      requiredCols: Array[String],
      hadoopConf: Configuration): RDD[Row] = {
    if (inputDF.isDefined) {
      inputDF.get.select(requiredCols.head, requiredCols.tail : _*).rdd
    } else {
      CommonUtil.configureCSVInputFormat(hadoopConf, carbonLoadModel)
      hadoopConf.set(FileInputFormat.INPUT_DIR, carbonLoadModel.getFactFilePath)
      val headerCols = carbonLoadModel.getCsvHeaderColumns.map(_.toLowerCase)
      val header2Idx = headerCols.zipWithIndex.toMap
      // index of dictionary columns in header
      val dictColIdx = requiredCols.map(c => header2Idx(c.toLowerCase))

      val jobConf = new JobConf(hadoopConf)
      SparkHadoopUtil.get.addCredentials(jobConf)
      TokenCache.obtainTokensForNamenodes(jobConf.getCredentials,
        Array[Path](new Path(carbonLoadModel.getFactFilePath)),
        jobConf)
      val dictRdd = new NewHadoopRDD[NullWritable, StringArrayWritable](
        sqlContext.sparkContext,
        classOf[CSVInputFormat],
        classOf[NullWritable],
        classOf[StringArrayWritable],
        jobConf)
        .setName("global dictionary")
        .map[Row] { currentRow =>
        val rawRow = currentRow._2.get()
        val destRow = new Array[String](dictColIdx.length)
        for (i <- dictColIdx.indices) {
          // dictionary index in this row
          val idx = dictColIdx(i)
          // copy specific dictionary value from source to dest
          if (idx < rawRow.length) {
            System.arraycopy(rawRow, idx, destRow, i, 1)
          }
        }
        Row.fromSeq(destRow)
      }
      dictRdd
    }
  }

  /**
   * check whether global dictionary have been generated successfully or not
   *
   * @param status checking whether the generating is  successful
   */
  private def checkStatus(carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      model: DictionaryLoadModel,
      status: Array[(Int, SegmentStatus)]) = {
    var result = false
    val tableName = model.table.getCarbonTableIdentifier.getTableName
    status.foreach { x =>
      val columnName = model.primDimensions(x._1).getColName
      if (SegmentStatus.LOAD_FAILURE == x._2) {
        result = true
        LOGGER.error(s"table:$tableName column:$columnName generate global dictionary file failed")
      }
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
        FileUtils
          .getPaths(CarbonUtil
            .checkAndAppendHDFSUrl(colPathMapTrim.substring(colNameWithPath(0).length + 1))))
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
          if (parentDimName.isEmpty) {
            middleDimName
          } else {
            "." + middleDimName
          }
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
        if (DataTypes.isArrayType(preDictDimension.getDataType)) {
          if (children(0).isComplex) {
            "val." + colName.substring(middleDimName.length + 1)
          } else {
            "val"
          }
        } else {
          colName.substring(middleDimName.length + 1)
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
   * @param dictFolderPath  generated global dict file path
   */
  def generatePredefinedColDictionary(colDictFilePath: String,
      table: CarbonTableIdentifier,
      dimensions: Array[CarbonDimension],
      carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      dictFolderPath: String): Unit = {
    // set pre defined dictionary column
    setPredefinedColumnDictPath(carbonLoadModel, colDictFilePath, table, dimensions)
    val dictLoadModel = createDictionaryLoadModel(carbonLoadModel, table, dimensions,
      dictFolderPath, forPreDefDict = true)
    // new RDD to achieve distributed column dict generation
    val extInputRDD = new CarbonColumnDictGenerateRDD(carbonLoadModel, dictLoadModel,
      sqlContext.sparkContext, table, dimensions, dictFolderPath)
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
          case _: Exception =>
            LOGGER.error("Read a bad dictionary record: " + x)
            accum += 1
        }
      }
    } else {
      try {
        columnName = csvFileColumns(tokens(0).toInt)
        value = tokens(1)
      } catch {
        case _: Exception =>
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
        .map(x => parseRecord(x, accumulator, csvFileColumns))

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
    val filePath = new Path(allDictionaryPath)
    val file = FileFactory.getCarbonFile(filePath.toString, fileType)
    val parentFile = FileFactory.getCarbonFile(filePath.getParent.toString, fileType)
    // filepath regex, look like "/path/*.dictionary"
    if (filePath.getName.startsWith("*")) {
      val dictExt = filePath.getName.substring(1)
      if (parentFile.exists()) {
        val listFiles = parentFile.listFiles()
        if (listFiles.exists(file =>
          file.getName.endsWith(dictExt) && file.getSize > 0)) {
          true
        } else {
          LOGGER.warn("No dictionary files found or empty dictionary files! " +
                      "Won't generate new dictionary.")
          false
        }
      } else {
        throw new DataLoadingException(
          s"The given dictionary file path is not found : $allDictionaryPath")
      }
    } else {
      if (file.exists()) {
        if (file.getSize > 0) {
          true
        } else {
          LOGGER.warn("No dictionary files found or empty dictionary files! " +
                      "Won't generate new dictionary.")
          false
        }
      } else {
        throw new DataLoadingException(
          s"The given dictionary file path is not found : $allDictionaryPath")
      }
    }
  }

  /**
   * generate global dictionary with SQLContext and CarbonLoadModel
   *
   * @param sqlContext      sql context
   * @param carbonLoadModel carbon load model
   */
  def generateGlobalDictionary(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration,
      dataFrame: Option[DataFrame] = None): Unit = {
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
      val dictfolderPath = CarbonTablePath.getMetadataPath(carbonLoadModel.getTablePath)
      // columns which need to generate global dictionary file
      val dimensions = carbonTable.getDimensionByTableName(
        carbonTable.getTableName).asScala.toArray
      // generate global dict from pre defined column dict file
      carbonLoadModel.initPredefDictMap()
      val allDictionaryPath = carbonLoadModel.getAllDictPath
      if (StringUtils.isEmpty(allDictionaryPath)) {
        LOGGER.info("Generate global dictionary from source data files!")
        // load data by using dataSource com.databricks.spark.csv
        val headers = carbonLoadModel.getCsvHeaderColumns.map(_.trim)
        val colDictFilePath = carbonLoadModel.getColDictFilePath
        if (colDictFilePath != null) {
          // generate predefined dictionary
          generatePredefinedColDictionary(colDictFilePath, carbonTableIdentifier,
            dimensions, carbonLoadModel, sqlContext, dictfolderPath)
        }

        val headerOfInputData: Array[String] = if (dataFrame.isDefined) {
          dataFrame.get.columns
        } else {
          headers
        }

        if (headers.length > headerOfInputData.length && !carbonTable.isHivePartitionTable) {
          val msg = "The number of columns in the file header do not match the " +
                    "number of columns in the data file; Either delimiter " +
                    "or fileheader provided is not correct"
          LOGGER.error(msg)
          throw new DataLoadingException(msg)
        }
        // use fact file to generate global dict
        val (requireDimension, requireColumnNames) = pruneDimensions(dimensions,
          headers, headerOfInputData)
        if (requireDimension.nonEmpty) {
          // select column to push down pruning
          val dictRdd = loadInputDataAsDictRdd(sqlContext, carbonLoadModel, dataFrame,
            requireColumnNames, hadoopConf)
          val model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier,
            requireDimension, dictfolderPath, false)
          // combine distinct value in a block and partition by column
          val inputRDD = new CarbonBlockDistinctValuesCombineRDD(dictRdd, model)
            .partitionBy(new ColumnPartitioner(model.primDimensions.length))
          // generate global dictionary files
          val statusList = new CarbonGlobalDictionaryGenerateRDD(inputRDD, model).collect()
          // check result status
          checkStatus(carbonLoadModel, sqlContext, model, statusList)
        } else {
          LOGGER.info("No column found for generating global dictionary in source data files")
        }
      } else {
        generateDictionaryFromDictionaryFiles(
          sqlContext,
          carbonLoadModel,
          carbonTableIdentifier,
          dictfolderPath,
          dimensions,
          allDictionaryPath)
      }
    } catch {
      case ex: Exception =>
        if (ex.getCause != null && ex.getCause.isInstanceOf[NoRetryException]) {
          LOGGER.error(ex.getCause, "generate global dictionary failed")
          throw new Exception("generate global dictionary failed, " +
                              ex.getCause.getMessage)
        }
        ex match {
          case spx: SparkException =>
            LOGGER.error(spx, "generate global dictionary failed")
            throw new Exception("generate global dictionary failed, " +
                                trimErrorMessage(spx.getMessage))
          case _ =>
            LOGGER.error(ex, "generate global dictionary failed")
            throw ex
        }
    }
  }

  def generateDictionaryFromDictionaryFiles(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      carbonTableIdentifier: CarbonTableIdentifier,
      dictFolderPath: String,
      dimensions: Array[CarbonDimension],
      allDictionaryPath: String): Unit = {
    LOGGER.info("Generate global dictionary from dictionary files!")
    val allDictionaryPathAppended = CarbonUtil.checkAndAppendHDFSUrl(allDictionaryPath)
    val isNonempty = validateAllDictionaryPath(allDictionaryPathAppended)
    if (isNonempty) {
      var headers = carbonLoadModel.getCsvHeaderColumns
      headers = headers.map(headerName => headerName.trim)
      // prune columns according to the CSV file header, dimension columns
      val (requireDimension, requireColumnNames) = pruneDimensions(dimensions, headers, headers)
      if (requireDimension.nonEmpty) {
        val model = createDictionaryLoadModel(carbonLoadModel, carbonTableIdentifier,
          requireDimension, dictFolderPath, false)
        // check if dictionary files contains bad record
        val accumulator = sqlContext.sparkContext.accumulator(0)
        // read local dictionary file, and group by key
        val allDictionaryRdd = readAllDictionaryFiles(sqlContext, headers,
          requireColumnNames, allDictionaryPathAppended, accumulator)
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

  // Get proper error message of TextParsingException
  def trimErrorMessage(input: String): String = {
    var errorMessage: String = null
    if (input != null && input.contains("TextParsingException:")) {
      if (input.split("Hint").length > 1 &&
          input.split("Hint")(0).split("TextParsingException: ").length > 1) {
        errorMessage = input.split("Hint")(0).split("TextParsingException: ")(1)
      } else if (input.split("Parser Configuration:").length > 1) {
        errorMessage = input.split("Parser Configuration:")(0)
      }
    } else if (input != null && input.contains("Exception:")) {
      errorMessage = input.split("Exception: ")(1).split("\n")(0)
    }
    errorMessage
  }

  /**
   * This method will write dictionary file, sortindex file and dictionary meta for new dictionary
   * column with default value
   *
   * @param columnSchema
   * @param absoluteTableIdentifier
   * @param defaultValue
   */
  def loadDefaultDictionaryValueForNewColumn(
      columnSchema: ColumnSchema,
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      defaultValue: String): Unit = {

    val dictLock = CarbonLockFactory
      .getCarbonLockObj(absoluteTableIdentifier,
        columnSchema.getColumnUniqueId + LockUsage.LOCK)
    var isDictionaryLocked = false
    try {
      isDictionaryLocked = dictLock.lockWithRetries()
      if (isDictionaryLocked) {
        LOGGER.info(s"Successfully able to get the dictionary lock for ${
          columnSchema.getColumnName
        }")
      } else {
        sys.error(s"Dictionary file ${
          columnSchema.getColumnName
        } is locked for updation. Please try after some time")
      }
      val columnIdentifier = new ColumnIdentifier(columnSchema.getColumnUniqueId,
        null,
        columnSchema.getDataType)
      val dictionaryColumnUniqueIdentifier: DictionaryColumnUniqueIdentifier = new
          DictionaryColumnUniqueIdentifier(
            absoluteTableIdentifier,
            columnIdentifier,
            columnIdentifier.getDataType)
      val parsedValue = DataTypeUtil.normalizeColumnValueForItsDataType(defaultValue, columnSchema)
      val valuesBuffer = new mutable.HashSet[String]
      if (null != parsedValue) {
        valuesBuffer += parsedValue
      }
      val dictWriteTask = new DictionaryWriterTask(valuesBuffer,
        dictionary = null,
        dictionaryColumnUniqueIdentifier,
        columnSchema,
        false
      )
      val distinctValues = dictWriteTask.execute
      LOGGER.info(s"Dictionary file writing is successful for new column ${
        columnSchema.getColumnName
      }")

      if (distinctValues.size() > 0) {
        val sortIndexWriteTask = new SortIndexWriterTask(
          dictionaryColumnUniqueIdentifier,
          columnSchema.getDataType,
          dictionary = null,
          distinctValues)
        sortIndexWriteTask.execute()
      }

      LOGGER.info(s"SortIndex file writing is successful for new column ${
        columnSchema.getColumnName
      }")

      // After sortIndex writing, update dictionaryMeta
      dictWriteTask.updateMetaData()

      LOGGER.info(s"Dictionary meta file writing is successful for new column ${
        columnSchema.getColumnName
      }")
    } catch {
      case ex: Exception =>
        LOGGER.error(ex)
        throw ex
    } finally {
      if (dictLock != null && isDictionaryLocked) {
        if (dictLock.unlock()) {
          LOGGER.info(s"Dictionary ${
            columnSchema.getColumnName
          } Unlocked Successfully.")
        } else {
          LOGGER.error(s"Unable to unlock Dictionary ${
            columnSchema.getColumnName
          }")
        }
      }
    }
  }
}
