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

package org.apache.spark.util

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{CarbonEnv, Column, DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.command.UpdateTableModel
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.merger.TableMeta
import org.apache.carbondata.processing.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.load.ValidateUtil
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, DictionaryLoadModel}
import org.apache.carbondata.spark.util.{CommonUtil, GlobalDictionaryUtil}

object DataLoadUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Validate if the table exists or not.
   *
   * @param databaseName
   * @param tableName
   * @param sparkSession
   * @return
   */
  def validateCarbonRelation(databaseName: Option[String], tableName: String)
    (sparkSession: SparkSession): CarbonRelation = {
    val dbName = databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null || relation.tableMeta.carbonTable == null) {
      LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
      LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
      sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    }
    relation
  }

  /**
   * Prepare the data load options using carbon properties and the load options provided by user
   * after validation.
   *
   * @param carbonProperty
   * @param options
   * @return
   */
  def getFinalOptions(carbonProperty: CarbonProperties, options: Map[String, String]):
  Map[String, String] = {
    val optionsFinal = scala.collection.mutable.Map[String, String]()
    optionsFinal.put("delimiter", options.getOrElse("delimiter", ","))
    optionsFinal.put("quotechar", options.getOrElse("quotechar", "\""))
    optionsFinal.put("fileheader", options.getOrElse("fileheader", ""))
    optionsFinal.put("escapechar", options.getOrElse("escapechar", "\\"))
    optionsFinal.put("commentchar", options.getOrElse("commentchar", "#"))
    optionsFinal.put("columndict", options.getOrElse("columndict", null))
    optionsFinal
      .put("serialization_null_format", options.getOrElse("serialization_null_format", "\\N"))
    optionsFinal.put("bad_records_logger_enable", options.getOrElse("bad_records_logger_enable",
      carbonProperty
        .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT)))
    val badRecordActionValue = carbonProperty
      .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    optionsFinal.put("bad_records_action", options.getOrElse("bad_records_action", carbonProperty
      .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
        badRecordActionValue)))
    optionsFinal
      .put("is_empty_data_bad_record", options.getOrElse("is_empty_data_bad_record", carbonProperty
        .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
          CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT)))
    optionsFinal.put("all_dictionary_path", options.getOrElse("all_dictionary_path", ""))
    optionsFinal
      .put("complex_delimiter_level_1", options.getOrElse("complex_delimiter_level_1", "\\$"))
    optionsFinal
      .put("complex_delimiter_level_2", options.getOrElse("complex_delimiter_level_2", "\\:"))
    optionsFinal.put("dateformat", options.getOrElse("dateformat",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
        CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)))

    optionsFinal.put("global_sort_partitions", options.getOrElse("global_sort_partitions",
      carbonProperty
        .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS, null)))

    optionsFinal.put("maxcolumns", options.getOrElse("maxcolumns", null))
    optionsFinal.put("sort_scope", options
      .getOrElse("sort_scope",
        carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
          carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
            CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))

    optionsFinal.put("batch_sort_size_inmb", options.getOrElse("batch_sort_size_inmb",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
        carbonProperty.getProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
          CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))))
    optionsFinal.put("bad_record_path", options.getOrElse("bad_record_path",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
        carbonProperty.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))))

    optionsFinal.put(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))

    optionsFinal.put(CarbonCommonConstants.CARBON_DATE_FORMAT, carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))

    val useOnePass = options.getOrElse("single_pass",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
        CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT)).trim.toLowerCase match {
      case "true" =>
        true
      case "false" =>
        // when single_pass = false  and if either alldictionarypath
        // or columnDict is configured the do not allow load
        if (StringUtils.isNotEmpty(optionsFinal("all_dictionary_path")) ||
            StringUtils.isNotEmpty(optionsFinal("columndict"))) {
          throw new MalformedCarbonCommandException(
            "Can not use all_dictionary_path or columndict without single_pass.")
        } else {
          false
        }
      case illegal =>
        val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
        LOGGER.error(s"Can't use single_pass, because illegal syntax found: [" + illegal + "] " +
                     "Please set it as 'true' or 'false'")
        false
    }
    optionsFinal.put("single_pass", useOnePass.toString)
    optionsFinal.put(CarbonCommonConstants.DICTIONARY_SERVER_PORT, carbonProperty
      .getProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
        CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT))
    optionsFinal.toMap
  }

  private def checkDefaultValue(value: String, default: String): String = {
    if (StringUtils.isEmpty(value)) {
      default
    } else {
      value
    }
  }

  /**
   * Prepare carbon load model using the load options.
   *
   * @param carbonLoadModel
   * @param relation
   * @param optionsFinal
   */
  def prepareCarbonLoadModel(carbonLoadModel: CarbonLoadModel,
      relation: CarbonRelation,
      optionsFinal: Map[String, String]): Unit = {
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.carbonTable.getStorePath)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTable.getFactTableName)
    carbonLoadModel
      .setCarbonDataLoadSchema(new CarbonDataLoadSchema(relation.tableMeta.carbonTable))
    val validatedMaxColumns = CommonUtil.validateMaxColumns(carbonLoadModel.getCsvHeaderColumns,
      optionsFinal("maxcolumns"))
    carbonLoadModel.setMaxColumns(validatedMaxColumns.toString)
    val fileHeaders = DataLoadUtil.getFileHeaders(optionsFinal, relation.tableMeta.carbonTable)
    carbonLoadModel.setCsvHeader(fileHeaders)
    carbonLoadModel.setMetaDataDirectoryPath(CarbonStorePath
      .getCarbonTablePath(relation.tableMeta.storePath,
        relation.metaData.carbonTable.getCarbonTableIdentifier).getMetadataDirectoryPath)
    carbonLoadModel
      .setDictionaryServerPort(optionsFinal(CarbonCommonConstants.DICTIONARY_SERVER_PORT).toInt)
    setLoadOptionsToModel(carbonLoadModel, optionsFinal, relation.tableMeta.carbonTable)
  }

  private def setLoadOptionsToModel(carbonLoadModel: CarbonLoadModel,
      optionsFinal: Map[String, String],
      carbonTable: CarbonTable) = {
    val sort_scope = optionsFinal("sort_scope")
    val single_pass = optionsFinal("single_pass")
    val bad_records_logger_enable = optionsFinal("bad_records_logger_enable")
    val bad_records_action = optionsFinal("bad_records_action")
    val bad_record_path = optionsFinal("bad_record_path")
    val global_sort_partitions = optionsFinal("global_sort_partitions")
    val dateFormat = optionsFinal("dateformat")
    val delimeter = optionsFinal("delimiter")
    val complex_delimeter_level1 = optionsFinal("complex_delimiter_level_1")
    val complex_delimeter_level2 = optionsFinal("complex_delimiter_level_2")
    val all_dictionary_path = optionsFinal("all_dictionary_path")
    val column_dict = optionsFinal("columndict")
    if (bad_records_logger_enable.toBoolean ||
        LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
      if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
        sys.error("Invalid bad records location.")
      }
    }
    if (carbonLoadModel.getLoadMetadataDetails.isEmpty && carbonLoadModel.getUseOnePass &&
        StringUtils.isEmpty(carbonLoadModel.getColDictFilePath) &&
        StringUtils.isEmpty(carbonLoadModel.getAllDictPath)) {
      LOGGER.info(s"Cannot use single_pass=true for ${
        carbonLoadModel.getDatabaseName}.${ carbonLoadModel.getTableName } during the first load")
      LOGGER.audit(s"Cannot use single_pass=true for ${
        carbonLoadModel.getDatabaseName}.${ carbonLoadModel.getTableName } during the first load")
      carbonLoadModel.setUseOnePass(false)
    }
    ValidateUtil.validateDateFormat(dateFormat, carbonTable)
    ValidateUtil.validateSortScope(carbonTable, sort_scope)
    ValidateUtil.validateGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setBadRecordsLocation(bad_record_path)
    carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal("escapechar"), "\\"))
    carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal("quotechar"), "\""))
    carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal("commentchar"), "#"))
    carbonLoadModel.setDateFormat(dateFormat)
    carbonLoadModel.setSortScope(sort_scope)
    carbonLoadModel.setBatchSortSizeInMb(optionsFinal("batch_sort_size_inmb"))
    carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimeter))
    carbonLoadModel.setColDictFilePath(column_dict)
    carbonLoadModel.setDirectLoad(true)
    carbonLoadModel.setDefaultDateFormat(optionsFinal(CarbonCommonConstants.CARBON_DATE_FORMAT))
    carbonLoadModel
      .setDefaultDateFormat(optionsFinal(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT))
    carbonLoadModel
      .setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," +
        optionsFinal("serialization_null_format"))
    carbonLoadModel
      .setBadRecordsAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + bad_records_action)
    carbonLoadModel
      .setIsEmptyDataBadRecord(
        DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," +
        optionsFinal("is_empty_data_bad_record"))
    carbonLoadModel.setGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setUseOnePass(single_pass.toBoolean)
    if (delimeter.equalsIgnoreCase(complex_delimeter_level1) ||
        complex_delimeter_level1.equalsIgnoreCase(complex_delimeter_level2) ||
        delimeter.equalsIgnoreCase(complex_delimeter_level2)) {
      sys.error(s"Field Delimiter & Complex types delimiter are same")
    } else {
      carbonLoadModel.setComplexDelimiterLevel1(
        CarbonUtil.delimiterConverter(complex_delimeter_level1))
      carbonLoadModel.setComplexDelimiterLevel2(
        CarbonUtil.delimiterConverter(complex_delimeter_level2))
    }
    // set local dictionary path, and dictionary file extension
    carbonLoadModel.setAllDictPath(all_dictionary_path)
    carbonLoadModel
      .setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + bad_records_logger_enable)
  }

  def updateTableMetadata(carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      model: DictionaryLoadModel,
      noDictDimension: Array[CarbonDimension]): Unit = {
    val sparkSession = sqlContext.sparkSession
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(model.hdfsLocation,
      model.table)

    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    // read TableInfo
    val tableInfo: format.TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)

    // modify TableInfo
    val columns = tableInfo.getFact_table.getTable_columns
    for (i <- 0 until columns.size) {
      if (noDictDimension.exists(x => columns.get(i).getColumn_id.equals(x.getColumnId))) {
        columns.get(i).encoders.remove(org.apache.carbondata.format.Encoding.DICTIONARY)
      }
    }
    val entry = tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
    entry.setTime_stamp(System.currentTimeMillis())

    // write TableInfo
    metastore.updateTableSchema(carbonTablePath.getCarbonTableIdentifier,
      carbonTablePath.getCarbonTableIdentifier,
      tableInfo, entry, carbonTablePath.getPath)(sparkSession)

    // update the schema modified time
    metastore.updateAndTouchSchemasUpdatedTime(model.hdfsLocation)

    // update CarbonDataLoadSchema
    val carbonTable = metastore.lookupRelation(Option(model.table.getDatabaseName),
      model.table.getTableName)(sqlContext.sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
  }

  /**
   * Extract file headers from the load options.
   * 1. if user has provided file headers in load command then header=true should not be provided
   * 2. if  user has provided file headers in load command and header=false -> prepare file headers
   * 3. if header option is not provided then take file headers from load command.
   *
   * @param options
   * @param carbonTable
   * @return
   */
  def getFileHeaders(options: Map[String, String], carbonTable: CarbonTable): String = {
    // if there isn't file header in csv file and load sql doesn't provide FILEHEADER option,
    // we should use table schema to generate file header.
    var fileHeader = options("fileheader")
    val headerOption = options.get("header")
    if (headerOption.isDefined) {
      // whether the csv file has file header
      // the default value is true
      val header = try {
        headerOption.get.toBoolean
      } catch {
        case ex: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "'header' option should be either 'true' or 'false'. " + ex.getMessage)
      }
      if (header) {
        if (fileHeader.nonEmpty) {
          throw new MalformedCarbonCommandException(
            "When 'header' option is true, 'fileheader' option is not required.")
        }
      } else {
        // generate file header
        if (fileHeader.isEmpty) {
          fileHeader = carbonTable.getCreateOrderColumn(carbonTable.getFactTableName)
            .asScala.map(_.getColName).mkString(",")
        }
      }
    }
    fileHeader
  }

  def getPartitionLocation(tableMeta: TableMeta): String = {
    tableMeta.storePath + CarbonCommonConstants.FILE_SEPARATOR +  "partition" +
    tableMeta.carbonTableIdentifier.getDatabaseName + CarbonCommonConstants.FILE_SEPARATOR +
    tableMeta.carbonTableIdentifier.getTableName + CarbonCommonConstants.FILE_SEPARATOR
  }

  def createMetaDataDirectoryIfNotExist(metaDataPath: String): Unit = {
    val fileType = FileFactory.getFileType(metaDataPath)
    if (!FileFactory.isFileExist(metaDataPath, fileType)) {
      FileFactory.mkdirs(metaDataPath, fileType)
    }
  }

  def checkIfDictionaryColumnExists(allDimensions: List[CarbonDimension]): Boolean = {
    allDimensions.exists {
      carbonDimension =>
        carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
        !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
    }
  }

  def startDataLoadProcess(carbonLoadModel: CarbonLoadModel,
      carbonRelation: CarbonRelation,
      isOverwriteTable: Boolean,
      dataFrame: Option[DataFrame],
      updateModel: Option[UpdateTableModel])(sparkSession: SparkSession): Unit = {
    val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
    if (checkIfDictionaryColumnExists(carbonRelation.metaData.carbonTable.getAllDimensions
      .asScala.toList) && carbonLoadModel.getUseOnePass) {
      DataLoadUtil
        .startSinglePassLoad(carbonRelation,
          carbonLoadModel,
          isOverwriteTable,
          dataFrame,
          updateModel, columnar)(sparkSession)
    } else {
      val (dictionaryDataFrame, loadDataFrame) = getDictionaryAndLoadDataFrame(updateModel,
        dataFrame)
      GlobalDictionaryUtil.generateGlobalDictionary(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonRelation.tableMeta.storePath,
        dictionaryDataFrame)
      CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
        carbonLoadModel,
        carbonRelation.tableMeta.storePath,
        columnar,
        CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS,
        None,
        isOverwriteTable,
        loadDataFrame,
        updateModel)
    }
  }

  /**
   * Start data load process using single pass
   *
   * @param carbonRelation
   * @param carbonLoadModel
   * @param isOverwriteTable
   * @param dataFrame
   * @param updateModel
   * @param columnar
   * @param sparkSession
   */
  def startSinglePassLoad(carbonRelation: CarbonRelation,
      carbonLoadModel: CarbonLoadModel,
      isOverwriteTable: Boolean,
      dataFrame: Option[DataFrame],
      updateModel: Option[UpdateTableModel], columnar: Boolean)
    (sparkSession: SparkSession): Unit = {
    val carbonTable = carbonRelation.metaData.carbonTable
    val storePath = carbonRelation.tableMeta.storePath
    generateExternalDictionaryIfProvided(carbonLoadModel, carbonTable)(sparkSession)
    val dictionaryServer = prepareDictionaryServer(sparkSession, carbonLoadModel, carbonTable)
    CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
      carbonLoadModel,
      storePath,
      columnar,
      CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS,
      dictionaryServer,
      isOverwriteTable,
      dataFrame,
      updateModel)
  }

  /**
   * Start dictionary generation process if user has provided column_dictionary or all_dictionary.
   *
   * @param carbonLoadModel
   * @param carbonTable
   * @param sparkSession
   */
  private def generateExternalDictionaryIfProvided(carbonLoadModel: CarbonLoadModel,
      carbonTable: CarbonTable)(sparkSession: SparkSession) = {
    val colDictFilePath = carbonLoadModel.getColDictFilePath
    val storePath = carbonTable.getStorePath
    val carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
    val dictFolderPath = CarbonStorePath
      .getCarbonTablePath(storePath, carbonTableIdentifier).getMetadataDirectoryPath
    val dimensions = carbonTable.getDimensionByTableName(
      carbonTable.getFactTableName).asScala.toArray
    if (!StringUtils.isEmpty(colDictFilePath)) {
      carbonLoadModel.initPredefDictMap()
      // generate predefined dictionary
      GlobalDictionaryUtil
        .generatePredefinedColDictionary(colDictFilePath, carbonTable.getCarbonTableIdentifier,
          dimensions, carbonLoadModel, sparkSession.sqlContext, storePath, dictFolderPath)
    }
    if (!StringUtils.isEmpty(carbonLoadModel.getAllDictPath)) {
      carbonLoadModel.initPredefDictMap()
      GlobalDictionaryUtil
        .generateDictionaryFromDictionaryFiles(sparkSession.sqlContext,
          carbonLoadModel,
          storePath,
          carbonTableIdentifier,
          dictFolderPath,
          dimensions)
    }
  }

  private def getDictionaryAndLoadDataFrame(updateTableModel: Option[UpdateTableModel],
      dataFrame: Option[DataFrame]): (Option[DataFrame], Option[DataFrame]) = {
    if (updateTableModel.isDefined) {
      val fields = dataFrame.get.schema.fields
      import org.apache.spark.sql.functions.udf
      // extracting only segment from tupleId
      val getSegIdUDF = udf((tupleId: String) =>
        CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
      // getting all fields except tupleId field as it is not required in the value
      var otherFields = fields.toSeq
        .filter(field => !field.name
          .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
        .map(field => new Column(field.name))

      // extract tupleId field which will be used as a key
      val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
        .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).as("segId")
      // use dataFrameWithoutTupleId as dictionaryDataFrame
      val dataFrameWithoutTupleId = dataFrame.get.select(otherFields: _*)
      otherFields = otherFields :+ segIdColumn
      // use dataFrameWithTupleId as loadDataFrame
      val dataFrameWithTupleId = dataFrame.get.select(otherFields: _*)
      (Some(dataFrameWithoutTupleId), Some(dataFrameWithTupleId))
    } else {
      (dataFrame, dataFrame)
    }
  }

  /**
   * Prepare and start dictionary with the appropriate host and port.
   *
   * @param sparkSession
   * @param carbonLoadModel
   * @param carbonTable
   * @return
   */
  private def prepareDictionaryServer(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      carbonTable: CarbonTable): Option[DictionaryServer] = {
    // dictionaryServerClient dictionary generator
    val sparkDriverHost = sparkSession.sqlContext.sparkContext.
      getConf.get("spark.driver.host")
    carbonLoadModel.setDictionaryServerHost(sparkDriverHost)
    val dictionaryServer = DictionaryServer
      .getInstance(carbonLoadModel.getDictionaryServerPort, carbonTable)
    carbonLoadModel.setDictionaryServerPort(dictionaryServer.getPort)
    sparkSession.sparkContext.addSparkListener(new SparkListener() {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
        dictionaryServer.shutdown()
      }
    })
    Some(dictionaryServer)
  }

}
