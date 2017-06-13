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

package org.apache.spark.sql.execution.command

import java.io.File

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{CarbonMetastore, CarbonRelation, HiveExternalCatalog}
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.load.ValidateUtil
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, DictionaryLoadModel}
import org.apache.carbondata.spark.util.{CarbonSparkUtil, CommonUtil, GlobalDictionaryUtil}

object Checker {
  def validateTableExists(
      dbName: Option[String],
      tableName: String,
      session: SparkSession): Unit = {
    val identifier = TableIdentifier(tableName, dbName)
    if (!CarbonEnv.getInstance(session).carbonMetastore.tableExists(identifier)(session)) {
      val err = s"table $dbName.$tableName not found"
      LogServiceFactory.getLogService(this.getClass.getName).error(err)
      throw new IllegalArgumentException(err)
    }
  }
}

/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
case class AlterTableCompaction(alterTableModel: AlterTableModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableModel.tableName.toLowerCase
    val databaseName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    val relation =
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(databaseName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    if (null == relation.tableMeta.carbonTable) {
      LOGGER.error(s"alter table failed. table not found: $databaseName.$tableName")
      sys.error(s"alter table failed. table not found: $databaseName.$tableName")
    }

    val carbonLoadModel = new CarbonLoadModel()

    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)

    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
    try {
      CarbonDataRDDFactory
        .alterTableForCompaction(sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          relation.tableMeta.storePath,
          storeLocation
        )
    } catch {
      case e: Exception =>
        if (null != e.getMessage) {
          sys.error(s"Compaction failed. Please check logs for more info. ${ e.getMessage }")
        } else {
          sys.error("Exception in compaction. Please check logs for more info.")
        }
    }
    Seq.empty
  }
}

case class CreateTable(cm: TableModel, createDSTable: Boolean = true) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    CarbonEnv.getInstance(sparkSession).carbonMetastore.checkSchemasModifiedTimeAndReloadTables()
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = getDB.getDatabaseName(cm.databaseNameOp, sparkSession)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm)

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        sys.error(s"Table [$tbName] already exists under database [$dbName]")
      }
    } else {
      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      // Need to fill partitioner class when we support partition
      val tablePath = catalog.createTableFromThrift(tableInfo, dbName, tbName)(sparkSession)
      if (createDSTable) {
        try {
          val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
          cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
          cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)
          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tbName
                |(${ fields.map(f => f.rawSchema).mkString(",") })
                |USING org.apache.spark.sql.CarbonSource""".stripMargin +
            s""" OPTIONS (tableName "$tbName", dbName "$dbName", tablePath "$tablePath") """)
        } catch {
          case e: Exception =>
            val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
            // call the drop table to delete the created table.

            CarbonEnv.getInstance(sparkSession).carbonMetastore
              .dropTable(catalog.storePath, identifier)(sparkSession)

            LOGGER.audit(s"Table creation with Database name [$dbName] " +
                         s"and Table name [$tbName] failed")
            throw e
        }
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }

    Seq.empty
  }

  def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get
      .set(ref, value.asInstanceOf[AnyRef])
  }
}

case class DeleteLoadsById(
    loadids: Seq[String],
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {

    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .getTableFromMetadata(getDB.getDatabaseName(databaseNameOp, sparkSession), tableName)
      .map(_.carbonTable).getOrElse(null)
    CarbonStore.deleteLoadById(
      loadids,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      carbonTable
    )
    Seq.empty

  }

}

case class DeleteLoadsByLoadDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.TableModel.tableSchema")

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .getTableFromMetadata(getDB.getDatabaseName(databaseNameOp, sparkSession), tableName)
      .map(_.carbonTable).getOrElse(null)
    CarbonStore.deleteLoadByDate(
      loadDate,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      carbonTable
    )
    Seq.empty
  }

}

object LoadTable {

  def updateTableMetadata(carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      model: DictionaryLoadModel,
      noDictDimension: Array[CarbonDimension]): Unit = {

    val carbonTablePath = CarbonStorePath.getCarbonTablePath(model.hdfsLocation,
      model.table)
    val schemaFilePath = carbonTablePath.getSchemaFilePath

    // read TableInfo
    val tableInfo = CarbonMetastore.readSchemaFileToThriftTable(schemaFilePath)

    // modify TableInfo
    val columns = tableInfo.getFact_table.getTable_columns
    for (i <- 0 until columns.size) {
      if (noDictDimension.exists(x => columns.get(i).getColumn_id.equals(x.getColumnId))) {
        columns.get(i).encoders.remove(org.apache.carbondata.format.Encoding.DICTIONARY)
      }
    }

    // write TableInfo
    CarbonMetastore.writeThriftTableToSchemaFile(schemaFilePath, tableInfo)

    val catalog = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetastore

    // update the schema modified time
    catalog.updateSchemasUpdatedTime(catalog.touchSchemaFileSystemTime(
      carbonLoadModel.getDatabaseName,
      carbonLoadModel.getTableName))

    // update Metadata
    catalog.updateMetadataByThriftTable(schemaFilePath, tableInfo,
      model.table.getDatabaseName, model.table.getTableName, carbonLoadModel.getStorePath)

    // update CarbonDataLoadSchema
    val carbonTable = catalog.lookupRelation(Option(model.table.getDatabaseName),
      model.table.getTableName)(sqlContext.sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
  }

}

case class LoadTableByInsert(relation: CarbonDatasourceHadoopRelation, child: LogicalPlan)
  extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val df = Dataset.ofRows(sparkSession, child)
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    val load = LoadTable(
      Some(relation.carbonRelation.databaseName),
      relation.carbonRelation.tableName,
      null,
      Seq(),
      scala.collection.immutable.Map("fileheader" -> header),
      isOverwriteExist = false,
      null,
      Some(df)).run(sparkSession)
    // updating relation metadata. This is in case of auto detect high cardinality
    relation.carbonRelation.metaData =
      CarbonSparkUtil.createSparkMeta(relation.carbonRelation.tableMeta.carbonTable)
    load
  }
}

case class LoadTable(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteExist: Boolean = false,
    var inputSqlString: String = null,
    dataFrame: Option[DataFrame] = None,
    updateModel: Option[UpdateTableModel] = None) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private def checkDefaultValue(value: String, default: String) = {
    if (StringUtils.isEmpty(value)) {
      default
    } else {
      value
    }
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    if (dataFrame.isDefined && !updateModel.isDefined) {
      val rdd = dataFrame.get.rdd
      if (rdd.partitions == null || rdd.partitions.length == 0) {
        LOGGER.warn("DataLoading finished. No data was loaded.")
        return Seq.empty
      }
    }

    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    if (isOverwriteExist) {
      sys.error(s"Overwrite is not supported for carbon table with $dbName.$tableName")
    }
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    if (null == relation.tableMeta.carbonTable) {
      LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
      LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
      sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    }

    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(relation.tableMeta.carbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK
      )
    try {
      // take lock only in case of normal data load.
      if (updateModel.isEmpty) {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Successfully able to get the table metadata file lock")
        } else {
          sys.error("Table is locked for updation. Please try after some time")
        }
      }

      val factPath = if (dataFrame.isDefined) {
        ""
      } else {
        FileUtils.getPaths(
          CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      }
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(relation.tableMeta.storePath)

      val table = relation.tableMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)

      val partitionLocation = relation.tableMeta.storePath + "/partition/" +
                              relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.tableMeta.carbonTableIdentifier.getTableName + "/"


      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean

      val delimiter = options.getOrElse("delimiter", ",")
      val quoteChar = options.getOrElse("quotechar", "\"")
      val fileHeader = options.getOrElse("fileheader", "")
      val escapeChar = options.getOrElse("escapechar", "\\")
      val commentChar = options.getOrElse("commentchar", "#")
      val columnDict = options.getOrElse("columndict", null)
      val serializationNullFormat = options.getOrElse("serialization_null_format", "\\N")
      val badRecordsLoggerEnable = options.getOrElse("bad_records_logger_enable", "false")
      val badRecordActionValue = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
      val badRecordsAction = options.getOrElse("bad_records_action", badRecordActionValue)
      val isEmptyDataBadRecord = options.getOrElse("is_empty_data_bad_record", "false")
      val allDictionaryPath = options.getOrElse("all_dictionary_path", "")
      val complex_delimiter_level_1 = options.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = options.getOrElse("complex_delimiter_level_2", "\\:")
      val dateFormat = options.getOrElse("dateformat", null)
      ValidateUtil.validateDateFormat(dateFormat, table, tableName)
      val maxColumns = options.getOrElse("maxcolumns", null)
      val sortScope = options.getOrElse("sort_scope", null)
      ValidateUtil.validateSortScope(table, sortScope)
      val batchSortSizeInMB = options.getOrElse("batch_sort_size_inmb", null)
      val globalSortPartitions = options.getOrElse("global_sort_partitions", null)
      ValidateUtil.validateGlobalSortPartitions(globalSortPartitions)
      carbonLoadModel.setEscapeChar(checkDefaultValue(escapeChar, "\\"))
      carbonLoadModel.setQuoteChar(checkDefaultValue(quoteChar, "\""))
      carbonLoadModel.setCommentChar(checkDefaultValue(commentChar, "#"))
      carbonLoadModel.setDateFormat(dateFormat)
      carbonLoadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
      carbonLoadModel.setDefaultDateFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
      carbonLoadModel
        .setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," + serializationNullFormat)
      carbonLoadModel
        .setBadRecordsLoggerEnable(
          TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + badRecordsLoggerEnable)
      carbonLoadModel
        .setBadRecordsAction(
          TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + badRecordsAction)
      carbonLoadModel
        .setIsEmptyDataBadRecord(
          DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," + isEmptyDataBadRecord)
      carbonLoadModel.setSortScope(sortScope)
      carbonLoadModel.setBatchSortSizeInMb(batchSortSizeInMB)
      carbonLoadModel.setGlobalSortPartitions(globalSortPartitions)
      val useOnePass = options.getOrElse("single_pass", "false").trim.toLowerCase match {
        case "true" =>
          true
        case "false" =>
          // when single_pass = false  and if either alldictionarypath
          // or columnDict is configured the do not allow load
          if (StringUtils.isNotEmpty(allDictionaryPath) || StringUtils.isNotEmpty(columnDict)) {
            throw new MalformedCarbonCommandException(
              "Can not use all_dictionary_path or columndict without single_pass.")
          } else {
            false
          }
        case illegal =>
          LOGGER.error(s"Can't use single_pass, because illegal syntax found: [" + illegal + "] " +
                       "Please set it as 'true' or 'false'")
          false
      }
      carbonLoadModel.setUseOnePass(useOnePass)
      if (delimiter.equalsIgnoreCase(complex_delimiter_level_1) ||
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2)) {
        sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else {
        carbonLoadModel.setComplexDelimiterLevel1(
          CarbonUtil.delimiterConverter(complex_delimiter_level_1))
        carbonLoadModel.setComplexDelimiterLevel2(
          CarbonUtil.delimiterConverter(complex_delimiter_level_2))
      }
      // set local dictionary path, and dictionary file extension
      carbonLoadModel.setAllDictPath(allDictionaryPath)

      val partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      try {
        // First system has to partition the data first and then call the load data
        LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
        carbonLoadModel.setFactFilePath(factPath)
        carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
        carbonLoadModel.setCsvHeader(fileHeader)
        carbonLoadModel.setColDictFilePath(columnDict)
        carbonLoadModel.setDirectLoad(true)
        carbonLoadModel.setCsvHeaderColumns(CommonUtil.getCsvHeaderColumns(carbonLoadModel))
        val validatedMaxColumns = CommonUtil.validateMaxColumns(carbonLoadModel.getCsvHeaderColumns,
          maxColumns)
        carbonLoadModel.setMaxColumns(validatedMaxColumns.toString)
        GlobalDictionaryUtil.updateTableMetadataFunc = LoadTable.updateTableMetadata
        if (carbonLoadModel.getUseOnePass) {
          val storePath = relation.tableMeta.storePath
          val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier
            .getCarbonTableIdentifier
          val carbonTablePath = CarbonStorePath
            .getCarbonTablePath(storePath, carbonTableIdentifier)
          val dictFolderPath = carbonTablePath.getMetadataDirectoryPath
          val dimensions = carbonTable.getDimensionByTableName(
            carbonTable.getFactTableName).asScala.toArray
          val colDictFilePath = carbonLoadModel.getColDictFilePath
          if (!StringUtils.isEmpty(colDictFilePath)) {
            carbonLoadModel.initPredefDictMap()
            // generate predefined dictionary
            GlobalDictionaryUtil
              .generatePredefinedColDictionary(colDictFilePath, carbonTableIdentifier,
                dimensions, carbonLoadModel, sparkSession.sqlContext, storePath, dictFolderPath)
          }
          if (!StringUtils.isEmpty(allDictionaryPath)) {
            carbonLoadModel.initPredefDictMap()
            GlobalDictionaryUtil
              .generateDictionaryFromDictionaryFiles(sparkSession.sqlContext,
                carbonLoadModel,
                storePath,
                carbonTableIdentifier,
                dictFolderPath,
                dimensions,
                allDictionaryPath)
          }
          // dictionaryServerClient dictionary generator
          val dictionaryServerPort = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
              CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT)
          val sparkDriverHost = sparkSession.sqlContext.sparkContext.
            getConf.get("spark.driver.host")
          carbonLoadModel.setDictionaryServerHost(sparkDriverHost)
          // start dictionary server when use one pass load and dimension with DICTIONARY
          // encoding is present.
          val allDimensions = table.getAllDimensions.asScala.toList
          val createDictionary = allDimensions.exists {
            carbonDimension => carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
                               !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
          }
          val server: Option[DictionaryServer] = if (createDictionary) {
            val dictionaryServer = DictionaryServer
              .getInstance(dictionaryServerPort.toInt)
            carbonLoadModel.setDictionaryServerPort(dictionaryServer.getPort)
            sparkSession.sparkContext.addSparkListener(new SparkListener() {
              override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
                dictionaryServer.shutdown()
              }
            })
            Some(dictionaryServer)
          } else {
            None
          }

          CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            columnar,
            partitionStatus,
            server,
            dataFrame,
            updateModel)
        }
        else {
          val (dictionaryDataFrame, loadDataFrame) = if (updateModel.isDefined) {
            val fields = dataFrame.get.schema.fields
            import org.apache.spark.sql.functions.udf
            // extracting only segment from tupleId
            val getSegIdUDF = udf((tupleId: String) =>
              CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
            // getting all fields except tupleId field as it is not required in the value
            var otherFields = fields.toSeq
              .filter(field => !field.name
                .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
              .map(field => {
                if (field.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION) && false) {
                  new Column(field.name
                    .substring(0,
                      field.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION)))
                } else {

                  new Column(field.name)
                }
              })

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
          GlobalDictionaryUtil
            .generateGlobalDictionary(
              sparkSession.sqlContext,
              carbonLoadModel,
              relation.tableMeta.storePath,
              dictionaryDataFrame)
          CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            columnar,
            partitionStatus,
            None,
            loadDataFrame,
            updateModel)
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. Please check the logs")
          throw ex
      } finally {
        // Once the data load is successful delete the unwanted partition files
        try {
          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
            val file = FileFactory
              .getCarbonFile(partitionLocation, fileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
        } catch {
          case ex: Exception =>
            LOGGER.error(ex)
            LOGGER.audit(s"Dataload failure for $dbName.$tableName. " +
                         "Problem deleting the partition folder")
            throw ex
        }

      }
    } catch {
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Table MetaData Unlocked Successfully after data load")
        } else {
          LOGGER.error("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class DeleteLoadByDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .getTableFromMetadata(getDB.getDatabaseName(databaseNameOp, sparkSession), tableName)
      .map(_.carbonTable).getOrElse(null)
    CarbonStore.deleteLoadByDate(
      loadDate,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      carbonTable
    )
    Seq.empty
  }
}

case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val relation = catalog
      .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
    val carbonTable = catalog
      .getTableFromMetadata(getDB.getDatabaseName(databaseNameOp, sparkSession), tableName)
      .map(_.carbonTable).getOrElse(null)
    CarbonStore.cleanFiles(
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      relation.asInstanceOf[CarbonRelation].tableMeta.storePath,
      carbonTable
    )
    Seq.empty
  }
}

case class ShowLoads(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .getTableFromMetadata(getDB.getDatabaseName(databaseNameOp, sparkSession), tableName)
      .map(_.carbonTable).getOrElse(null)
    CarbonStore.showSegments(
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      limit,
      carbonTable.getMetaDataFilepath
    )
  }
}

case class CarbonDropTableCommand(ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = getDB.getDatabaseName(databaseNameOp, sparkSession)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    val carbonLock = CarbonLockFactory.getCarbonLockObj(carbonTableIdentifier.getDatabaseName,
        carbonTableIdentifier.getTableName + CarbonCommonConstants.UNDERSCORE +
        LockUsage.DROP_TABLE_LOCK)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val storePath = catalog.storePath
    var isLocked = false
    catalog.checkSchemasModifiedTimeAndReloadTables()
    try {
      isLocked = carbonLock.lockWithRetries()
      if (isLocked) {
        logInfo("Successfully able to get the lock for drop.")
      }
      else {
        LOGGER.audit(s"Dropping table $dbName.$tableName failed as the Table is locked")
        sys.error("Table is locked for deletion. Please try after some time")
      }
      LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")
      val carbonTable = catalog.getTableFromMetadata(dbName, tableName).map(_.carbonTable).orNull
      if (null != carbonTable) {
        // clear driver B-tree and dictionary cache
        ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
      }
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .dropTable(storePath, identifier)(sparkSession)
      LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")
    } finally {
      if (carbonLock != null && isLocked) {
        if (carbonLock.unlock()) {
          logInfo("Table MetaData Unlocked Successfully after dropping the table")
          // deleting any remaining files.
          val metadataFilePath = CarbonStorePath
            .getCarbonTablePath(storePath, carbonTableIdentifier).getMetadataDirectoryPath
          val fileType = FileFactory.getFileType(metadataFilePath)
          if (FileFactory.isFileExist(metadataFilePath, fileType)) {
            val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
            CarbonUtil.deleteFoldersAndFiles(file.getParentFile)
          }
        }
        // delete bad record log after drop table
        val badLogPath = CarbonUtil.getBadLogPath(dbName + File.separator + tableName)
        val badLogFileType = FileFactory.getFileType(badLogPath)
        if (FileFactory.isFileExist(badLogPath, badLogFileType)) {
          val file = FileFactory.getCarbonFile(badLogPath, badLogFileType)
          CarbonUtil.deleteFoldersAndFiles(file)
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tblIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    val dims = relation.metaData.dims.map(x => x.toLowerCase)
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val fieldName = field.name.toLowerCase
      val comment = if (dims.contains(fieldName)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
          relation.tableMeta.carbonTableIdentifier.getTableName, fieldName)
        if (null != dimension.getColumnProperties && !dimension.getColumnProperties.isEmpty) {
          colProps.append(fieldName).append(".")
            .append(mapper.writeValueAsString(dimension.getColumnProperties))
            .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN" + (dimension.hasEncoding(Encoding.INVERTED_INDEX) match {
            case false => ",NOINVERTEDINDEX"
            case _ => ""
          })
        } else {
          "KEY COLUMN" + (dimension.hasEncoding(Encoding.INVERTED_INDEX) match {
            case false => ",NOINVERTEDINDEX"
            case _ => ""
          })
        }
      } else {
        "MEASURE"
      }
      (field.name, field.dataType.simpleString, comment)
    }
    val colPropStr = if (colProps.toString().trim().length() > 0) {
      // drops additional comma at end
      colProps.toString().dropRight(1)
    } else {
      colProps.toString()
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name: ", relation.tableMeta.carbonTableIdentifier
      .getDatabaseName, "")
    )
    results ++= Seq(("Table Name: ", relation.tableMeta.carbonTableIdentifier.getTableName, ""))
    results ++= Seq(("CARBON Store Path: ", relation.tableMeta.storePath, ""))
    val carbonTable = relation.tableMeta.carbonTable
    results ++= Seq(("Table Block Size : ", carbonTable.getBlockSizeInMB + " MB", ""))
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("ADAPTIVE", "", ""))
    }
    val dimension = carbonTable
      .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }

  private def getColumnGroups(dimensions: List[CarbonDimension]): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] =
      Seq(("", "", ""), ("##Column Group Information", "", ""))
    val groupedDimensions = dimensions.groupBy(x => x.columnGroupId()).filter {
      case (groupId, _) => groupId != -1
    }.toSeq.sortBy(_._1)
    val groups = groupedDimensions.map(colGroups => {
      colGroups._2.map(dim => dim.getColName).mkString(", ")
    })
    var index = 1
    groups.foreach { x =>
      results = results :+ (s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }
}
