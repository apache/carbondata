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
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.execution.{RunnableCommand, SparkPlan}
import org.apache.spark.sql.hive.CarbonMetastore
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.carbon.path.CarbonStorePath
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, DictionaryLoadModel}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, GlobalDictionaryUtil}

/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
private[sql] case class AlterTableCompaction(alterTableModel: AlterTableModel) extends
  RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    // TODO : Implement it.
    val tableName = alterTableModel.tableName
    val databaseName = getDB.getDatabaseName(alterTableModel.dbName, sqlContext)
    if (null == org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(databaseName + "_" + tableName)) {
      logError(s"alter table failed. table not found: $databaseName.$tableName")
      sys.error(s"alter table failed. table not found: $databaseName.$tableName")
    }

    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation1(Option(databaseName), tableName)(sqlContext)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
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

    val kettleHomePath = CarbonScalaUtil.getKettleHome(sqlContext)

    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
    try {
      CarbonDataRDDFactory
        .alterTableForCompaction(sqlContext,
          alterTableModel,
          carbonLoadModel,
          relation.tableMeta.storePath,
          kettleHomePath,
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

case class CreateTable(cm: TableModel) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = getDB.getDatabaseName(cm.databaseNameOp, sqlContext)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm, sqlContext)

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

    if (sqlContext.tableNames(dbName).exists(_.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        sys.error(s"Table [$tbName] already exists under database [$dbName]")
      }
    } else {
      // Add Database to catalog and persist
      val catalog = CarbonEnv.get.carbonMetastore
      // Need to fill partitioner class when we support partition
      val tablePath = catalog.createTableFromThrift(tableInfo, dbName, tbName, null)(sqlContext)
      try {
        sqlContext.sql(
          s"""CREATE TABLE $dbName.$tbName USING carbondata""" +
          s""" OPTIONS (tableName "$dbName.$tbName", tablePath "$tablePath") """)
          .collect
      } catch {
        case e: Exception =>
          val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
          // call the drop table to delete the created table.

          CarbonEnv.get.carbonMetastore
            .dropTable(catalog.storePath, identifier)(sqlContext)

          LOGGER.audit(s"Table creation with Database name [$dbName] " +
                       s"and Table name [$tbName] failed")
          throw e
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

private[sql] case class DeleteLoadsById(
    loadids: Seq[String],
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {

    val databaseName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"Delete segment by Id request has been received for $databaseName.$tableName")

    // validate load ids first
    validateLoadIds
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)

    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore.lookupRelation1(
      identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Delete segment by Id is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(dbName + '_' + tableName)

    if (null == carbonTable) {
      CarbonEnv.get.carbonMetastore
        .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadids.asJava, path).asScala

      if (invalidLoadIds.isEmpty) {

        LOGGER.audit(s"Delete segment by Id is successfull for $databaseName.$tableName.")
      }
      else {
        sys.error("Delete segment by Id is failed. Invalid ID is:" +
                  s" ${ invalidLoadIds.mkString(",") }")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }

    Seq.empty

  }

  // validates load ids
  private def validateLoadIds: Unit = {
    if (loadids.isEmpty) {
      val errorMessage = "Error: Segment id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)

    }
  }
}

private[sql] case class DeleteLoadsByLoadDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.tablemodel.tableSchema")

  def run(sqlContext: SQLContext): Seq[Row] = {

    LOGGER.audit("The delete segment by load date request has been received.")
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER
        .audit(s"Delete segment by load date is failed. Table $dbName.$tableName does not " +
               s"exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val timeObj = Cast(Literal(loadDate), TimestampType).eval()
    if (null == timeObj) {
      val errorMessage = "Error: Invalid load start time format " + loadDate
      throw new MalformedCarbonCommandException(errorMessage)
    }

    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(dbName + '_' + tableName)
    if (null == carbonTable) {
      var relation = CarbonEnv.get.carbonMetastore
        .lookupRelation1(identifier, None)(sqlContext).asInstanceOf[CarbonRelation]
    }
    val path = carbonTable.getMetaDataFilepath()

    try {
      val invalidLoadTimestamps = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadDate, path,
        timeObj.asInstanceOf[java.lang.Long]).asScala
      if (invalidLoadTimestamps.isEmpty) {
        LOGGER.audit(s"Delete segment by date is successfull for $dbName.$tableName.")
      }
      else {
        sys.error("Delete segment by date is failed. No matching segment found.")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
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

    // update Metadata
    val catalog = CarbonEnv.get.carbonMetastore
    catalog.updateMetadataByThriftTable(schemaFilePath, tableInfo,
      model.table.getDatabaseName, model.table.getTableName, carbonLoadModel.getStorePath)

    // update CarbonDataLoadSchema
    val carbonTable = catalog.lookupRelation1(Option(model.table.getDatabaseName),
      model.table.getTableName)(sqlContext).asInstanceOf[CarbonRelation].tableMeta.carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
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
    dataFrame: Option[DataFrame] = None) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  def run(sqlContext: SQLContext): Seq[Row] = {

    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    if (isOverwriteExist) {
      sys.error(s"Overwrite is not supported for carbon table with $dbName.$tableName")
    }
    if (null == org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(dbName + "_" + tableName)) {
      logError(s"Data loading failed. table not found: $dbName.$tableName")
      LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
      sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    }

    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(Option(dbName), tableName)(sqlContext)
      .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(relation.tableMeta.carbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK
      )
    try {
      if (carbonLock.lockWithRetries()) {
        logInfo("Successfully able to get the table metadata file lock")
      } else {
        sys.error("Table is locked for updation. Please try after some time")
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
      if (dimFilesPath.isEmpty) {
        carbonLoadModel.setDimFolderPath(null)
      } else {
        val x = dimFilesPath.map(f => f.table + ":" + CarbonUtil.checkAndAppendHDFSUrl(f.loadPath))
        carbonLoadModel.setDimFolderPath(x.mkString(","))
      }

      val table = relation.tableMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)

      val partitionLocation = relation.tableMeta.storePath + "/partition/" +
                              relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.tableMeta.carbonTableIdentifier.getTableName + "/"


      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      val kettleHomePath = CarbonScalaUtil.getKettleHome(sqlContext)

      // TODO It will be removed after kettle is removed.
      val useKettle = options.get("use_kettle") match {
        case Some(value) => value.toBoolean
        case _ =>
          val useKettleLocal = System.getProperty("use.kettle")
          if (useKettleLocal == null) {
            sqlContext.sparkContext.getConf.get("use_kettle_default", "true").toBoolean
          } else {
            useKettleLocal.toBoolean
          }
      }

      val delimiter = options.getOrElse("delimiter", ",")
      val quoteChar = options.getOrElse("quotechar", "\"")
      val fileHeader = options.getOrElse("fileheader", "")
      val escapeChar = options.getOrElse("escapechar", "\\")
      val commentchar = options.getOrElse("commentchar", "#")
      val columnDict = options.getOrElse("columndict", null)
      val serializationNullFormat = options.getOrElse("serialization_null_format", "\\N")
      val badRecordsLoggerEnable = options.getOrElse("bad_records_logger_enable", "false")
      val badRecordsLoggerRedirect = options.getOrElse("bad_records_action", "force")
      val allDictionaryPath = options.getOrElse("all_dictionary_path", "")
      val complex_delimiter_level_1 = options.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = options.getOrElse("complex_delimiter_level_2", "\\:")
      val dateFormat = options.getOrElse("dateformat", null)
      validateDateFormat(dateFormat, table)
      val multiLine = options.getOrElse("multiline", "false").trim.toLowerCase match {
        case "true" => true
        case "false" => false
        case illegal =>
          val errorMessage = "Illegal syntax found: [" + illegal + "] .The value multiline in " +
                             "load DDL which you set can only be 'true' or 'false', please check " +
                             "your input DDL."
          throw new MalformedCarbonCommandException(errorMessage)
      }
      val maxColumns = options.getOrElse("maxcolumns", null)
      carbonLoadModel.setMaxColumns(maxColumns)
      carbonLoadModel.setEscapeChar(escapeChar)
      carbonLoadModel.setQuoteChar(quoteChar)
      carbonLoadModel.setCommentChar(commentchar)
      carbonLoadModel.setDateFormat(dateFormat)
      carbonLoadModel
        .setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," + serializationNullFormat)
      carbonLoadModel
        .setBadRecordsLoggerEnable(
          TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + badRecordsLoggerEnable)
      carbonLoadModel
        .setBadRecordsAction(
          TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + badRecordsLoggerRedirect)

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
        GlobalDictionaryUtil.updateTableMetadataFunc = LoadTable.updateTableMetadata
        GlobalDictionaryUtil
          .generateGlobalDictionary(sqlContext, carbonLoadModel, relation.tableMeta.storePath,
            dataFrame)
        CarbonDataRDDFactory.loadCarbonData(sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            kettleHomePath,
            columinar,
            partitionStatus,
            useKettle,
            dataFrame)
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
          logInfo("Table MetaData Unlocked Successfully after data load")
        } else {
          logError("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }

  private def validateDateFormat(dateFormat: String, table: CarbonTable): Unit = {
    val dimensions = table.getDimensionByTableName(tableName).asScala
    if (dateFormat != null) {
      if (dateFormat.trim == "") {
        throw new MalformedCarbonCommandException("Error: Option DateFormat is set an empty " +
                                                  "string.")
      } else {
        var dateFormats: Array[String] = dateFormat.split(CarbonCommonConstants.COMMA)
        for (singleDateFormat <- dateFormats) {
          val dateFormatSplits: Array[String] = singleDateFormat.split(":", 2)
          val columnName = dateFormatSplits(0).trim.toLowerCase
          if (!dimensions.exists(_.getColName.equals(columnName))) {
            throw new MalformedCarbonCommandException("Error: Wrong Column Name " +
                                                      dateFormatSplits(0) +
                                                      " is provided in Option DateFormat.")
          }
          if (dateFormatSplits.length < 2 || dateFormatSplits(1).trim.isEmpty) {
            throw new MalformedCarbonCommandException("Error: Option DateFormat is not provided " +
                                                      "for " + "Column " + dateFormatSplits(0) +
                                                      ".")
          }
        }
      }
    }
  }
}

private[sql] case class DropTableCommand(ifExistsSet: Boolean, databaseNameOp: Option[String],
    tableName: String)
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(carbonTableIdentifier, LockUsage.DROP_TABLE_LOCK)
    val storePath = CarbonEnv.get.carbonMetastore.storePath
    var isLocked = false
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
      CarbonEnv.get.carbonMetastore.dropTable(storePath, identifier)(sqlContext)
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
          // delete bad record log after drop table
          val badLogPath = CarbonUtil.getBadLogPath(dbName + File.separator + tableName)
          val badLogFileType = FileFactory.getFileType(badLogPath)
          if (FileFactory.isFileExist(badLogPath, badLogFileType)) {
            val file = FileFactory.getCarbonFile(badLogPath, badLogFileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
        } else {
          logError("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class ShowLoads(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sqlContext: SQLContext): Seq[Row] = {
    val databaseName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    val tableUniqueName = databaseName + "_" + tableName
    // Here using checkSchemasModifiedTimeAndReloadTables in tableExists to reload metadata if
    // schema is changed by other process, so that tableInfoMap woulb be refilled.
    val tableExists = CarbonEnv.get.carbonMetastore
      .tableExists(TableIdentifier(tableName, databaseNameOp))(sqlContext)
    if (!tableExists) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
      .getCarbonTable(tableUniqueName)
    if (carbonTable == null) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val path = carbonTable.getMetaDataFilepath
    val loadMetadataDetailsArray = SegmentStatusManager.readLoadMetadata(path)
    if (loadMetadataDetailsArray.nonEmpty) {

      val parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP)

      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith(
        (l1, l2) => java.lang.Double.parseDouble(l1.getLoadName) > java.lang.Double
          .parseDouble(l2.getLoadName)
      )


      if (limit.isDefined) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(load => load.getVisibility.equalsIgnoreCase("true"))
        val limitLoads = limit.get
        try {
          val lim = Integer.parseInt(limitLoads)
          loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.slice(0, lim)
        } catch {
          case ex: NumberFormatException => sys.error(s" Entered limit is not a valid Number")
        }

      }

      loadMetadataDetailsSortedArray.filter(load => load.getVisibility.equalsIgnoreCase("true"))
        .map(load =>
          Row(
            load.getLoadName,
            load.getLoadStatus,
            new java.sql.Timestamp(parser.parse(load.getLoadStartTime).getTime),
            new java.sql.Timestamp(parser.parse(load.getTimestamp).getTime))).toSeq
    } else {
      Seq.empty

    }
  }

}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(tblIdentifier)(sqlContext).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val comment = if (relation.metaData.dims.contains(field.name)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
          relation.tableMeta.carbonTableIdentifier.getTableName,
          field.name)
        if (null != dimension.getColumnProperties && dimension.getColumnProperties.size() > 0) {
          val colprop = mapper.writeValueAsString(dimension.getColumnProperties)
          colProps.append(field.name).append(".")
            .append(mapper.writeValueAsString(dimension.getColumnProperties))
            .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN"
        } else {
          "KEY COLUMN"
        }
      } else {
        ("MEASURE")
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
      results ++= Seq(("NONE", "", ""))
    }
    val dimension = carbonTable
      .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s $dataType%-80s $comment%-72s")
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
    groups.map { x =>
      results = results :+ (s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }
}

private[sql] case class DeleteLoadByDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    dateValue: String
) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"The delete load by date request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(identifier)(sqlContext).asInstanceOf[CarbonRelation]
    var level: String = ""
    val carbonTable = org.apache.carbondata.core.carbon.metadata.CarbonMetadata
      .getInstance().getCarbonTable(dbName + '_' + tableName)
    if (relation == null) {
      LOGGER.audit(s"The delete load by date is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    val matches: Seq[AttributeReference] = relation.dimensionsAttr.filter(
      filter => filter.name.equalsIgnoreCase(dateField) &&
                filter.dataType.isInstanceOf[TimestampType]).toList
    if (matches.isEmpty) {
      LOGGER.audit("The delete load by date is failed. " +
                   s"Table $dbName.$tableName does not contain date field: $dateField")
      sys.error(s"Table $dbName.$tableName does not contain date field $dateField")
    } else {
      level = matches.asJava.get(0).name
    }
    val actualColName = relation.metaData.carbonTable.getDimensionByName(tableName, level)
      .getColName
    CarbonDataRDDFactory.deleteLoadByDate(
      sqlContext,
      new CarbonDataLoadSchema(carbonTable),
      dbName,
      tableName,
      CarbonEnv.get.carbonMetastore.storePath,
      level,
      actualColName,
      dateValue)
    LOGGER.audit(s"The delete load by date $dateValue is successful for $dbName.$tableName.")
    Seq.empty
  }

}

private[sql] case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sqlContext: SQLContext): Seq[Row] = {
    val dbName = getDB.getDatabaseName(databaseNameOp, sqlContext)
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
    val identifier = TableIdentifier(tableName, Option(dbName))
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(identifier)(sqlContext).
      asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"The clean files request is failed. Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }

    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    try {
      CarbonDataRDDFactory.cleanFiles(
        sqlContext.sparkContext,
        carbonLoadModel,
        relation.tableMeta.storePath)
      LOGGER.audit(s"Clean files request is successfull for $dbName.$tableName.")
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty
  }
}
