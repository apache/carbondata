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

import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.implicitConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonRelation}
import org.apache.spark.util.{AlterTableUtil, PartitionUtils}
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.{CarbonSparkUtil, CommonUtil}

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
 * Interface for command that modifies schema
 */
trait SchemaProcessCommand {
  def processSchema(sparkSession: SparkSession): Seq[Row]
}

/**
 * Interface for command that need to process data in file system
 */
trait DataProcessCommand {
  def processData(sparkSession: SparkSession): Seq[Row]
}

/**
 * Command for show table partitions Command
 *
 * @param tableIdentifier
 */
private[sql] case class ShowCarbonPartitionsCommand(
    tableIdentifier: TableIdentifier) extends RunnableCommand with SchemaProcessCommand {

  override val output = CommonUtil.partitionInfoOutput
  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(tableIdentifier)(sparkSession).
        asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    val tableName = carbonTable.getFactTableName
    val partitionInfo = carbonTable.getPartitionInfo(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    if (partitionInfo == null) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableName")
    }
    val partitionType = partitionInfo.getPartitionType
    val columnName = partitionInfo.getColumnSchemaList.get(0).getColumnName
    val LOGGER = LogServiceFactory.getLogService(ShowCarbonPartitionsCommand.getClass.getName)
    LOGGER.info("partition column name:" + columnName)
    CommonUtil.getPartitionInfo(columnName, partitionType, partitionInfo)
  }
}

/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
case class AlterTableCompaction(alterTableModel: AlterTableModel) extends RunnableCommand
    with DataProcessCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
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
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.carbonTable.getStorePath)

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

/**
 * Command for Alter Table Add & Split partition
 * Add is a special case of Splitting the default partition (part0)
 * @param splitPartitionModel
 */
case class AlterTableSplitPartitionCommand(splitPartitionModel: AlterTableSplitPartitionModel)
  extends RunnableCommand with DataProcessCommand with SchemaProcessCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  val tableName = splitPartitionModel.tableName
  val splitInfo = splitPartitionModel.splitInfo
  val partitionId = splitPartitionModel.partitionId.toInt
  var partitionInfo: PartitionInfo = null
  var carbonMetaStore: CarbonMetaStore = null
  var relation: CarbonRelation = null
  var dbName: String = null
  var storePath: String = null
  var table: CarbonTable = null
  var carbonTableIdentifier: CarbonTableIdentifier = null
  val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()
  val timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
  val dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance
    .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
  val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
    LockUsage.COMPACTION_LOCK,
    LockUsage.DELETE_SEGMENT_LOCK,
    LockUsage.DROP_TABLE_LOCK,
    LockUsage.CLEAN_FILES_LOCK,
    LockUsage.ALTER_PARTITION_LOCK)

  // TODO will add rollback function incase process data failure
  def run(sparkSession: SparkSession): Seq[Row] = {
      processSchema(sparkSession)
      processData(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    dbName = splitPartitionModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    carbonTableIdentifier = relation.tableMeta.carbonTableIdentifier
    storePath = relation.tableMeta.storePath
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTables(storePath)
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      LOGGER.error(s"Alter table failed. table not found: $dbName.$tableName")
      sys.error(s"Alter table failed. table not found: $dbName.$tableName")
    }
    table = relation.tableMeta.carbonTable
    partitionInfo = table.getPartitionInfo(tableName)
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)

    if (partitionInfo == null) {
      sys.error(s"Table $tableName is not a partition table.")
    }
    if (partitionInfo.getPartitionType == PartitionType.HASH) {
      sys.error(s"Hash partition table cannot be added or split!")
    }
    PartitionUtils.updatePartitionInfo(partitionInfo, partitionIds, partitionId,
      splitInfo, timestampFormatter, dateFormatter)

    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    // read TableInfo
    val tableInfo = carbonMetaStore.getThriftTableInfo(carbonTablePath)(sparkSession)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo,
      dbName, tableName, storePath)
    val tableSchema = wrapperTableInfo.getFactTable
    tableSchema.setPartitionInfo(partitionInfo)
    wrapperTableInfo.setFactTable(tableSchema)
    wrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    val thriftTable =
      schemaConverter.fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
    carbonMetaStore.updateMetadataByThriftTable(schemaFilePath, thriftTable,
      dbName, tableName, storePath)
    CarbonUtil.writeThriftTableToSchemaFile(schemaFilePath, thriftTable)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime(storePath)
    sparkSession.catalog.refreshTable(tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    var locks = List.empty[ICarbonLock]
    var success = false
    try {
      locks = AlterTableUtil.validateTableAndAcquireLock(dbName, tableName,
        locksToBeAcquired)(sparkSession)
      val carbonLoadModel = new CarbonLoadModel()
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(storePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      CarbonDataRDDFactory.alterTableSplitPartition(sparkSession.sqlContext,
        partitionId.toString,
        carbonLoadModel,
        oldPartitionIds.asScala.toList
      )
      success = true
    } catch {
      case e: Exception =>
        success = false
        sys.error(s"Add/Split Partition failed. Please check logs for more info. ${ e.getMessage }")
    } finally {
      AlterTableUtil.releaseLocks(locks)
      CacheProvider.getInstance().dropAllCache()
      LOGGER.info("Locks released after alter table add/split partition action.")
      LOGGER.audit("Locks released after alter table add/split partition action.")
      if (success) {
        LOGGER.info(s"Alter table add/split partition is successful for table $dbName.$tableName")
        LOGGER.audit(s"Alter table add/split partition is successful for table $dbName.$tableName")
      }
    }
    Seq.empty
  }
}

case class AlterTableDropPartition(alterTableDropPartitionModel: AlterTableDropPartitionModel)
  extends RunnableCommand with DataProcessCommand with SchemaProcessCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  val tableName = alterTableDropPartitionModel.tableName
  var dbName: String = null
  val partitionId = alterTableDropPartitionModel.partitionId
  val dropWithData = alterTableDropPartitionModel.dropWithData
  if (partitionId == 0 ) {
    sys.error(s"Cannot drop default partition! Please use delete statement!")
  }
  var partitionInfo: PartitionInfo = null
  var carbonMetaStore: CarbonMetaStore = null
  var relation: CarbonRelation = null
  var storePath: String = null
  var table: CarbonTable = null
  var carbonTableIdentifier: CarbonTableIdentifier = null
  val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()
  val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
    LockUsage.COMPACTION_LOCK,
    LockUsage.DELETE_SEGMENT_LOCK,
    LockUsage.DROP_TABLE_LOCK,
    LockUsage.CLEAN_FILES_LOCK,
    LockUsage.ALTER_PARTITION_LOCK)

  def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
    processData(sparkSession)
    Seq.empty
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    dbName = alterTableDropPartitionModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    carbonTableIdentifier = relation.tableMeta.carbonTableIdentifier
    storePath = relation.tableMeta.storePath
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTables(storePath)
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      LOGGER.error(s"Alter table failed. table not found: $dbName.$tableName")
      sys.error(s"Alter table failed. table not found: $dbName.$tableName")
    }
    table = relation.tableMeta.carbonTable
    partitionInfo = table.getPartitionInfo(tableName)
    if (partitionInfo == null) {
      sys.error(s"Table $tableName is not a partition table.")
    }
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)
    val partitionIndex = partitionIds.indexOf(Integer.valueOf(partitionId))
    partitionInfo.getPartitionType match {
      case PartitionType.HASH => sys.error(s"Hash partition cannot be dropped!")
      case PartitionType.RANGE =>
        val rangeInfo = new util.ArrayList(partitionInfo.getRangeInfo)
        val rangeToRemove = partitionInfo.getRangeInfo.get(partitionIndex - 1)
        rangeInfo.remove(rangeToRemove)
        partitionInfo.setRangeInfo(rangeInfo)
      case PartitionType.LIST =>
        val listInfo = new util.ArrayList(partitionInfo.getListInfo)
        val listToRemove = partitionInfo.getListInfo.get(partitionIndex - 1)
        listInfo.remove(listToRemove)
        partitionInfo.setListInfo(listInfo)
      case PartitionType.RANGE_INTERVAL =>
        sys.error(s"Dropping range interval partition isn't support yet!")
    }
    partitionInfo.dropPartition(partitionIndex)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    // read TableInfo
    val tableInfo = carbonMetaStore.getThriftTableInfo(carbonTablePath)(sparkSession)

    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo,
      dbName, tableName, storePath)
    val tableSchema = wrapperTableInfo.getFactTable
    tableSchema.setPartitionInfo(partitionInfo)
    wrapperTableInfo.setFactTable(tableSchema)
    wrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    val thriftTable =
      schemaConverter.fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
    thriftTable.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis)
    carbonMetaStore.updateMetadataByThriftTable(schemaFilePath, thriftTable,
      dbName, tableName, storePath)
    CarbonUtil.writeThriftTableToSchemaFile(schemaFilePath, thriftTable)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime(storePath)
    // sparkSession.catalog.refreshTable(tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    var locks = List.empty[ICarbonLock]
    var success = false
    try {
      locks = AlterTableUtil.validateTableAndAcquireLock(dbName, tableName,
        locksToBeAcquired)(sparkSession)
      val carbonLoadModel = new CarbonLoadModel()
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(storePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      CarbonDataRDDFactory.alterTableDropPartition(sparkSession.sqlContext,
        partitionId,
        carbonLoadModel,
        dropWithData,
        oldPartitionIds.asScala.toList
      )
      success = true
    } catch {
      case e: Exception =>
        sys.error(s"Drop Partition failed. Please check logs for more info. ${ e.getMessage } ")
      success = false
    } finally {
      CacheProvider.getInstance().dropAllCache()
      AlterTableUtil.releaseLocks(locks)
      LOGGER.info("Locks released after alter table drop partition action.")
      LOGGER.audit("Locks released after alter table drop partition action.")
    }
    LOGGER.info(s"Alter table drop partition is successful for table $dbName.$tableName")
    LOGGER.audit(s"Alter table drop partition is successful for table $dbName.$tableName")
    Seq.empty
  }
}

  case class CreateTable(cm: TableModel, createDSTable: Boolean = true) extends RunnableCommand
    with SchemaProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get
      .set(ref, value.asInstanceOf[AnyRef])
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val storePath = CarbonEnv.getInstance(sparkSession).storePath
    CarbonEnv.getInstance(sparkSession).carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables(storePath)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = getDB.getDatabaseName(cm.databaseNameOp, sparkSession)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm)

    // Add validation for sort scope when create table
    val sortScope = tableInfo.getFactTable.getTableProperties
      .getOrDefault("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    if (!CarbonUtil.isValidSortOption(sortScope)) {
      throw new InvalidConfigurationException(s"Passing invalid SORT_SCOPE '$sortScope'," +
        s" valid SORT_SCOPE are 'NO_SORT', 'BATCH_SORT', 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

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
      val tableIdentifier = AbsoluteTableIdentifier.from(storePath, dbName, tbName)
      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val tablePath = tableIdentifier.getTablePath
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tablePath)
      if (createDSTable) {
        try {
          val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
          cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
          cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)

          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tbName
               |(${ fields.map(f => f.rawSchema).mkString(",") })
               |USING org.apache.spark.sql.CarbonSource""".stripMargin +
                s""" OPTIONS (tableName "$tbName", dbName "$dbName", tablePath """.stripMargin +
                s""""$tablePath"$carbonSchemaString) """)
        } catch {
          case e: Exception =>
            val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
            // call the drop table to delete the created table.
            CarbonEnv.getInstance(sparkSession).carbonMetastore
                .dropTable(tablePath, identifier)(sparkSession)

            LOGGER.audit(s"Table creation with Database name [$dbName] " +
                s"and Table name [$tbName] failed")
            throw e
        }
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }
    Seq.empty
  }
}

case class DeleteLoadsById(
    loadids: Seq[String],
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore.
        lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation].
        tableMeta.carbonTable
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
    loadDate: String) extends RunnableCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore.
        lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation].
        tableMeta.carbonTable
    CarbonStore.deleteLoadByDate(
      loadDate,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      carbonTable
    )
    Seq.empty
  }
}

case class LoadTableByInsert(relation: CarbonDatasourceHadoopRelation,
    child: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val df = Dataset.ofRows(sparkSession, child)
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    val load = LoadTable(
      TableIdentifier(relation.tableName, Some(relation.databaseName)),
      relation.carbonRelation.tableName,
      Seq(),
      scala.collection.immutable.Map("fileheader" -> header),
      overwrite,
      null,
      Some(df)).run(sparkSession)
    // updating relation metadata. This is in case of auto detect high cardinality
    relation.carbonRelation.metaData =
        CarbonSparkUtil.createSparkMeta(relation.carbonRelation.tableMeta.carbonTable)
    load
  }
}

case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String, forceTableClean: Boolean = false)
  extends RunnableCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    if (forceTableClean) {
      CarbonStore.cleanFiles(
        getDB.getDatabaseName(databaseNameOp, sparkSession),
        tableName,
        CarbonEnv.getInstance(sparkSession).storePath,
        null,
        forceTableClean)
    } else {
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val relation = catalog
        .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
      val carbonTable = relation.tableMeta.carbonTable
      CarbonStore.cleanFiles(
        getDB.getDatabaseName(databaseNameOp, sparkSession),
        tableName,
        relation.asInstanceOf[CarbonRelation].tableMeta.storePath,
        carbonTable,
        forceTableClean)
    }
    Seq.empty
  }
}

case class ShowLoads(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore.
        lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation].
        tableMeta.carbonTable
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
  extends RunnableCommand with SchemaProcessCommand with DataProcessCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
    processData(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = getDB.getDatabaseName(databaseNameOp, sparkSession)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    val carbonEnv = CarbonEnv.getInstance(sparkSession)
    val catalog = carbonEnv.carbonMetastore
    val storePath = carbonEnv.storePath
    val tableIdentifier =
      AbsoluteTableIdentifier.from(CarbonEnv.getInstance(sparkSession).storePath,
        dbName.toLowerCase, tableName.toLowerCase)
    catalog.checkSchemasModifiedTimeAndReloadTables(tableIdentifier.getStorePath)
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
       locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(carbonTableIdentifier, lock)
      }
      LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")

      CarbonEnv.getInstance(sparkSession).carbonMetastore
          .dropTable(tableIdentifier.getTablePath, identifier)(sparkSession)
      LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")
    } catch {
      case ex: Exception =>
        LOGGER.error(ex, s"Dropping table $dbName.$tableName failed")
        sys.error(s"Dropping table $dbName.$tableName failed: ${ex.getMessage}")
    } finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          logInfo("Table MetaData Unlocked Successfully")
        }
      }
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    val dbName = getDB.getDatabaseName(databaseNameOp, sparkSession)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val tableIdentifier =
      AbsoluteTableIdentifier.from(CarbonEnv.getInstance(sparkSession).storePath, dbName, tableName)
    val metadataFilePath =
      CarbonStorePath.getCarbonTablePath(tableIdentifier).getMetadataDirectoryPath
    val fileType = FileFactory.getFileType(metadataFilePath)
    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
    }
    Seq.empty
  }
}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand with SchemaProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
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

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
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
    results ++= Seq(("SORT_SCOPE", carbonTable.getTableInfo.getFactTable
      .getTableProperties.getOrDefault("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("ADAPTIVE", "", ""))
    }
    results ++= Seq(("SORT_COLUMNS", relation.metaData.carbonTable.getSortColumns(
      relation.tableMeta.carbonTableIdentifier.getTableName).asScala
        .map(column => column).mkString(","), ""))
    val dimension = carbonTable
        .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    if (carbonTable.getPartitionInfo(carbonTable.getFactTableName) != null) {
      results ++=
          Seq(("Partition Columns: ", carbonTable.getPartitionInfo(carbonTable.getFactTableName)
              .getColumnSchemaList.asScala.map(_.getColumnName).mkString(","), ""))
    }
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }
}
