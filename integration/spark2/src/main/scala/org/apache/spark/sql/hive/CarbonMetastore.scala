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

package org.apache.spark.sql.hive

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.{GregorianCalendar, LinkedHashSet, UUID}

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.parsing.combinator.RegexParsers
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NoSuchTableException}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.sql.types._
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.CarbonTableIdentifier
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.apache.carbondata.core.reader.ThriftReader
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.integration.spark.merger.TableMeta
import org.apache.carbondata.lcm.locks.ZookeeperInit
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.spark.util.CarbonSparkUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference

case class MetaData(var tablesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String],
    msrs: Seq[String],
    carbonTable: CarbonTable,
    dictionaryMap: DictionaryMap)

object CarbonMetastore {

  def readSchemaFileToThriftTable(schemaFilePath: String): TableInfo = {
    val createTBase = new ThriftReader.TBaseCreator() {
      override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
        new TableInfo()
      }
    }
    val thriftReader = new ThriftReader(schemaFilePath, createTBase)
    var tableInfo: TableInfo = null
    try {
      thriftReader.open()
      tableInfo = thriftReader.read().asInstanceOf[TableInfo]
    } finally {
      thriftReader.close()
    }
    tableInfo
  }

  def writeThriftTableToSchemaFile(schemaFilePath: String, tableInfo: TableInfo): Unit = {
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    try {
      thriftWriter.open()
      thriftWriter.write(tableInfo);
    } finally {
      thriftWriter.close()
    }
  }

}

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

class CarbonMetastore(conf: RuntimeConfig, val storePath: String) extends Logging {

  @transient
  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

  private val nextId = new AtomicLong(0)

  def nextQueryId: String = {
    s"query_${nextId.getAndIncrement()}"
  }

  val metadata = loadMetadata(storePath, nextQueryId)

  def getTableCreationTime(databaseName: String, tableName: String): Long = {
    val tableMeta = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(databaseName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
    val tableCreationTime = tableMeta.head.carbonTable.getTableLastUpdatedTime
    tableCreationTime
  }

  def lookupRelation(dbName: Option[String],
                     tableName: String)(sparkSession: SparkSession): LogicalPlan = {
    lookupRelation(TableIdentifier(tableName, dbName))(sparkSession)
  }

  def lookupRelation(tableIdentifier: TableIdentifier,
                     alias: Option[String] = None)(sparkSession: SparkSession): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(
      sparkSession.catalog.currentDatabase
    )
    val tables = getTableFromMetadata(database, tableIdentifier.table)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableIdentifier.table,
          CarbonSparkUtil.createSparkMeta(tables.head.carbonTable), tables.head, alias)
      case None =>
        LOGGER.audit(s"Table Not Found: ${tableIdentifier.table}")
        throw new NoSuchTableException(database, tableIdentifier.table)
    }
  }

  /**
   * This method will search for a table in the catalog metadata
   *
   * @param database
   * @param tableName
   * @return
   */
  def getTableFromMetadata(database: String,
      tableName: String): Option[TableMeta] = {
    metadata.tablesMeta
      .find(c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
                 c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
  }

  def tableExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableIdentifier.table))
    tables.nonEmpty
  }

  def loadMetadata(metadataPath: String, queryId: String): MetaData = {
    val recorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val statistic = new QueryStatistic()
    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    val zookeeperUrl: String = conf.get(CarbonCommonConstants.ZOOKEEPER_URL, null)
    if (zookeeperUrl != null) {
      CarbonProperties.getInstance.addProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperUrl)
      ZookeeperInit.getInstance(zookeeperUrl)
      LOGGER.info("Zookeeper url is configured. Taking the zookeeper as lock type.")
      var configuredLockType = CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.LOCK_TYPE)
      if (null == configuredLockType) {
        configuredLockType = CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER
        CarbonProperties.getInstance
          .addProperty(CarbonCommonConstants.LOCK_TYPE,
            configuredLockType)
      }
    }

    if (metadataPath == null) {
      return null
    }
    val fileType = FileFactory.getFileType(metadataPath)
    val metaDataBuffer = new ArrayBuffer[TableMeta]
    fillMetaData(metadataPath, fileType, metaDataBuffer)
    updateSchemasUpdatedTime(readSchemaFileSystemTime("", ""))
    statistic.addStatistics(QueryStatisticsConstants.LOAD_META,
      System.currentTimeMillis())
    recorder.recordStatisticsForDriver(statistic, queryId)
    MetaData(metaDataBuffer)
  }

  private def fillMetaData(basePath: String, fileType: FileType,
      metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
    val databasePath = basePath // + "/schemas"
    try {
      if (FileFactory.isFileExist(databasePath, fileType)) {
        val file = FileFactory.getCarbonFile(databasePath, fileType)
        val databaseFolders = file.listFiles()

        databaseFolders.foreach(databaseFolder => {
          if (databaseFolder.isDirectory) {
            val dbName = databaseFolder.getName
            val tableFolders = databaseFolder.listFiles()

            tableFolders.foreach(tableFolder => {
              if (tableFolder.isDirectory) {
                val carbonTableIdentifier = new CarbonTableIdentifier(databaseFolder.getName,
                  tableFolder.getName, UUID.randomUUID().toString)
                val carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                  carbonTableIdentifier)
                val tableMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
                  val tableName = tableFolder.getName
                  val tableUniqueName = databaseFolder.getName + "_" + tableFolder.getName


                  val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                      new TableInfo()
                    }
                  }
                  val thriftReader = new ThriftReader(tableMetadataFile, createTBase)
                  thriftReader.open()
                  val tableInfo: TableInfo = thriftReader.read().asInstanceOf[TableInfo]
                  thriftReader.close()

                  val schemaConverter = new ThriftWrapperSchemaConverterImpl
                  val wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, basePath)
                  val schemaFilePath = CarbonStorePath
                    .getCarbonTablePath(storePath, carbonTableIdentifier).getSchemaFilePath
                  wrapperTableInfo.setStorePath(storePath)
                  wrapperTableInfo
                    .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  val carbonTable =
                    org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
                      .getCarbonTable(tableUniqueName)
                  metaDataBuffer += new TableMeta(carbonTable.getCarbonTableIdentifier, storePath,
                    carbonTable)
                }
              }
            })
          }
        })
      } else {
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
    }
  }

  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableinfo
   *
   */
  def createTableFromThrift(
      tableInfo: org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo,
      dbName: String, tableName: String)
    (sparkSession: SparkSession): String = {
    if (tableExists(TableIdentifier(tableName, Some(dbName)))(sparkSession)) {
      sys.error(s"Table [$tableName] already exists under Database [$dbName]")
    }
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)

    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
      tableInfo.getFactTable.getTableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val tableMeta = new TableMeta(carbonTableIdentifier, storePath,
      CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName))

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    metadata.tablesMeta += tableMeta
    logInfo(s"Table $tableName for Database $dbName created successfully.")
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
    carbonTablePath.getPath
  }

  private def updateMetadataByWrapperTable(
      wrapperTableInfo: org.apache.carbondata.core.carbon.metadata.schema.table.TableInfo): Unit = {

    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      wrapperTableInfo.getTableUniqueName)
    for (i <- metadata.tablesMeta.indices) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.tablesMeta(i).carbonTableIdentifier.getTableUniqueName)) {
        metadata.tablesMeta(i).carbonTable = carbonTable
      }
    }
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, storePath: String): Unit = {

    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, storePath)
    wrapperTableInfo
      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
    wrapperTableInfo.setStorePath(storePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }

  /**
   * Shows all schemas which has Database name like
   */
  def showDatabases(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
      schemaLike match {
        case Some(name) =>
          if (c.carbonTableIdentifier.getDatabaseName.contains(name)) {
            c.carbonTableIdentifier
              .getDatabaseName
          } else {
            null
          }
        case _ => c.carbonTableIdentifier.getDatabaseName
      }
    }.filter(f => f != null)
  }

  /**
   * Shows all tables in all schemas.
   */
  def getAllTables(): Seq[TableIdentifier] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
      TableIdentifier(c.carbonTableIdentifier.getTableName,
        Some(c.carbonTableIdentifier.getDatabaseName))
    }
  }

  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    val dbName = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val tableName = tableIdentifier.table

    val tablePath = CarbonStorePath.getCarbonTablePath(this.storePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getPath

    val fileType = FileFactory.getFileType(tablePath)
    FileFactory.isFileExist(tablePath, fileType)
  }

  def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession) {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table

    val metadataFilePath = CarbonStorePath.getCarbonTablePath(tableStorePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getMetadataDirectoryPath

    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTables
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      val metadataToBeRemoved: Option[TableMeta] = getTableFromMetadata(dbName,
        tableIdentifier.table)
      metadataToBeRemoved match {
        case Some(tableMeta) =>
          metadata.tablesMeta -= tableMeta
          org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
            .removeTable(dbName + "_" + tableName)
          org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
            .removeTable(dbName + "_" + tableName)
          updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
        case None =>
          logInfo(s"Metadata does not contain entry for table $tableName in database $dbName")
      }
      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
      // discard cached table info in cachedDataSourceTables
      sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    }
  }

  private def getTimestampFileAndType(databaseName: String, tableName: String) = {
    val timestampFile = storePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  /**
   * This method will put the updated timestamp of schema file in the table modified time store map
   *
   * @param timeStamp
   */
  def updateSchemasUpdatedTime(timeStamp: Long) {
    tableModifiedTimeStore.put("default", timeStamp)
  }

  /**
   * This method will read the timestamp of empty schema file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  def readSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime
    } else {
      System.currentTimeMillis()
    }
  }

  /**
   * This method will check and create an empty schema timestamp file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  def touchSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $databaseName.$tableName")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }
    val systemTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(systemTime)
    systemTime
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        refreshCache()
      }
    }
  }

  def refreshCache() {
    metadata.tablesMeta = loadMetadata(storePath, nextQueryId).tablesMeta
  }

  def getSchemaLastUpdatedTime(databaseName: String, tableName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      schemaLastUpdatedTime = FileFactory.getCarbonFile(timestampFile, timestampFileType)
        .getLastModifiedTime
    }
    schemaLastUpdatedTime
  }

  def readTableMetaDataFile(tableFolder: CarbonFile,
      fileType: FileFactory.FileType):
  (String, String, String, String, Partitioner, Long) = {
    val tableMetadataFile = tableFolder.getAbsolutePath + "/metadata"

    var schema: String = ""
    var databaseName: String = ""
    var tableName: String = ""
    var dataPath: String = ""
    var partitioner: Partitioner = null
    val cal = new GregorianCalendar(2011, 1, 1)
    var tableCreationTime = cal.getTime.getTime

    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      // load metadata
      val in = FileFactory.getDataInputStream(tableMetadataFile, fileType)
      var len = 0
      try {
        len = in.readInt()
      } catch {
        case others: EOFException => len = 0
      }

      while (len > 0) {
        val databaseNameBytes = new Array[Byte](len)
        in.readFully(databaseNameBytes)

        databaseName = new String(databaseNameBytes, "UTF8")
        val tableNameLen = in.readInt()
        val tableNameBytes = new Array[Byte](tableNameLen)
        in.readFully(tableNameBytes)
        tableName = new String(tableNameBytes, "UTF8")

        val dataPathLen = in.readInt()
        val dataPathBytes = new Array[Byte](dataPathLen)
        in.readFully(dataPathBytes)
        dataPath = new String(dataPathBytes, "UTF8")

        val versionLength = in.readInt()
        val versionBytes = new Array[Byte](versionLength)
        in.readFully(versionBytes)

        val schemaLen = in.readInt()
        val schemaBytes = new Array[Byte](schemaLen)
        in.readFully(schemaBytes)
        schema = new String(schemaBytes, "UTF8")

        val partitionLength = in.readInt()
        val partitionBytes = new Array[Byte](partitionLength)
        in.readFully(partitionBytes)
        val inStream = new ByteArrayInputStream(partitionBytes)
        val objStream = new ObjectInputStream(inStream)
        partitioner = objStream.readObject().asInstanceOf[Partitioner]
        objStream.close()

        try {
          tableCreationTime = in.readLong()
          len = in.readInt()
        } catch {
          case others: EOFException => len = 0
        }

      }
      in.close()
    }

    (databaseName, tableName, dataPath, schema, partitioner, tableCreationTime)
  }

  def createDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    FileFactory.mkdirs(databasePath, fileType)
  }

  def dropDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    if (FileFactory.isFileExist(databasePath, fileType)) {
      val dbPath = FileFactory.getCarbonFile(databasePath, fileType)
      CarbonUtil.deleteFoldersAndFiles(dbPath)
    }
  }

}


object CarbonMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ShortType |
    "short" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "long" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    fixedDecimalType |
    "decimal" ^^^ "decimal" ^^^ DecimalType(18, 2) |
    "varchar\\((\\d+)\\)".r ^^^ StringType |
    "timestamp" ^^^ TimestampType

  protected lazy val fixedDecimalType: Parser[DataType] =
    "decimal" ~> "(" ~> "^[1-9]\\d*".r ~ ("," ~> "^[0-9]\\d*".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField, ",") <~ ">" ^^ {
      case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = {
    parseAll(dataType, metastoreType) match {
      case Success(result, _) => result
      case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
    }
  }

  def toMetastoreType(dt: DataType): String = {
    dt match {
      case ArrayType(elementType, _) => s"array<${ toMetastoreType(elementType) }>"
      case StructType(fields) =>
        s"struct<${
          fields.map(f => s"${ f.name }:${ toMetastoreType(f.dataType) }")
            .mkString(",")
        }>"
      case StringType => "string"
      case FloatType => "float"
      case IntegerType => "int"
      case ShortType => "tinyint"
      case DoubleType => "double"
      case LongType => "bigint"
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DecimalType() => "decimal"
      case TimestampType => "timestamp"
    }
  }
}


/**
  * Represents logical plan for one carbon table
  */
case class CarbonRelation(
    databaseName: String,
    tableName: String,
    metaData: CarbonMetaData,
    tableMeta: TableMeta,
    alias: Option[String])
  extends LeafNode with MultiInstanceRelation {

  def recursiveMethod(dimName: String, childDim: CarbonDimension): String = {
    childDim.getDataType.toString.toLowerCase match {
      case "array" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:array<${ getArrayChildren(childDim.getColName) }>"
      case "struct" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:struct<${ getStructChildren(childDim.getColName) }>"
      case dType => s"${ childDim.getColName.substring(dimName.length + 1) }:${ dType }"
    }
  }

  def getArrayChildren(dimName: String): String = {
    metaData.carbonTable.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.toString.toLowerCase match {
        case "array" => s"array<${ getArrayChildren(childDim.getColName) }>"
        case "struct" => s"struct<${ getStructChildren(childDim.getColName) }>"
        case dType => addDecimalScaleAndPrecision(childDim, dType)
      }
    }).mkString(",")
  }

  def getStructChildren(dimName: String): String = {
    metaData.carbonTable.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.toString.toLowerCase match {
        case "array" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:array<${ getArrayChildren(childDim.getColName) }>"
        case "struct" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:struct<${ metaData.carbonTable.getChildren(childDim.getColName)
          .asScala.map(f => s"${ recursiveMethod(childDim.getColName, f) }").mkString(",")
        }>"
        case dType => s"${ childDim.getColName
          .substring(dimName.length() + 1) }:${ addDecimalScaleAndPrecision(childDim, dType) }"
      }
    }).mkString(",")
  }

  override def newInstance(): LogicalPlan = {
    CarbonRelation(databaseName, tableName, metaData, tableMeta, alias)
      .asInstanceOf[this.type]
  }

  val dimensionsAttr = {
    val sett = new LinkedHashSet(
      tableMeta.carbonTable.getDimensionByTableName(tableMeta.carbonTableIdentifier.getTableName)
        .asScala.asJava)
    sett.asScala.toSeq.filter(!_.getColumnSchema.isInvisible).map(dim => {
      val dimval = metaData.carbonTable
        .getDimensionByName(metaData.carbonTable.getFactTableName, dim.getColName)
      val output: DataType = dimval.getDataType
        .toString.toLowerCase match {
        case "array" =>
          CarbonMetastoreTypes.toDataType(s"array<${ getArrayChildren(dim.getColName) }>")
        case "struct" =>
          CarbonMetastoreTypes.toDataType(s"struct<${ getStructChildren(dim.getColName) }>")
        case dType =>
          val dataType = addDecimalScaleAndPrecision(dimval, dType)
          CarbonMetastoreTypes.toDataType(dataType)
      }

      AttributeReference(
        dim.getColName,
        output,
        nullable = true)()
    })
  }

  val measureAttr = {
    val factTable = tableMeta.carbonTable.getFactTableName
    new LinkedHashSet(
      tableMeta.carbonTable.
        getMeasureByTableName(tableMeta.carbonTable.getFactTableName).
        asScala.asJava).asScala.toSeq.filter(!_.getColumnSchema.isInvisible)
      .map(x => AttributeReference(x.getColName, CarbonMetastoreTypes.toDataType(
        metaData.carbonTable.getMeasureByName(factTable, x.getColName).getDataType.toString
          .toLowerCase match {
          case "int" => "long"
          case "short" => "long"
          case "decimal" => "decimal(" + x.getPrecision + "," + x.getScale + ")"
          case others => others
        }),
        nullable = true)())
  }

  override val output = dimensionsAttr ++ measureAttr

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = this.sizeInBytes)

  override def equals(other: Any): Boolean = {
    other match {
      case p: CarbonRelation =>
        p.databaseName == databaseName && p.output == output && p.tableName == tableName
      case _ => false
    }
  }

  def addDecimalScaleAndPrecision(dimval: CarbonDimension, dataType: String): String = {
    var dType = dataType
    if (dimval.getDataType
      == org.apache.carbondata.core.carbon.metadata.datatype.DataType.DECIMAL) {
      dType +=
        "(" + dimval.getColumnSchema.getPrecision + "," + dimval.getColumnSchema.getScale + ")"
    }
    dType
  }

  private var tableStatusLastUpdateTime = 0L

  private var sizeInBytesLocalValue = 0L

  def sizeInBytes: Long = {
    val tableStatusNewLastUpdatedTime = SegmentStatusManager.getTableStatusLastModifiedTime(
      tableMeta.carbonTable.getAbsoluteTableIdentifier)

    if (tableStatusLastUpdateTime != tableStatusNewLastUpdatedTime) {
      val tablePath = CarbonStorePath.getCarbonTablePath(
        tableMeta.storePath,
        tableMeta.carbonTableIdentifier).getPath
      val fileType = FileFactory.getFileType(tablePath)
      if(FileFactory.isFileExist(tablePath, fileType)) {
        tableStatusLastUpdateTime = tableStatusNewLastUpdatedTime
        sizeInBytesLocalValue = FileFactory.getDirectorySize(tablePath)
      }
    }
    sizeInBytesLocalValue
  }

}
