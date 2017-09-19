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
import java.util.UUID

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.reader.ThriftReader
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.processing.merger.TableMeta

case class MetaData(var tablesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String],
    msrs: Seq[String],
    carbonTable: CarbonTable,
    dictionaryMap: DictionaryMap)

object CarbonMetastore {

  def readSchemaFileToThriftTable(hadoopConf: Configuration, schemaFilePath: String): TableInfo = {
    val createTBase = new ThriftReader.TBaseCreator() {
      override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
        new TableInfo()
      }
    }
    val thriftReader = new ThriftReader(hadoopConf, schemaFilePath, createTBase)
    var tableInfo: TableInfo = null
    try {
      thriftReader.open()
      tableInfo = thriftReader.read().asInstanceOf[TableInfo]
    } finally {
      thriftReader.close()
    }
    tableInfo
  }

  def writeThriftTableToSchemaFile(hadoopConf: Configuration, schemaFilePath: String,
      tableInfo: TableInfo): Unit = {
    val thriftWriter = new ThriftWriter(hadoopConf, schemaFilePath, false)
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

class CarbonMetastore(hiveContext: HiveContext, val storePath: String,
    client: ClientInterface, queryId: String) extends HiveMetastoreCatalog(client, hiveContext) {

  @transient
  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

  val metadata = loadMetadata(storePath)

  private def getConf = hiveContext.sparkContext.hadoopConfiguration

  def getTableCreationTime(databaseName: String, tableName: String): Long = {
    val tableMeta = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(databaseName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
    val tableCreationTime = tableMeta.head.carbonTable.getTableLastUpdatedTime
    tableCreationTime
  }

  def lookupRelation1(dbName: Option[String],
      tableName: String)(sqlContext: SQLContext): LogicalPlan = {
    lookupRelation1(TableIdentifier(tableName, dbName))(sqlContext)
  }

  def lookupRelation1(tableIdentifier: TableIdentifier,
      alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tables = getTableFromMetadata(database, tableIdentifier.table)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableIdentifier.table,
          CarbonSparkUtil.createSparkMeta(tables.head.carbonTable), tables.head, alias)(sqlContext)
      case None =>
        LOGGER.audit(s"Table Not Found: ${tableIdentifier.table}")
        throw new NoSuchTableException
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

  def tableExists(identifier: TableIdentifier)(sqlContext: SQLContext): Boolean = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = identifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(identifier.table))
    tables.nonEmpty
  }

  def loadMetadata(metadataPath: String): MetaData = {
    val recorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val statistic = new QueryStatistic()
    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    val zookeeperurl = hiveContext.getConf(CarbonCommonConstants.ZOOKEEPER_URL, null)
    if (null != zookeeperurl) {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperurl)
    }
    if (metadataPath == null) {
      return null
    }
    // if no locktype is configured and store type is HDFS set HDFS lock as default
    if (null == CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.LOCK_TYPE) &&
        FileType.HDFS == FileFactory.getFileType(metadataPath)) {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.LOCK_TYPE,
          CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS
        )
      LOGGER.info("Default lock type HDFSLOCK is configured")
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
      if (FileFactory.isFileExist(getConf, databasePath, fileType)) {
        val file = FileFactory.getCarbonFile(getConf, databasePath, fileType)
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
                  carbonTableIdentifier, getConf)
                val tableMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(getConf, tableMetadataFile, fileType)) {
                  val tableName = tableFolder.getName
                  val tableUniqueName = databaseFolder.getName + "_" + tableFolder.getName


                  val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                      new TableInfo()
                    }
                  }
                  val thriftReader = new ThriftReader(getConf, tableMetadataFile, createTBase)
                  thriftReader.open()
                  val tableInfo: TableInfo = thriftReader.read().asInstanceOf[TableInfo]
                  thriftReader.close()

                  val schemaConverter = new ThriftWrapperSchemaConverterImpl
                  val wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, basePath)
                  val schemaFilePath = CarbonStorePath.getCarbonTablePath(storePath,
                    carbonTableIdentifier, getConf).getSchemaFilePath
                  wrapperTableInfo.setStorePath(storePath)
                  wrapperTableInfo
                    .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
                  metaDataBuffer += new TableMeta(carbonTable.getCarbonTableIdentifier, storePath,
                    null, carbonTable)
                }
              }
            })
          }
        })
      } else {
        // Create folders and files.
        FileFactory.mkdirs(getConf, databasePath, fileType)
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        FileFactory.mkdirs(getConf, databasePath, fileType)
    }
  }

  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableinfo
   *
   */
  def createTableFromThrift(
      tableInfo: org.apache.carbondata.core.metadata.schema.table.TableInfo,
      dbName: String, tableName: String, partitioner: Partitioner)
    (sqlContext: SQLContext): String = {
    if (tableExists(TableIdentifier(tableName, Some(dbName)))(sqlContext)) {
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
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val carbonTablePath =
      CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier, hadoopConf)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val tableMeta = new TableMeta(carbonTableIdentifier, storePath, null,
      CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName))

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(hadoopConf, schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(hadoopConf, schemaMetadataPath, fileType)
    }

    val thriftWriter = new ThriftWriter(hadoopConf, schemaFilePath, false)
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
      wrapperTableInfo: org.apache.carbondata.core.metadata.schema.table.TableInfo): Unit = {

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
   * Shows all tables for given schema.
   */
  def getTables(databaseName: Option[String])(sqlContext: SQLContext): Seq[(String, Boolean)] = {

    val dbName =
      databaseName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.filter { c =>
      c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(dbName)
    }.map { c => (c.carbonTableIdentifier.getTableName, false) }
  }

  def isTablePathExists(tableIdentifier: TableIdentifier)(sqlContext: SQLContext): Boolean = {
    val dbName = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sqlContext))
    val tableName = tableIdentifier.table
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val tablePath = CarbonStorePath.getCarbonTablePath(this.storePath,
      new CarbonTableIdentifier(dbName, tableName, ""), hadoopConf).getPath

    val fileType = FileFactory.getFileType(tablePath)
    FileFactory.isFileExist(hadoopConf, tablePath, fileType)
  }

  def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sqlContext: SQLContext) {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val metadataFilePath = CarbonStorePath.getCarbonTablePath(tableStorePath,
      new CarbonTableIdentifier(dbName, tableName, ""), hadoopConf).getMetadataDirectoryPath

    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(hadoopConf, metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTables
      val file = FileFactory.getCarbonFile(hadoopConf, metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      val metadataToBeRemoved: Option[TableMeta] = getTableFromMetadata(dbName,
        tableIdentifier.table)
      metadataToBeRemoved match {
        case Some(tableMeta) =>
          metadata.tablesMeta -= tableMeta
          CarbonMetadata.getInstance.removeTable(dbName + "_" + tableName)
          CarbonMetadata.getInstance.removeTable(dbName + "_" + tableName)
          updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
        case None =>
          logInfo(s"Metadata does not contain entry for table $tableName in database $dbName")
      }
      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sqlContext)
      // discard cached table info in cachedDataSourceTables
      sqlContext.catalog.refreshTable(tableIdentifier)
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
    if (FileFactory.isFileExist(getConf, timestampFile, timestampFileType)) {
      FileFactory.getCarbonFile(getConf, timestampFile, timestampFileType).getLastModifiedTime
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
    if (!FileFactory.isFileExist(getConf, timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $databaseName.$tableName")
      FileFactory.createNewFile(getConf, timestampFile, timestampFileType)
    }
    val systemTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(getConf, timestampFile, timestampFileType)
      .setLastModifiedTime(systemTime)
    systemTime
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(getConf, timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(getConf, timestampFile, timestampFileType).
        getLastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        refreshCache()
      }
    }
  }

  def refreshCache() {
    metadata.tablesMeta = loadMetadata(storePath).tablesMeta
  }

  def getSchemaLastUpdatedTime(databaseName: String, tableName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(getConf, timestampFile, timestampFileType)) {
      schemaLastUpdatedTime =
        FileFactory.getCarbonFile(getConf, timestampFile, timestampFileType).getLastModifiedTime
    }
    schemaLastUpdatedTime
  }

  def createDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    FileFactory.mkdirs(getConf, databasePath, fileType)
  }

  def dropDatabaseDirectory(dbName: String) {
    val databasePath = storePath + File.separator + dbName
    val fileType = FileFactory.getFileType(databasePath)
    if (FileFactory.isFileExist(getConf, databasePath, fileType)) {
      val dbPath = FileFactory.getCarbonFile(getConf, databasePath, fileType)
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
    "timestamp" ^^^ TimestampType |
    "date" ^^^ DateType |
    "char\\((\\d+)\\)".r ^^^ StringType

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
      case DateType => "date"
      case TimestampType => "timestamp"
    }
  }
}
