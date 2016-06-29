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
import java.net.{InetAddress, InterfaceAddress, NetworkInterface}
import java.util.GregorianCalendar
import java.util.UUID

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AggregateTableAttributes, Partitioner}
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types._

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.carbondata.core.locks.ZookeeperInit
import org.carbondata.core.reader.ThriftReader
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.core.writer.ThriftWriter
import org.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.carbondata.spark.util.CarbonScalaUtil.CarbonSparkUtil

case class MetaData(var cubesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String],
  msrs: Seq[String],
  carbonTable: CarbonTable,
  dictionaryMap: DictionaryMap)

case class TableMeta(carbonTableIdentifier: CarbonTableIdentifier, storePath: String,
    var carbonTable: CarbonTable, partitioner: Partitioner)

object CarbonMetastoreCatalog {

  def readSchemaFileToThriftTable(schemaFilePath: String): TableInfo = {
    val createTBase = new ThriftReader.TBaseCreator() {
      override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
        return new TableInfo();
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
      thriftWriter.open();
      thriftWriter.write(tableInfo);
    } finally {
      thriftWriter.close();
    }
  }

}

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

class CarbonMetastoreCatalog(hive: HiveContext, val storePath: String, client: ClientInterface)
  extends HiveMetastoreCatalog(client, hive)
    with spark.Logging {

  @transient val LOGGER = LogServiceFactory
    .getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val cubeModifiedTimeStore = new java.util.HashMap[String, Long]()
  cubeModifiedTimeStore.put("default", System.currentTimeMillis())

  val metadata = loadMetadata(storePath)

  def lookupRelation1(
      databaseName: Option[String],
      tableName: String,
      alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    val db = databaseName match {
      case Some(name) => name
      case _ => null
    }
    if (db == null) {
      lookupRelation2(Seq(tableName), alias)(sqlContext)
    } else {
      lookupRelation2(Seq(db, tableName), alias)(sqlContext)
    }
  }

  override def lookupRelation(tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    try {
      super.lookupRelation(tableIdentifier, alias)
    } catch {
      case s: java.lang.Exception =>
        lookupRelation2(tableIdentifier, alias)(hive.asInstanceOf[SQLContext])
    }
  }

  def getCubeCreationTime(schemaName: String, cubeName: String): Long = {
    val cubeMeta = metadata.cubesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(schemaName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))
    val cubeCreationTime = cubeMeta.head.carbonTable.getTableLastUpdatedTime
    cubeCreationTime
  }


  def lookupRelation2(tableIdentifier: Seq[String],
      alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadCubes()
    tableIdentifier match {
      case Seq(schemaName, cubeName) =>
        val cubes = metadata.cubesMeta.filter(
          c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(schemaName) &&
               c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))
        if (cubes.nonEmpty) {
          CarbonRelation(schemaName, cubeName,
            CarbonSparkUtil.createSparkMeta(cubes.head.carbonTable), cubes.head, alias)(sqlContext)
        } else {
          LOGGER.audit(s"Table Not Found: $schemaName.$cubeName")
          throw new NoSuchTableException
        }
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(
          c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(currentDatabase) &&
               c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))
        if (cubes.nonEmpty) {
          CarbonRelation(currentDatabase, cubeName,
            CarbonSparkUtil.createSparkMeta(cubes.head.carbonTable), cubes.head, alias)(sqlContext)
        } else {
          LOGGER.audit(s"Table Not Found: $currentDatabase.$cubeName")
          throw new NoSuchTableException
        }
      case _ =>
        LOGGER.audit(s"Table Not Found: $tableIdentifier")
        throw new NoSuchTableException
    }
  }

  def cubeExists(db: Option[String], tableName: String)(sqlContext: SQLContext): Boolean = {
    if (db.isEmpty || db.get == null || db.get == "") {
      cubeExists(Seq(tableName))(sqlContext)
    } else {
      cubeExists(Seq(db.get, tableName))(sqlContext)
    }
  }

  def cubeExists(tableIdentifier: Seq[String])(sqlContext: SQLContext): Boolean = {
    checkSchemasModifiedTimeAndReloadCubes()
    tableIdentifier match {
      case Seq(schemaName, cubeName) =>
        val cubes = metadata.cubesMeta.filter(
          c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(schemaName) &&
               c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))
        cubes.nonEmpty
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(
          c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(currentDatabase) &&
               c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))
        cubes.nonEmpty
      case _ => false
    }
  }

  def loadMetadata(metadataPath: String): MetaData = {

    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    if (CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT)
      .equalsIgnoreCase(CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER)) {
      val zookeeperUrl = hive.getConf("spark.deploy.zookeeper.url", "127.0.0.1:2181")
      ZookeeperInit.getInstance(zookeeperUrl)
    }

    if (metadataPath == null) {
      return null
    }
    val fileType = FileFactory.getFileType(metadataPath)
    val metaDataBuffer = new ArrayBuffer[TableMeta]
    fillMetaData(metadataPath, fileType, metaDataBuffer)
    updateSchemasUpdatedTime("", "")
    MetaData(metaDataBuffer)

  }

  private def fillMetaData(basePath: String, fileType: FileType,
      metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
    val schemasPath = basePath // + "/schemas"
    try {
      if (FileFactory.isFileExist(schemasPath, fileType)) {
        val file = FileFactory.getCarbonFile(schemasPath, fileType)
        val schemaFolders = file.listFiles()

        schemaFolders.foreach(schemaFolder => {
          if (schemaFolder.isDirectory) {
            val dbName = schemaFolder.getName
            val cubeFolders = schemaFolder.listFiles()

            cubeFolders.foreach(cubeFolder => {
              if (cubeFolder.isDirectory) {
                val carbonTableIdentifier = new CarbonTableIdentifier(schemaFolder.getName,
                    cubeFolder.getName, UUID.randomUUID().toString)
                val carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                  carbonTableIdentifier)
                val cubeMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
                  val tableName = cubeFolder.getName
                  val cubeUniqueName = schemaFolder.getName + "_" + cubeFolder.getName


                  val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                      new TableInfo()
                    }
                  }
                  val thriftReader = new ThriftReader(cubeMetadataFile, createTBase)
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
                  val carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
                      .getCarbonTable(cubeUniqueName);
                  metaDataBuffer += TableMeta(
                    carbonTable.getCarbonTableIdentifier,
                    storePath,
                    carbonTable,
                    // TODO: Need to update Database thirft to hold partitioner
                    // information and reload when required.
                    Partitioner("org.carbondata.spark.partition.api.impl." +
                                "SampleDataPartitionerImpl",
                      Array(""), 1, getNodeList))
                }
              }
            })
          }
        })
      }
      else {
        // Create folders and files.
        FileFactory.mkdirs(schemasPath, fileType)

      }
    }
    catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        FileFactory.mkdirs(schemasPath, fileType)

    }
  }


  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableinfo
   *
   */
  def createCubeFromThrift(tableInfo: org.carbondata.core.carbon.metadata.schema.table.TableInfo,
      dbName: String, tableName: String, partitioner: Partitioner)
    (sqlContext: SQLContext): String = {

    if (cubeExists(Seq(dbName, tableName))(sqlContext)) {
      sys.error(s"Table [$tableName] already exists under Database [$dbName]")
    }

    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)

    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
        tableInfo.getFactTable().getTableId())
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)

    val cubeMeta = TableMeta(
      carbonTableIdentifier,
      storePath,
      CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName),
      Partitioner("org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
        Array(""), 1, getNodeList))

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }

    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()

    metadata.cubesMeta += cubeMeta
    logInfo(s"Table $tableName for Database $dbName created successfully.")
    LOGGER.info("Table " + tableName + " for Database " + dbName + " created successfully.")
    updateSchemasUpdatedTime(dbName, tableName)
    carbonTablePath.getPath
  }

  private def updateMetadataByWrapperTable(
      wrapperTableInfo: org.carbondata.core.carbon.metadata.schema.table.TableInfo): Unit = {

    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      wrapperTableInfo.getTableUniqueName)
    for (i <- 0 until metadata.cubesMeta.size) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.cubesMeta(i).carbonTableIdentifier.getTableUniqueName)) {
        metadata.cubesMeta(i).carbonTable = carbonTable
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

  /*
   * This method will return the list of executers in the cluster.
   * For this we take the  memory status of all node with getExecutorMemoryStatus
   * and extract the keys. getExecutorMemoryStatus also returns the driver memory also
   * In client mode driver will run in the localhost
   * There can be executor spawn in same drive node. So we can remove first occurance of
   * localhost for retriving executor list
   */
  def getNodeList: Array[String] = {

    val arr =
      hive.sparkContext.getExecutorMemoryStatus.map {
        kv =>
          kv._1.split(":")(0)
      }.toSeq
    val localhostIPs = getLocalhostIPs
    val selectedLocalIPList = localhostIPs.filter(arr.contains(_))

    val nodelist: List[String] = withoutDriverIP(arr.toList)(selectedLocalIPList.contains(_))
    val masterMode = hive.sparkContext.getConf.get("spark.master")
    if (nodelist.nonEmpty) {
      // Specific for Yarn Mode
      if ("yarn-cluster".equals(masterMode) || "yarn-client".equals(masterMode)) {
        val nodeNames = nodelist.map { x =>
          val addr = InetAddress.getByName(x)
          addr.getHostName
        }
        nodeNames.toArray
      }
      else {
        // For Standalone cluster, node IPs will be returned.
        nodelist.toArray
      }
    }
    else {
      Seq(InetAddress.getLocalHost.getHostName).toArray
    }
  }

  private def getLocalhostIPs = {
    val iface = NetworkInterface.getNetworkInterfaces
    var addresses: List[InterfaceAddress] = List.empty
    while (iface.hasMoreElements) {
      addresses = iface.nextElement().getInterfaceAddresses.asScala.toList ++ addresses
    }
    val inets = addresses.map(_.getAddress.getHostAddress)
    inets
  }

  /*
   * This method will remove the first occurance of any of the ips  mentioned in the predicate.
   * Eg: l = List(Master,slave1,Master,slave2,slave3) is the list of nodes where first Master is
   * the Driver  node.
   * this method withoutFirst (l)(x=> x == 'Master') will remove the first occurance of Master.
   * The resulting List containt List(slave1,Master,slave2,slave3)
   */
  def withoutDriverIP[A](xs: List[A])(p: A => Boolean): List[A] = {
    xs match {
      case x :: rest => if (p(x)) {
        rest
      } else {
        x :: withoutDriverIP(rest)(p)
      }
      case _ => Nil
    }
  }


  def getDimensions(carbonTable: CarbonTable,
      aggregateAttributes: List[AggregateTableAttributes]): Array[String] = {
    var dimArray = Array[String]()
    aggregateAttributes.filter { agg => null == agg.aggType }.foreach { agg =>
      val colName = agg.colName
      if (null != carbonTable.getMeasureByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Measure must be provided along with aggregate function :: $colName")
      }
      if (null == carbonTable.getDimensionByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Invalid column name. Cannot create an aggregate table :: $colName")
      }
      if (dimArray.contains(colName)) {
        sys.error(s"Duplicate column name. Cannot create an aggregate table :: $colName")
      }
      dimArray :+= colName
    }
    dimArray
  }

  def getAggregateTableName(carbonTable: CarbonTable, factTableName: String): String = {
    CarbonUtil.getNewAggregateTableName(carbonTable.getAggregateTablesName, factTableName)
  }

  /**
   * Shows all schemas which has Database name like
   */
  def showSchemas(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.map { c =>
      schemaLike match {
        case Some(name) =>
          if (c.carbonTableIdentifier.getDatabaseName.contains(name)) {
            c.carbonTableIdentifier
              .getDatabaseName
          }
          else {
            null
          }
        case _ => c.carbonTableIdentifier.getDatabaseName
      }
    }.filter(f => f != null)
  }

  /**
   * Shows all cubes for given schema.
   */
  def getCubes(databaseName: Option[String])(sqlContext: SQLContext): Seq[(String, Boolean)] = {

    val schemaName = databaseName
      .getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.filter { c =>
      c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(schemaName)
    }.map { c => (c.carbonTableIdentifier.getTableName, false) }
  }

  /**
   * Shows all cubes in all schemas.
   */
  def getAllCubes()(sqlContext: SQLContext): Seq[(String, String)] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta
      .map { c => (c.carbonTableIdentifier.getDatabaseName, c.carbonTableIdentifier.getTableName) }
  }

  def dropCube(partitionCount: Int, tableStorePath: String, schemaName: String, cubeName: String)
    (sqlContext: SQLContext) {
    if (!cubeExists(Seq(schemaName, cubeName))(sqlContext)) {
      LOGGER.audit(s"Drop Table failed. Table with $schemaName.$cubeName does not exist")
      sys.error(s"Table with $schemaName.$cubeName does not exist")
    }

    val carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + cubeName)

    if (null != carbonTable) {
      val metadatFilePath = carbonTable.getMetaDataFilepath
      val fileType = FileFactory.getFileType(metadatFilePath)

      if (FileFactory.isFileExist(metadatFilePath, fileType)) {
        val file = FileFactory.getCarbonFile(metadatFilePath, fileType)
        CarbonUtil.renameCubeForDeletion(partitionCount, tableStorePath, schemaName, cubeName)
        CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      }

      val partitionLocation = tableStorePath + File.separator + "partition" + File.separator +
                              schemaName + File.separator + cubeName
      val partitionFileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, partitionFileType)) {
        CarbonUtil
          .deleteFoldersAndFiles(FileFactory.getCarbonFile(partitionLocation, partitionFileType))
      }
    }

    try {
      sqlContext.sql(s"DROP TABLE $schemaName.$cubeName").collect()
    } catch {
      case e: Exception =>
        LOGGER.audit(
          s"Error While deleting the table $schemaName.$cubeName during drop Table" + e.getMessage)
    }

    metadata.cubesMeta -= metadata.cubesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(schemaName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(cubeName))(0)
    org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .removeTable(schemaName + "_" + cubeName)
    logInfo(s"Table $cubeName of $schemaName Database dropped syccessfully.")
    LOGGER.info("Table " + cubeName + " of " + schemaName + " Database dropped syccessfully.")

  }

  private def getTimestampFileAndType(schemaName: String, cubeName: String) = {

    val timestampFile = storePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE

    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  def updateSchemasUpdatedTime(schemaName: String, cubeName: String) {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)

    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $schemaName.$cubeName")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }

    touchSchemasTimestampFile(schemaName, cubeName)

    cubeModifiedTimeStore.put("default",
      FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime)

  }

  def touchSchemasTimestampFile(schemaName: String, cubeName: String) {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(System.currentTimeMillis())
  }

  def checkSchemasModifiedTimeAndReloadCubes() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime == cubeModifiedTimeStore.get("default"))) {
        refreshCache()
      }
    }
  }

  def refreshCache() {
    metadata.cubesMeta = loadMetadata(storePath).cubesMeta
  }

  def getSchemaLastUpdatedTime(schemaName: String, cubeName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      schemaLastUpdatedTime = FileFactory.getCarbonFile(timestampFile, timestampFileType)
        .getLastModifiedTime
    }
    schemaLastUpdatedTime
  }

  def readCubeMetaDataFile(cubeFolder: CarbonFile,
      fileType: FileFactory.FileType):
  (String, String, String, String, Partitioner, Long) = {
    val cubeMetadataFile = cubeFolder.getAbsolutePath + "/metadata"

    var schema: String = ""
    var schemaName: String = ""
    var cubeName: String = ""
    var dataPath: String = ""
    var partitioner: Partitioner = null
    val cal = new GregorianCalendar(2011, 1, 1)
    var cubeCreationTime = cal.getTime.getTime

    if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
      // load metadata
      val in = FileFactory.getDataInputStream(cubeMetadataFile, fileType)
      var len = 0
      try {
        len = in.readInt()
      } catch {
        case others: EOFException => len = 0
      }

      while (len > 0) {
        val schemaNameBytes = new Array[Byte](len)
        in.readFully(schemaNameBytes)

        schemaName = new String(schemaNameBytes, "UTF8")
        val cubeNameLen = in.readInt()
        val cubeNameBytes = new Array[Byte](cubeNameLen)
        in.readFully(cubeNameBytes)
        cubeName = new String(cubeNameBytes, "UTF8")

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
          cubeCreationTime = in.readLong()
          len = in.readInt()
        } catch {
          case others: EOFException => len = 0
        }

      }
      in.close()
    }

    (schemaName, cubeName, dataPath, schema, partitioner, cubeCreationTime)
  }

}


object CarbonMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ShortType |
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
