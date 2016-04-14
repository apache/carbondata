/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hive

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.File
import java.io.ObjectInputStream
import java.net.InetAddress
import java.net.InterfaceAddress
import java.net.NetworkInterface
import java.util.GregorianCalendar
import java.util.HashMap
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers
import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.cubemodel.AggregateTableAttributes
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.thrift.TDeserializer
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.metadata.converter.SchemaConverter
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl
import org.carbondata.core.datastorage.store.fileperations.FileWriteOperation
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.carbondata.core.metadata.CarbonMetadata.Cube
import org.carbondata.core.metadata.CarbonMetadata.Dimension
import org.carbondata.core.util.CarbonProperties
import org.carbondata.core.util.CarbonUtil
import org.carbondata.format.ColumnSchema
import org.carbondata.format.ConvertedType
import org.carbondata.format.Encoding
import org.carbondata.format.SchemaEvolution
import org.carbondata.format.SchemaEvolutionEntry
import org.carbondata.format.TableInfo
import org.carbondata.format.TableSchema
import org.carbondata.integration.spark.load.CarbonLoaderUtil
import org.carbondata.integration.spark.util.CarbonScalaUtil.CarbonSparkUtil
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.util.CarbonEngineLogEvent
import org.eigenbase.xom.XOMUtil
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.carbondata.core.writer.ThriftWriter
import org.carbondata.core.reader.ThriftReader
import org.apache.thrift.TBase

case class MetaData(var cubesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String], msrs: Seq[String], carbonTable: CarbonTable, cube: Cube = null)

//case class TableMeta(schemaName: String, cubeName: String, dataPath: String, schema: CarbonDef.Schema, var cube: Cube, partitioner: Partitioner, cubeCreationTime:Long)
case class TableMeta(dbName: String, tableName: String, dataPath: String, carbonTable: CarbonTable, partitioner: Partitioner, schema: CarbonDef.Schema = null, var cube: Cube = null)

object CarbonMetastoreCatalog {

   
  def parseStringToSchema(schema: String): CarbonDef.Schema = {
    val xmlParser = XOMUtil.createDefaultParser()
    val baoi = new ByteArrayInputStream(schema.getBytes())
    val defin = xmlParser.parse(baoi)
    new CarbonDef.Schema(defin)
  }

      /**
     * Gets content via Apache VFS and decrypt the content.
     *  File must exist and have content.
     *
     * @param url String
     * @return Apache VFS FileContent for further processing
     */
  def readSchema(url: String, encrypted: Boolean): CarbonDef.Schema = {
    val fileType = FileFactory.getFileType(url)
    val out = FileFactory.getDataInputStream(url, fileType)
    val baos = new ByteArrayOutputStream();
    var bais: ByteArrayInputStream = null;
    var decryptedStream: ByteArrayInputStream = null;
    val buffer = new Array[Byte](1024);
    var read = 0;
    try {
      read = out.read(buffer, 0, buffer.length)
      while (read != -1) {
        baos.write(buffer, 0, read);
        read = out.read(buffer, 0, buffer.length)
      }
      baos.flush();
      bais = new ByteArrayInputStream(baos.toByteArray())
      decryptedStream = bais
      val xmlParser = XOMUtil.createDefaultParser()
      val defin = xmlParser.parse(decryptedStream)
      out.close
      baos.close
      decryptedStream.close
      return new CarbonDef.Schema(defin);
    }
    catch {
      case s: Exception =>
        throw s
      }
    }
}

class CarbonMetastoreCatalog(hive: HiveContext, val metadataPath: String,client: ClientInterface)
  extends HiveMetastoreCatalog(client, hive)
  with spark.Logging {

  @transient val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog");

   val cubeModifiedTimeStore = new HashMap[String, Long]()
   cubeModifiedTimeStore.put("default", System.currentTimeMillis())
  
  val metadata = loadMetadata(metadataPath)
  
  lazy val useUniquePath = if("true".equalsIgnoreCase(CarbonProperties.getInstance().
      getProperty(
                CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH,
                CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH_DEFAULT))){ true } else { false }
    
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
        case s:java.lang.Exception =>
          lookupRelation2(tableIdentifier, alias)(hive.asInstanceOf[SQLContext])
      }
    }

  def getCubeCreationTime(schemaName: String, cubeName: String): Long = {
    val cubeMeta = metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(schemaName) && (c.tableName.equalsIgnoreCase(cubeName))))
    val cubeCreationTime = cubeMeta.head.carbonTable.getTableLastUpdatedTime
    cubeCreationTime
    }
  

   def lookupRelation2(tableIdentifier: Seq[String],
                     alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadCubes()
    tableIdentifier match {
      case Seq(schemaName, cubeName) =>
        val cubes = metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(schemaName) && (c.tableName.equalsIgnoreCase(cubeName))))
        if (cubes.length > 0)
          CarbonRelation(schemaName, cubeName, CarbonSparkUtil.createSparkMeta(cubes.head.carbonTable), cubes.head, alias)(sqlContext)
        else {
          LOGGER.audit(s"Table Not Found: $schemaName $cubeName")
          sys.error(s"Table Not Found: $schemaName $cubeName")
        }
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(currentDatabase) && (c.tableName.equalsIgnoreCase(cubeName))))
        if (cubes.length > 0)
          CarbonRelation(currentDatabase, cubeName, CarbonSparkUtil.createSparkMeta(cubes.head.carbonTable), cubes.head, alias)(sqlContext)
        else {
          LOGGER.audit(s"Table Not Found: $cubeName")
          sys.error(s"Table Not Found: $cubeName")
        }
      case _ => {
        LOGGER.audit(s"Table Not Found: $tableIdentifier")
        sys.error(s"Table Not Found: $tableIdentifier")
      }
    }
  }

  def cubeExists(db: Option[String], tableName: String)(sqlContext: SQLContext): Boolean = {
    if (db.get == null || db.get == "") {
      cubeExists(Seq(tableName))(sqlContext)
    }
      else {
          cubeExists(Seq(db.get, tableName))(sqlContext)
          }
  }

  def cubeExists(tableIdentifier: Seq[String])(sqlContext: SQLContext): Boolean = {
    checkSchemasModifiedTimeAndReloadCubes()
    tableIdentifier match {
      case Seq(schemaName, cubeName) =>
        val cubes = metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(schemaName) && (c.tableName.equalsIgnoreCase(cubeName))))
        cubes.length > 0
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(currentDatabase) && (c.tableName.equalsIgnoreCase(cubeName))))
        cubes.length > 0
      case _ => false
    }
  }

  def loadMetadata(metadataPath: String): MetaData = {
    if (metadataPath == null) return null
    val fileType = FileFactory.getFileType(metadataPath)

    
    CarbonProperties.getInstance().addProperty("carbon.storelocation", metadataPath);

    val metaDataBuffer = new ArrayBuffer[TableMeta]
    
    if(useUniquePath){
      if (FileFactory.isFileExist(metadataPath, fileType)) {
        val file = FileFactory.getCarbonFile(metadataPath, fileType)
        val schemaFolders = file.listFiles();

        schemaFolders.foreach(schemaFolder => {
          if (schemaFolder.isDirectory()) {
            val cubeFolders = schemaFolder.listFiles();

            cubeFolders.foreach(cubeFolder => {
            val schemaPath = metadataPath + "/" + schemaFolder.getName + "/" + cubeFolder.getName
              try{
                fillMetaData(schemaPath, fileType, metaDataBuffer)
                updateSchemasUpdatedTime(schemaFolder.getName, cubeFolder.getName)
              } catch {
                case ex: org.apache.hadoop.security.AccessControlException => 
                 // Ingnore Access control exception and get only accessible cube details
              }
            })
            }
        })
      }
      
    } else {
      
    	fillMetaData(metadataPath, fileType, metaDataBuffer)
    	updateSchemasUpdatedTime("", "")
    }
    MetaData(metaDataBuffer)
    
     
  }
  
  private def fillMetaData(basePath: String, fileType: FileType, metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
	  val schemasPath = basePath + "/schemas"
      try {
          
      if (FileFactory.isFileExist(schemasPath, fileType)) {
	      val file = FileFactory.getCarbonFile(schemasPath, fileType)
	      val schemaFolders = file.listFiles();

        schemaFolders.foreach(schemaFolder => {
          if (schemaFolder.isDirectory()) {
        	  val dbName = schemaFolder.getName
	          val cubeFolders = schemaFolder.listFiles();

            cubeFolders.foreach(cubeFolder => {
              if (cubeFolder.isDirectory()) {
	            	val cubeMetadataFile = cubeFolder.getAbsolutePath() + "/metadata/schema.file"

                if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
		            	//load metadata
//		            	val in = FileFactory.getDataInputStream(cubeMetadataFile, fileType)
		            	val tableName = cubeFolder.getName
                  val cubeUniqueName = schemaFolder.getName+ "_" + cubeFolder.getName


                 val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                        return new TableInfo();
                    }
                 }
                 val thriftReader = new ThriftReader(cubeMetadataFile,  createTBase)
                 thriftReader.open()
                 val tableInfo: TableInfo =  thriftReader.read().asInstanceOf[TableInfo]
                 thriftReader.close()
                 
                 val schemaConverter = new ThriftWrapperSchemaConverterImpl
                 val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName)
                 wrapperTableInfo.setMetaDataFilepath(cubeFolder.getAbsolutePath() + "/metadata/")
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  metaDataBuffer += TableMeta(
		            				dbName,
		            				tableName,
		            				metadataPath+"/store",
		            				org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance().getCarbonTable(cubeUniqueName),
		            				//TODO: Need to update schema thirft to hold partitioner information and reload when required.
		            				    Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", Array(""), 1,  getNodeList()))
	            	}
	            }
	          })
	        }
	      })
      }
      else {
	      //Create folders and files.
	      FileFactory.mkdirs(schemasPath, fileType)

	    }
    }
    catch {
      case s: java.io.FileNotFoundException =>
        //Create folders and files.
        FileFactory.mkdirs(schemasPath, fileType)

    }
  }
  
  
  /**
   * 
   * Prepare Thrift Schema from wrapper TableInfo and write to schema file.
   * Load CarbonTable from wrapper tableinfo
   * 
   * @param tableInfo
   * @param dbName
   * @param tableName
   * @param partitioner
   * @param sqlContext
   * @return
   */
  def createCubeFromThrift(tableInfo: org.carbondata.core.carbon.metadata.schema.table.TableInfo, dbName: String, tableName: String, partitioner: Partitioner)
                                    (sqlContext: SQLContext) : String = {
    
    if (cubeExists(Seq(dbName, tableName))(sqlContext))
      sys.error(s"Table [$tableName] already exists under schema [$dbName]")

    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime) 
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.add(schemaEvolutionEntry)
    
    //TODO : Need to get from PathUtils
    val schemaMetadataPath = metadataPath + "/schemas/" + dbName + "/" + tableName + "/metadata/"
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)

    val cubeMeta = TableMeta(
        dbName,
        tableName,
        metadataPath+"/store",
        CarbonMetadata.getInstance().getCarbonTable(dbName+"_"+tableName),
            Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", Array(""), 1, getNodeList()))
        
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    
    val thriftWriter = new ThriftWriter(schemaMetadataPath+"schema.file", false)
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    
    metadata.cubesMeta += cubeMeta
      logInfo(s"Cube $tableName for schema $dbName created successfully.")
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Cube " + tableName + " for schema " + dbName + " created successfully.")
    updateSchemasUpdatedTime(dbName, tableName)
    metadataPath + "/" + dbName + "/" + tableName + "/metadata/" 
  }


//  def updatePartitioner(partitioner: Partitioner, cube: Cube): Partitioner = {
//    if (partitioner.partitionColumn == null) {
//      var level: Dimension = null;
//      cube.getDimensions(cube.getFactTableName()).foreach { dim =>
//        if (level == null) level = dim
//      }
//
//      Partitioner(partitioner.partitionClass, if (null == level) null else Seq(level.getColName()).toArray, partitioner.partitionCount, getNodeList())
//    } else {
//      Partitioner(partitioner.partitionClass, partitioner.partitionColumn, partitioner.partitionCount, getNodeList())
//    }
//  }

  /*
   * This method will return the list of executers in the cluster.
 * For this we take the  memory status of all node with getExecutorMemoryStatus
   * and extract the keys. getExecutorMemoryStatus also returns the driver memory also
 * In client mode driver will run in the localhost
   * There can be executor spawn in same drive node. So we can remove first occurance of
 * localhost for retriving executor list
 */
  def getNodeList(): Array[String] = {

    val arr =
      hive.sparkContext.getExecutorMemoryStatus.map {
        kv =>
	    	  kv._1.split(":")(0)
		}.toSeq
    val localhostIPs = getLocalhostIPs
    val selectedLocalIPList = localhostIPs.filter(arr.contains(_))

    val nodelist: List[String] = withoutDriverIP(arr.toList)(selectedLocalIPList.contains(_))
	  val masterMode = hive.sparkContext.getConf.get("spark.master")
    if (nodelist.length > 0) {
      //Specific for Yarn Mode
      if ("yarn-cluster".equals(masterMode) || "yarn-client".equals(masterMode)) {
        val nodeNames = nodelist.map { x =>
				val addr = InetAddress.getByName(x)
				addr.getHostName()
		 	}
			nodeNames.toSeq.toArray
	    }
      else {
	      //For Standalone cluster, node IPs will be returned.
	    	nodelist.toArray
	}
    }
	else
		Seq(InetAddress.getLocalHost().getHostName()).toArray
  }

  def getLocalhostIPs() = {
     val iface = NetworkInterface.getNetworkInterfaces()
    var addresses: List[InterfaceAddress] = List.empty
    while (iface.hasMoreElements()) {
     	addresses = iface.nextElement().getInterfaceAddresses().toList ++ addresses
     }
     val inets = addresses.map(_.getAddress().getHostAddress())
     inets
  }

  /*
 * This method will remove the first occurance of any of the ips  mentioned in the predicate. 
 * Eg: l = List(Master,slave1,Master,slave2,slave3) is the list of nodes where first Master is the Driver  node .
 * this method withoutFirst (l)(x=> x == 'Master') will remove the first occurance of Master. 
 * The resulting List containt List(slave1,Master,slave2,slave3)  
 *  
 */
   def withoutDriverIP[A](xs: List[A])(p: A => Boolean): List[A] = xs.toList match {
  	case x :: rest =>  if (p(x)) rest else x :: withoutDriverIP(rest)(p)
  	case _ => Nil
  }

   //old flow to create cube from schema xml
//  def loadCube(schemaPath: String, encrypted: Boolean, aggTablesGen: Boolean, partitioner: Partitioner)(sqlContext: SQLContext) {
//    val schema = CarbonMetastoreCatalog.readSchema(schemaPath, encrypted)
//    loadCube(schema, aggTablesGen, partitioner)(sqlContext)
//  }

  def updateCube(schemaPath: String, encrypted: Boolean, aggTablesGen: Boolean)(sqlContext: SQLContext) {
    val schema = CarbonMetastoreCatalog.readSchema(schemaPath, encrypted)
    updateCube(schema, aggTablesGen)(sqlContext)
  }

  def validateAndGetNewAggregateColsList(schema: CarbonDef.Schema, aggregateAttributes: List[AggregateTableAttributes]): List[AggregateTableAttributes] = {
    val dimensions = schema.cubes(0).dimensions
    val measures = schema.cubes(0).measures
    var aggColsArray = Array[AggregateTableAttributes]()
    aggregateAttributes.foreach { aggCol =>
      var found = false
      val colName = aggCol.colName
      dimensions.foreach { dimension =>
        if (dimension.name.toLowerCase().equals(colName.toLowerCase())) {
          aggColsArray :+= AggregateTableAttributes(dimension.name, aggCol.aggType)
          found = true
        }
      }
      if (!found) {
        measures.foreach { measure =>
          if (measure.name.toLowerCase().equals(colName.toLowerCase())) {
            aggColsArray :+= AggregateTableAttributes(measure.name, aggCol.aggType)
            found = true
          }
        }
        if (!found) {
          LOGGER.audit(s"Failed to create the aggregate table. Provided column does not exist in the cube :: $colName")
          sys.error(s"Provided column does not exist in the cube :: $colName")
        }
      }
    }
    aggColsArray.toList
  }

  def getDimensions(carbonTable: CarbonTable, aggregateAttributes: List[AggregateTableAttributes]): Array[String] = {
    var dimArray = Array[String]()
    aggregateAttributes.filter { agg => null == agg.aggType }.map { agg =>
      val colName = agg.colName
      if (null != carbonTable.getMeasureByName(carbonTable.getFactTableName(), colName)) sys.error(s"Measure must be provided along with aggregate function :: $colName")
      if (null == carbonTable.getDimensionByName(carbonTable.getFactTableName(), colName)) sys.error(s"Invalid column name. Cannot create an aggregate table :: $colName")
      if (dimArray.contains(colName)) {
         sys.error(s"Duplicate column name. Cannot create an aggregate table :: $colName")
      }
      dimArray :+= colName
    }
    return dimArray;
  }

  def updateCubeWithAggregatesDetail(schema: CarbonDef.Schema, updatedSchema: CarbonDef.Schema) = {
    schema.cubes(0).autoAggregationType = "auto"
    schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables = updatedSchema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
    //    return schema
  }

  def updateCubeWithAggregates(schema: CarbonDef.Schema, schemaName: String = null, cubeName: String, aggTableName: String, aggColsList: List[AggregateTableAttributes]): CarbonDef.Schema = {
    var cube = org.carbondata.core.metadata.CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    if (null == cube) {
      throw new Exception("Missing metadata for the aggregate table: " + aggTableName)
    }
    val aggregateAttributes = validateAndGetNewAggregateColsList(schema, aggColsList)
    var aggTableColumns = getDimensions(CarbonMetadata.getInstance().getCarbonTable(schemaName + "_" + cubeName), aggregateAttributes)
    if (aggTableColumns.length == 0) {
      LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Please provide at least one valid dimension name to create aggregate table successfully.")
      sys.error(s"Please provide at least one valid dimension name to create aggregate table successfully")
    }
    aggTableColumns = CarbonDataProcessorUtil.getReorderedLevels(schema, schema.cubes(0), aggTableColumns, cube.getFactTableName)
    schema.cubes(0).autoAggregationType = "auto"
    var aggDims = Array[CarbonDef.AggMeasure]()
    var msrs = Array[CarbonDef.AggMeasure]()
    aggregateAttributes.filter(agg => null != agg.aggType).foreach { measure =>
      val mondAggMsr = new CarbonDef.AggMeasure
      val name = measure.colName
      if (null != cube.getMeasure(cube.getFactTableName(), name)) {
        mondAggMsr.name = s"[Measures].[$name]"
        mondAggMsr.column = name
        val aggType = measure.aggType
        mondAggMsr.aggregator = aggType
        if (msrs.contains(mondAggMsr)) {
          LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Duplicate column with same aggregate function.")
          sys.error(s"Duplicate column with same aggregate function. Cannot create an aggregate table :: $name :: $aggType")
        }
        msrs :+= mondAggMsr
      }
      else {
        val dimension = cube.getDimension(name, cube.getFactTableName())
        if (null == dimension) sys.error(s"Invalid column name. Cannot create an aggregate table :: $name")
        val dimName = dimension.getDimName()
        val hierName = dimension.getHierName()
        val levelName = dimension.getName()
        mondAggMsr.name = s"[$dimName].[$hierName].[$levelName]"
        mondAggMsr.column = name
        val aggType = measure.aggType
        mondAggMsr.aggregator = aggType
        if (aggDims.contains(mondAggMsr)) {
          LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Duplicate column with same aggregate function.")
          sys.error(s"Duplicate column with same aggregate function. Cannot create an aggregate table :: $name :: $aggType")
        }
        aggDims :+= mondAggMsr
      }
    }

    val aggMsrs = aggDims ++ msrs

    val mondAgg = new CarbonDef.AggName()
    mondAgg.name = aggTableName
    val bufferOfCarbonAggName = new ArrayBuffer[CarbonDef.AggName]
    val list = org.carbondata.core.metadata.CarbonMetadata.getInstance().getAggLevelsForAggTable(cube, aggTableName, aggTableColumns.toList)
    val array = list.toBuffer;
    mondAgg.levels = array.toArray
    mondAgg.measures = aggMsrs
    val factCount = new CarbonDef.AggFactCount
    factCount.column = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].name + "_count"
    mondAgg.factcount = factCount
    if (CarbonLoaderUtil.aggTableAlreadyExistWithSameMeasuresndLevels(mondAgg, schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables)) {
      LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Already an aggregate table exists with same columns")
      sys.error(s"Already an aggregate table exists with same columns")
    }
    bufferOfCarbonAggName.add(mondAgg)
    schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables = (schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables.toSeq ++ bufferOfCarbonAggName).toArray
    return schema
  }

  def getAggregateTableName(carbonTable: CarbonTable, factTableName: String): String = {
    return CarbonUtil.getNewAggregateTableName(carbonTable.getAggregateTablesName, factTableName)
  }

  def  updateCube(schema: CarbonDef.Schema, aggTablesGen: Boolean)(sqlContext: SQLContext) {
    val schemaName = schema.name
    val cubeName = schema.cubes(0).name
    val schemaXML: String = schema.toXML
    if (!cubeExists(Seq(schemaName, cubeName))(sqlContext)) {
       sys.error(s"Cube does not exist with $schemaName and $cubeName to update")
     }
    val schemaNew = CarbonMetastoreCatalog.parseStringToSchema(schemaXML)
    //Remove the cube and load again for aggregates.
    org.carbondata.core.metadata.CarbonMetadata.getInstance().loadSchema(schemaNew)
    val cube = org.carbondata.core.metadata.CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    val fileType = FileFactory.getFileType(metadataPath)

    val metadataFilePath = if(useUniquePath) {
      metadataPath + '/' +  schemaName + "/" + cubeName + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    } else { 
      metadataPath + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    }
    val oldMetadataFile = FileFactory.getCarbonFile(metadataFilePath, fileType)

    val fileOperation = new AtomicFileOperationsImpl(metadataFilePath, FileFactory.getFileType(metadataFilePath))
    val tempMetadataFilePath = metadataFilePath + CarbonCommonConstants.UPDATING_METADATA

    if (FileFactory.isFileExist(tempMetadataFilePath, fileType)) {
      FileFactory.getCarbonFile(tempMetadataFilePath, fileType).delete()
    }

    FileFactory.createNewFile(tempMetadataFilePath, fileType)

    val tempMetadataFile = FileFactory.getCarbonFile(tempMetadataFilePath, fileType)

    metadata.cubesMeta.map { c =>
      if (c.dbName.equalsIgnoreCase(schemaName) && c.tableName.equalsIgnoreCase(cubeName)) {
        val cubeMeta = TableMeta(schemaName, cubeName, metadataPath+"/store", c.carbonTable, c.partitioner)
        val out = fileOperation.openForWrite(FileWriteOperation.OVERWRITE)

        //Need to be handled as per new thrift object while alter cube or adding new aggregate table
      	fileOperation.close()
      }


    }
    refreshCache
    updateSchemasUpdatedTime(schemaName, cubeName)
  }

  //Old flow to create cube from schema xml
//  def loadCube(schema: CarbonDef.Schema, aggTablesGen: Boolean, partitioner: Partitioner)(sqlContext: SQLContext) {
//    var partitionerLocal = partitioner
//    if (partitionerLocal == null) {
//      partitionerLocal = Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", null, 20, null)
//    }
//    createCube(schema.name, schema.cubes(0).name, schema.toXML, partitionerLocal, aggTablesGen)(sqlContext)
//  }

  /**
   * Shows all schemas which has schema name like
   */
  def showSchemas(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.map { c =>
      println(c.dbName)
      schemaLike match {
        case Some(name) =>
          if (c.dbName.contains(name)) c.dbName else null
        case _ => c.dbName
      }
    }.filter(f => f != null)
  }

  /**
   * Shows all cubes for given schema.
   */
 def getCubes(databaseName: Option[String])(sqlContext: SQLContext): Seq[(String, Boolean)] = {

    val schemaName = databaseName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.filter { c =>
      c.dbName.equalsIgnoreCase(schemaName)
    }.map { c => (c.tableName, false) }
  }

 /**
   * Shows all cubes in all schemas.
   */
 def getAllCubes()(sqlContext: SQLContext): Seq[(String, String)] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.map { c => (c.dbName, c.tableName) }
  }

  def dropCube(partitionCount:Int, storePath: String, schemaName: String, cubeName: String)(sqlContext: SQLContext) {
    if (!cubeExists(Seq(schemaName, cubeName))((sqlContext))) {
      LOGGER.audit(s"Drop cube failed. Cube with $schemaName.$cubeName does not exist");
      sys.error(s"Cube with $schemaName.$cubeName does not exist")
    }

    val carbonTable=org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance.getCarbonTable(schemaName + "_" + cubeName)

    if (null != carbonTable) {
    	val metadatFilePath = carbonTable.getMetaDataFilepath
    	val fileType = FileFactory.getFileType(metadatFilePath)

      if (FileFactory.isFileExist(metadatFilePath, fileType)) {
    	    val file = FileFactory.getCarbonFile(metadatFilePath, fileType)
          CarbonUtil.renameCubeForDeletion(partitionCount,storePath, schemaName, cubeName)
    	    CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
    	}

    	val partitionLocation = storePath + File.separator + "partition" + File.separator + schemaName + File.separator + cubeName
    	val partitionFileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, partitionFileType)) {
    	   CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(partitionLocation, partitionFileType))
    	}
    }

    try {
          sqlContext.sql(s"DROP TABLE $schemaName.$cubeName").collect()
    } catch {
        case e: Exception =>
        LOGGER.audit(s"Error While deleting the table $schemaName.$cubeName during drop cube" + e.getMessage)
    }

    metadata.cubesMeta -= metadata.cubesMeta.filter(c => (c.dbName.equalsIgnoreCase(schemaName) && (c.tableName.equalsIgnoreCase(cubeName))))(0)
    org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance.removeTable(schemaName + "_" + cubeName)
    logInfo(s"Cube $cubeName of $schemaName schema dropped syccessfully.")
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Cube " + cubeName + " of " + schemaName + " schema dropped syccessfully.");

  }

  def getTimestampFileAndType(schemaName : String, cubeName : String) = {
    
    var timestampFile =  if(useUniquePath)
    {
       metadataPath + "/" + schemaName + "/" + cubeName + "/schemas/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
  }
    else
    {
       metadataPath + "/schemas/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    }
  
      val timestampFileType = FileFactory.getFileType(timestampFile)
      (timestampFile, timestampFileType)
  }

  def updateSchemasUpdatedTime(schemaName : String, cubeName : String) {
   val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)

    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit("Creating timestamp file")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }

    touchSchemasTimestampFile(schemaName, cubeName)
  
    if(useUniquePath)
  {
      cubeModifiedTimeStore.put(schemaName + '_' + cubeName, FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime())
    }
    else
	  {
      cubeModifiedTimeStore.put("default", FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime())
	  }
	  
  }

  def touchSchemasTimestampFile(schemaName : String, cubeName : String) {
   val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)
	  FileFactory.getCarbonFile(timestampFile, timestampFileType).setLastModifiedTime(System.currentTimeMillis())
  }

  def checkSchemasModifiedTimeAndReloadCubes() {
    if (useUniquePath) {
      metadata.cubesMeta.foreach(c => {
        val (timestampFile, timestampFileType) = getTimestampFileAndType(c.dbName, c.tableName)

        if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
          if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime() == cubeModifiedTimeStore.get(c.dbName + "_" + c.tableName))) {
		    refreshCache
		  }
	  }
      })
    } else {
      val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
      if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
        if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
          getLastModifiedTime() == cubeModifiedTimeStore.get("default"))) {
          refreshCache
  }
      }
    }
  }

  def refreshCache() {
    metadata.cubesMeta = loadMetadata(metadataPath).cubesMeta
  }
  
  def getSchemaLastUpdatedTime(schemaName: String, cubeName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      schemaLastUpdatedTime = FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime()
    }
    schemaLastUpdatedTime
  }

  def readCubeMetaDataFile(cubeFolder: CarbonFile, fileType: FileFactory.FileType): (String, String, String, String, Partitioner, Long) = {
    val cubeMetadataFile = cubeFolder.getAbsolutePath() + "/metadata"

    var schema: String = ""
    var schemaName: String = ""
    var cubeName: String = ""
    var dataPath: String = ""
    var partitioner: Partitioner = null
    val cal = new GregorianCalendar(2011, 1, 1)
    var cubeCreationTime=cal.getTime().getTime()

    if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
      //load metadata
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
        val version = new String(versionBytes, "UTF8")

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
        objStream.close

        try {
          cubeCreationTime = in.readLong()
          len = in.readInt()
        } catch {
          case others: EOFException => len = 0
        }

      }
      in.close
      ()
    }

    return (schemaName,cubeName,dataPath,schema,partitioner,cubeCreationTime)
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
//      "decimal" ^^^ DecimalType() |
      "decimal" ^^^ "decimal" ^^^ DecimalType(18, 2) |
      "varchar\\((\\d+)\\)".r ^^^ StringType |
      "timestamp" ^^^ TimestampType

    protected lazy val arrayType: Parser[DataType] =
      "array" ~> "<" ~> dataType<~ ">" ^^ {
        case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

    protected lazy val structType: Parser[DataType] =
      "struct" ~> "<" ~> repsep(structField,",") <~ ">" ^^ {
        case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
      arrayType |
      mapType |
      structType |
      primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
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