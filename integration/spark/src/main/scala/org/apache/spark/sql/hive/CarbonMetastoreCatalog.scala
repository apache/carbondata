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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException, File, ObjectInputStream, ObjectOutputStream}
import java.net.{InetAddress, InterfaceAddress, NetworkInterface}
import java.util.{GregorianCalendar, HashMap}

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.cubemodel.{AggregateTableAttributes, Partitioner}
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.fileperations.{FileWriteOperation, AtomicFileOperationsImpl}
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.metadata.CarbonMetadata.{Dimension, Cube}
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.util.{CarbonUtil, CarbonVersion, CarbonProperties}
import org.carbondata.integration.spark.load.CarbonLoaderUtil
import org.carbondata.integration.spark.util.CarbonScalaUtil.OlapUtil
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.util.CarbonEngineLogEvent
import org.eigenbase.xom.XOMUtil

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList, seqAsJavaList}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

case class MetaData(var cubesMeta: ArrayBuffer[TableMeta])

case class TableMeta(schemaName: String, cubeName: String, dataPath: String, schema: CarbonDef.Schema, var cube: Cube, partitioner: Partitioner, cubeCreationTime:Long)

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
      //      if(encrypted) {
      //        decryptedStream = SimpleFileEncryptor.decryptContent(bais);
      //      }
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

  @transient val LOGGER = LogServiceFactory.getLogService("org.apahce.spark.sql.CarbonMetastoreCatalog");

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
    val cubeMeta = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(schemaName) && (c.cubeName.equalsIgnoreCase(cubeName))))
    val cubeCreationTime = cubeMeta.head.cubeCreationTime
    cubeCreationTime
    }
  

   def lookupRelation2(tableIdentifier: Seq[String],
                     alias: Option[String] = None)(sqlContext: SQLContext): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadCubes()
    tableIdentifier match {
      case Seq(schemaName, cubeName) =>
        val cubes = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(schemaName) && (c.cubeName.equalsIgnoreCase(cubeName))))
        if (cubes.length > 0)
          CarbonRelation(schemaName, cubeName, OlapUtil.createSparkMeta(cubes.head.cube), cubes.head, alias)(sqlContext)
        else {
          LOGGER.audit(s"Table Not Found: $schemaName $cubeName")
          sys.error(s"Table Not Found: $schemaName $cubeName")
        }
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(currentDatabase) && (c.cubeName.equalsIgnoreCase(cubeName))))
        if (cubes.length > 0)
          CarbonRelation(currentDatabase, cubeName, OlapUtil.createSparkMeta(cubes.head.cube), cubes.head, alias)(sqlContext)
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
        val cubes = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(schemaName) && (c.cubeName.equalsIgnoreCase(cubeName))))
        cubes.length > 0
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(currentDatabase) && (c.cubeName.equalsIgnoreCase(cubeName))))
        cubes.length > 0
      case _ => false
    }
  }

  //    def registerTable(
  //      databaseName: Option[String],
  //      tableName: String,
  //      plan: LogicalPlan): Unit = {
  //  }
  //  /**
  //   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
  //   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
  //   */
  // override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
  //
  // }
  //
  //     def unregisterTable(
  //      databaseName: Option[String],
  //      tableName: String) = {}
  //
  //  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
  //
  //  }
  //
  //  override def unregisterAllTables(): Unit = {}

  def loadMetadata(metadataPath: String): MetaData = {
    if (metadataPath == null) return null
    //    System.setProperty("com.huawei.iweb.LogService", "com.huawei.unibi.carbon.log.CarbonLogService");
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
	          val cubeFolders = schemaFolder.listFiles();

            cubeFolders.foreach(cubeFolder => {
              if (cubeFolder.isDirectory()) {
	            	val cubeMetadataFile = cubeFolder.getAbsolutePath() + "/metadata"

                if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
		            	//load metadata
		            	val in = FileFactory.getDataInputStream(cubeMetadataFile, fileType)
		            	var len = 0
                  try {
		            		len = in.readInt()
		            	}
                  catch {
                    case others: EOFException => len = 0
		            	}

                  while (len > 0) {
		            		val schemaNameBytes = new Array[Byte](len)
					        in.readFully(schemaNameBytes)

                    val schemaName = new String(schemaNameBytes, "UTF8")
					        val cubeNameLen = in.readInt()
					        val cubeNameBytes = new Array[Byte](cubeNameLen)
					        in.readFully(cubeNameBytes)
                    val cubeName = new String(cubeNameBytes, "UTF8")

		            		val dataPathLen = in.readInt()
					        val dataPathBytes = new Array[Byte](dataPathLen)
					        in.readFully(dataPathBytes)
                    val dataPath = new String(dataPathBytes, "UTF8")

		            		val versionLength = in.readInt()
					        val versionBytes = new Array[Byte](versionLength)
					        in.readFully(versionBytes)
                    val version = new String(versionBytes, "UTF8")

		            		val schemaLen = in.readInt()
					        val schemaBytes = new Array[Byte](schemaLen)
					        in.readFully(schemaBytes)
                    val schema = new String(schemaBytes, "UTF8")

		            		val partitionLength = in.readInt()
					        val partitionBytes = new Array[Byte](partitionLength)
					        in.readFully(partitionBytes)
				            val inStream = new ByteArrayInputStream(partitionBytes)
				            val objStream = new ObjectInputStream(inStream)
				            val partitioner = objStream.readObject().asInstanceOf[Partitioner]
				            objStream.close

                    val cal = new GregorianCalendar(2011, 1, 1)
				    var cubeCreationTime=cal.getTime().getTime()
                    try {
				    cubeCreationTime = in.readLong()
				            	len = in.readInt()
		            		}
                    catch {
                      case others: EOFException => len = 0
		            		}
				        	val mondSchema = CarbonMetastoreCatalog.parseStringToSchema(schema)
                  val cubeUniqueName = schemaName + "_" + cubeName
		            		CarbonMetadata.getInstance().loadSchema(mondSchema)
		            		val cube = CarbonMetadata.getInstance().getCube(cubeUniqueName)
                    metaDataBuffer += TableMeta(
		            				schemaName,
		            				cubeName,
		            				dataPath,
		            				mondSchema,
		            				cube,
				      updatePartitioner(partitioner, cube),
				      cubeCreationTime)
		            	}
		            	in.close
	            	}
	            }
	          })
	        }
	      })
      }
      else {
	      //Create folders and files.
	      FileFactory.mkdirs(schemasPath, fileType)
	      //FileFactory.createNewFile(metadataPath+"/"+"metadata", fileType)
        
	    }
    }
    catch {
      case s: java.io.FileNotFoundException =>
        //Create folders and files.
        FileFactory.mkdirs(schemasPath, fileType)
        //FileFactory.createNewFile(metadataPath+"/"+"metadata", fileType)
      
    }
  }

  /**
   * Add schema to the catalog and perisist to the metadata
   */
  def createCube(schemaName: String, cubeName: String, schemaXML: String, partitioner: Partitioner, aggTablesGen: Boolean)
                (sqlContext: SQLContext) : String =  {
    if (cubeExists(Seq(schemaName, cubeName))(sqlContext))
      sys.error(s"Cube [$cubeName] already exists under schema [$schemaName]")
    var cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    var schema = CarbonMetastoreCatalog.parseStringToSchema(schemaXML)
    //if(aggTablesGen) schema = GenerateAggTables(schema).apply
    //Remove the cube and load again for aggregates.
    CarbonMetadata.getInstance().loadSchema(schema)
    cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)

    val cubeMetaDataPath = if(useUniquePath) {
      metadataPath + "/" + schemaName + "/" + cubeName + "/schemas/" + schemaName + "/" + cubeName
    } else {
      metadataPath + "/schemas/" + schemaName + "/" + cubeName
    }
    
    val dataPath = if(useUniquePath) {
      metadataPath + "/" + schemaName + "/" + cubeName + "/store"
    } else {
      metadataPath + "/store"
    }
    
    val cubeCreationTime = System.currentTimeMillis()
    val cubeMeta = TableMeta(
        schemaName,
        cubeName,
      dataPath,
        schema,
        cube,
        updatePartitioner(partitioner, cube),
        cubeCreationTime)

    val fileType = FileFactory.getFileType(metadataPath)
    //Debug code to print complexTypes
//    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.
//    	lookupRelation2(Seq(cubeMeta.schemaName, cubeMeta.cubeName))(sqlContext).asInstanceOf[CarbonRelation]
//    val complexTypes = cubeMeta.schema.cubes(0).dimensions.filter(aDim => aDim.asInstanceOf[CarbonDef.Dimension].hierarchies(0).levels.length > 1).map(dim => dim.name)
//    val complexTypeStrings = relation.output.filter(attr => complexTypes.contains(attr.name)).map(attr => (attr.name, attr.dataType.simpleString))
//    complexTypeStrings.map(a => println(a._1 +" "+a._2))
    if (!FileFactory.isFileExist(cubeMetaDataPath, fileType)) {
      FileFactory.mkdirs(cubeMetaDataPath, fileType)
    }

    val file = FileFactory.getCarbonFile(cubeMetaDataPath, fileType)

    val out = FileFactory.getDataOutputStream(cubeMetaDataPath + "/" + "metadata", fileType)

      val schemaNameBytes = cubeMeta.schemaName.getBytes()
      val cubeNameBytes = cubeMeta.cubeName.getBytes()
      val dataPathBytes = cubeMeta.dataPath.getBytes()
      val schemaArray = cubeMeta.schema.toXML.getBytes()
      val outStream = new ByteArrayOutputStream
      val objStream = new ObjectOutputStream(outStream)
      objStream.writeObject(cubeMeta.partitioner);
      objStream.close
      val partitionArray = outStream.toByteArray()
      val partitionClass = cubeMeta.partitioner.partitionClass.getBytes()
    val versionNoBytes = CarbonVersion.getCubeVersion().getBytes()
      out.writeInt(schemaNameBytes.length)
      out.write(schemaNameBytes)
      out.writeInt(cubeNameBytes.length)
      out.write(cubeNameBytes)
      out.writeInt(dataPathBytes.length)
      out.write(dataPathBytes)
      out.writeInt(versionNoBytes.length)
      out.write(versionNoBytes)
      out.writeInt(schemaArray.length)
      out.write(schemaArray)
      out.writeInt(partitionArray.length)
      out.write(partitionArray)
      out.writeLong(cubeCreationTime)
      out.close

      metadata.cubesMeta += cubeMeta
      logInfo(s"Cube $cubeName for schema $schemaName created successfully.")
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Cube " + cubeName + " for schema " + schemaName + " created successfully.")
    updateSchemasUpdatedTime(schemaName, cubeName)
    metadataPath + "/" + schemaName + "/" + cubeName
    //  updateCubeCreationTimeMap(schemaName, cubeName)
  }

  def updatePartitioner(partitioner: Partitioner, cube: Cube): Partitioner = {
    if (partitioner.partitionColumn == null) {
      var level: Dimension = null;
      cube.getDimensions(cube.getFactTableName()).foreach { dim =>
        if (level == null) level = dim
        //        if(level.getNoOfbits() < dim.getNoOfbits()) level = dim
      }

      Partitioner(partitioner.partitionClass, if (null == level) null else Seq(level.getColName()).toArray, partitioner.partitionCount, getNodeList())
    } else {
      Partitioner(partitioner.partitionClass, partitioner.partitionColumn, partitioner.partitionCount, getNodeList())
    }
  }


  //  def getNodeList(): Array[String] = {
  //	val arr =
  //		olap.sparkContext.getExecutorMemoryStatus.map{
  //			kv=>
  //			val addr = InetAddress.getByName( (kv._1.split(":")(0)) )
  //			addr.getHostName()
  //	}.toSeq
  //	val localhost = InetAddress.getLocalHost().getHostName()
  //	val nodelist : List[String] = withoutFirst(arr.toList)(_ == localhost)
  //	nodelist.toSeq.toArray
  //
  //}
  //  def withoutFirst[A](xs: List[A])(p: A => Boolean): List[A] = xs.toList match {
  //	case x :: rest => if (p(x)) rest else x :: withoutFirst(rest)(p)
  //	case _ => Nil
  //}
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
	    	//val addr = InetAddress.getByName( (kv._1.split(":")(0)) )
	    	//addr.getHostName()
		}.toSeq
    val localhostIPs = getLocalhostIPs
 //   val ipcheck: List[(String,Boolean)] = localhostIPs.map( x=> (x,arr.contains(x))).filter()

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

 /* def getLocalhosts() = {
    val iface = NetworkInterface.getNetworkInterfaces()
    val ifaces_list = new scala.collection.JavaConversions.JEnumerationWrapper(iface)
    val ips = ifaces_list.toSeq.map(x=>new JEnumerationWrapper(x.getInetAddresses).map(_.getAddress()))

  }
   *
  */
	/*
	val set = new TreeSet[String]
    olap.sparkContext.getExecutorMemoryStatus.foreach{kv=>
      set.add(kv._1.split(":")(0))
    }
	if(set.size>0)
	{
	    if(!olap.sparkContext.getConf.get("spark.executor.at.driver","true").toBoolean) {
	      val curHostName = InetAddress.getLocalHost().getHostName()
	      set.filterNot(_.equalsIgnoreCase(curHostName)).toArray
	    } else {
        set.toArray
	    }
	} else {
	  Seq("localhost").toArray
	}
	* */


  def loadCube(schemaPath: String, encrypted: Boolean, aggTablesGen: Boolean, partitioner: Partitioner)(sqlContext: SQLContext) {
    val schema = CarbonMetastoreCatalog.readSchema(schemaPath, encrypted)
    loadCube(schema, aggTablesGen, partitioner)(sqlContext)
  }

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

  def getDimensions(cube: Cube, aggregateAttributes: List[AggregateTableAttributes]): Array[String] = {
    var dimArray = Array[String]()
    aggregateAttributes.filter { agg => null == agg.aggType }.map { agg =>
      val colName = agg.colName
      if (null != cube.getMeasure(cube.getFactTableName(), colName)) sys.error(s"Measure must be provided along with aggregate function :: $colName")
      if (null == cube.getDimensionByLevelName(colName, colName, colName, cube.getFactTableName())) sys.error(s"Invalid column name. Cannot create an aggregate table :: $colName")
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
    var cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    if (null == cube) {
      throw new Exception("Missing metadata for the aggregate table: " + aggTableName)
    }
    val aggregateAttributes = validateAndGetNewAggregateColsList(schema, aggColsList)
    var aggTableColumns = getDimensions(cube, aggregateAttributes)
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
    val list = CarbonMetadata.getInstance().getAggLevelsForAggTable(cube, aggTableName, aggTableColumns.toList)
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

  def getAggregateTableName(schema: CarbonDef.Schema, factTableName: String): String = {
    var dbTablesList = Array[String]()
    val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
    aggTables.foreach { aggTable => dbTablesList :+= CarbonLoaderUtil.getAggregateTableName(aggTable) }
    val newAggregateTableName = CarbonUtil.getNewAggregateTableName(dbTablesList.toList, factTableName)
    return newAggregateTableName
  }

  def  updateCube(schema: CarbonDef.Schema, aggTablesGen: Boolean)(sqlContext: SQLContext) {
    val schemaName = schema.name
    val cubeName = schema.cubes(0).name
    val schemaXML: String = schema.toXML
    if (!cubeExists(Seq(schemaName, cubeName))(sqlContext)) {
       sys.error(s"Cube does not exist with $schemaName and $cubeName to update")
     }
    val schemaNew = CarbonMetastoreCatalog.parseStringToSchema(schemaXML)
    //if(aggTablesGen) schemaNew = GenerateAggTables(schemaNew).apply
    //Remove the cube and load again for aggregates.
    CarbonMetadata.getInstance().loadSchema(schemaNew)
    val cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    val fileType = FileFactory.getFileType(metadataPath)

    val metadataFilePath = if(useUniquePath) {
      metadataPath + '/' +  schemaName + "/" + cubeName + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    } else { 
      metadataPath + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    }
    val oldMetadataFile = FileFactory.getCarbonFile(metadataFilePath, fileType)

    /*if(oldMetadataFile.exists())
    {
      oldMetadataFile.delete()
    }*/
    val fileOperation = new AtomicFileOperationsImpl(metadataFilePath, FileFactory.getFileType(metadataFilePath))
    val tempMetadataFilePath = metadataFilePath + CarbonCommonConstants.UPDATING_METADATA

    if (FileFactory.isFileExist(tempMetadataFilePath, fileType)) {
      FileFactory.getCarbonFile(tempMetadataFilePath, fileType).delete()
    }

    FileFactory.createNewFile(tempMetadataFilePath, fileType)

    val tempMetadataFile = FileFactory.getCarbonFile(tempMetadataFilePath, fileType)

    metadata.cubesMeta.map { c =>
      if (c.schemaName.equalsIgnoreCase(schemaName) && c.cubeName.equalsIgnoreCase(cubeName)) {
        val cubeMeta = TableMeta(schemaName, cubeName, metadataPath+"/store", schemaNew, cube, updatePartitioner(c.partitioner, cube),c.cubeCreationTime)
        val out = fileOperation.openForWrite(FileWriteOperation.OVERWRITE)

        val schemaNameBytes = c.schemaName.getBytes()
        val cubeNameBytes = c.cubeName.getBytes()
        val dataPathBytes = c.dataPath.getBytes()
        val schemaArray = schemaNew.toXML.getBytes()
        val outStream = new ByteArrayOutputStream
        val objStream = new ObjectOutputStream(outStream)
        objStream.writeObject(c.partitioner);
        objStream.close
        val partitionArray = outStream.toByteArray()
        val partitionClass = c.partitioner.partitionClass.getBytes()
        val versionNoBytes = CarbonVersion.getCubeVersion().getBytes()

        out.writeInt(schemaNameBytes.length)
        out.write(schemaNameBytes)
        out.writeInt(cubeNameBytes.length)
        out.write(cubeNameBytes)
        out.writeInt(dataPathBytes.length)
        out.write(dataPathBytes)
        out.writeInt(versionNoBytes.length)
        out.write(versionNoBytes)
      	out.writeInt(schemaArray.length)
      	out.write(schemaArray)
      	out.writeInt(partitionArray.length)
      	out.write(partitionArray)
        out.writeLong(c.cubeCreationTime)
      	fileOperation.close()
//      	out.close
//      	tempMetadataFile.renameForce(oldMetadataFile.getAbsolutePath())
      }


    }
    refreshCache
    updateSchemasUpdatedTime(schemaName, cubeName)
  }

  def loadCube(schema: CarbonDef.Schema, aggTablesGen: Boolean, partitioner: Partitioner)(sqlContext: SQLContext) {
    var partitionerLocal = partitioner
    if (partitionerLocal == null) {
      partitionerLocal = Partitioner("org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl", null, 20, null)
    }
    createCube(schema.name, schema.cubes(0).name, schema.toXML, partitionerLocal, aggTablesGen)(sqlContext)
  }

  /**
   * Shows all schemas which has schema name like
   */
  def showSchemas(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.map { c =>
      println(c.schemaName)
      schemaLike match {
        case Some(name) =>
          if (c.schemaName.contains(name)) c.schemaName else null
        case _ => c.schemaName
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
      c.schemaName.equalsIgnoreCase(schemaName)
    }.map { c => (c.cubeName, false) }
  }

 /**
   * Shows all cubes in all schemas.
   */
 def getAllCubes()(sqlContext: SQLContext): Seq[(String, String)] = {
    checkSchemasModifiedTimeAndReloadCubes()
    metadata.cubesMeta.map { c => (c.schemaName, c.cubeName) }
  }

  def dropCube(partitionCount: Int, storePath: String, schemaName: String, cubeName: String)(sqlContext: SQLContext) {
    if (!cubeExists(Seq(schemaName, cubeName))((sqlContext))) {
      LOGGER.audit(s"Drop cube failed. Cube with $schemaName.$cubeName does not exist");
      sys.error(s"Cube with $schemaName.$cubeName does not exist")
    }

    val cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName)

    if (null != cube) {
    	val metadatFilePath = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName).getMetaDataFilepath()
    	val fileType = FileFactory.getFileType(metadatFilePath)

      if (FileFactory.isFileExist(metadatFilePath, fileType)) {
    	    val file = FileFactory.getCarbonFile(metadatFilePath, fileType)
          CarbonUtil.renameCubeForDeletion(partitionCount, storePath, schemaName, cubeName)
    	    CarbonUtil.deleteFoldersAndFilesSilent(file)
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

    metadata.cubesMeta -= metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(schemaName) && (c.cubeName.equalsIgnoreCase(cubeName))))(0)
    CarbonMetadata.getInstance().removeCube(schemaName + '_' + cubeName)
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
        val (timestampFile, timestampFileType) = getTimestampFileAndType(c.schemaName, c.cubeName)

        if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
          if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime() == cubeModifiedTimeStore.get(c.schemaName + "_" + c.cubeName))) {
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

  private def loadCubeFromMetaData(
    fileType: FileFactory.FileType,
    buffer: scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.hive.TableMeta],
    cubeFolder: CarbonFile): Unit = {
        if (cubeFolder.isDirectory()) {

         val (schemaName,cubeName,dataPath,schema,partitioner,cubeCreationTime) = readCubeMetaDataFile(cubeFolder,fileType)

            val mondSchema = CarbonMetastoreCatalog.parseStringToSchema(schema)
            val cubeUniqueName = schemaName + "_" + cubeName
            CarbonMetadata.getInstance().loadSchema(mondSchema)
            val cube = CarbonMetadata.getInstance().getCube(cubeUniqueName)
            buffer += TableMeta(
                schemaName,
                cubeName,
                dataPath,
                mondSchema,
                cube,
                updatePartitioner(partitioner, cube),cubeCreationTime)
          }
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

case class CarbonMetaData(dims: Seq[String], msrs: Seq[String], cube: Cube)