package org.apache.spark.sql.hive

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException, File, ObjectInputStream, ObjectOutputStream}
import java.net.{InetAddress, InterfaceAddress, NetworkInterface}
import java.util.{GregorianCalendar, HashMap}

import com.huawei.datasight.molap.load.MolapLoaderUtil
import com.huawei.datasight.spark.processors.OlapUtil
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.unibi.molap.datastorage.store.fileperations.{AtomicFileOperationsImpl, FileWriteOperation}
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.metadata.MolapMetadata.{Cube, Dimension}
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.{MolapDataProcessorUtil, MolapProperties, MolapUtil, MolapVersion}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.cubemodel.{AggregateTableAttributes, Partitioner}
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.eigenbase.xom.XOMUtil

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList, seqAsJavaList}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

/**
 * Created by w00228970 on 2014/5/16.
 */

case class MetaData(var cubesMeta: ArrayBuffer[CubeMeta])

case class CubeMeta(schemaName: String, cubeName: String, dataPath: String, schema: MolapDef.Schema, var cube: Cube, partitioner: Partitioner, cubeCreationTime:Long)

object OlapMetastoreCatalog {

  def parseStringToSchema(schema: String): MolapDef.Schema = {
    val xmlParser = XOMUtil.createDefaultParser()
    val baoi = new ByteArrayInputStream(schema.getBytes())
    val defin = xmlParser.parse(baoi)
    new MolapDef.Schema(defin)
  }

      /**
     * Gets content via Apache VFS and decrypt the content.
     *  File must exist and have content.
     *
     * @param url String
     * @return Apache VFS FileContent for further processing
     */
  def readSchema(url: String, encrypted: Boolean): MolapDef.Schema = {
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
      return new MolapDef.Schema(defin);
    }
    catch {
      case s: Exception =>
        throw s
      }
    }
}

class OlapMetastoreCatalog(hive: HiveContext, val metadataPath: String,client: ClientInterface)
  extends HiveMetastoreCatalog(client, hive)
  with spark.Logging {

  @transient val LOGGER = LogServiceFactory.getLogService("org.apahce.spark.sql.OlapMetastoreCatalog");

   val cubeModifiedTimeStore = new HashMap[String, Long]()
   cubeModifiedTimeStore.put("default", System.currentTimeMillis())
  
  val metadata = loadMetadata(metadataPath)
  
  lazy val useUniquePath = if("true".equalsIgnoreCase(MolapProperties.getInstance().
      getProperty(
                MolapCommonConstants.CARBON_UNIFIED_STORE_PATH, 
                MolapCommonConstants.CARBON_UNIFIED_STORE_PATH_DEFAULT))){ true } else { false }
    
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

//    override def lookupRelation(
//        databaseName: Option[String],
//        tableName: String,
//        alias: Option[String] = None): LogicalPlan = {
//      try {
//        super.lookupRelation(databaseName, tableName, alias)
//      } catch {
//        case s:java.lang.RuntimeException =>
//           val db = databaseName match {
//             case Some(name)=> name
//             case _=>null
//           }
//           if(db == null){
//  	      lookupRelation2(Seq(tableName), alias)
//  	    } else {
//  	      lookupRelation2(Seq(db, tableName), alias)
//  	    }
//      }
//    }

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
          OlapRelation(schemaName, cubeName, OlapUtil.createSparkMeta(cubes.head.cube), cubes.head, alias)(sqlContext)
        else {
          LOGGER.audit(s"Table Not Found: $schemaName $cubeName")
          sys.error(s"Table Not Found: $schemaName $cubeName")
        }
      case Seq(cubeName) =>
        val currentDatabase = getDB.getDatabaseName(None, sqlContext)
        val cubes = metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(currentDatabase) && (c.cubeName.equalsIgnoreCase(cubeName))))
        if (cubes.length > 0)
          OlapRelation(currentDatabase, cubeName, OlapUtil.createSparkMeta(cubes.head.cube), cubes.head, alias)(sqlContext)
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
    //    System.setProperty("com.huawei.iweb.LogService", "com.huawei.unibi.molap.log.CarbonLogService");
    val fileType = FileFactory.getFileType(metadataPath)

    
    MolapProperties.getInstance().addProperty("molap.storelocation", metadataPath);

    val metaDataBuffer = new ArrayBuffer[CubeMeta]
    
    if(useUniquePath){
      if (FileFactory.isFileExist(metadataPath, fileType)) {
        val file = FileFactory.getMolapFile(metadataPath, fileType)
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
  
  private def fillMetaData(basePath: String, fileType: FileType, metaDataBuffer: ArrayBuffer[CubeMeta]): Unit = {
	  val schemasPath = basePath + "/schemas"
      try {
          
      if (FileFactory.isFileExist(schemasPath, fileType)) {
	      val file = FileFactory.getMolapFile(schemasPath, fileType)
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
				        	val mondSchema = OlapMetastoreCatalog.parseStringToSchema(schema)
                  val cubeUniqueName = schemaName + "_" + cubeName
		            		MolapMetadata.getInstance().loadSchema(mondSchema)
		            		val cube = MolapMetadata.getInstance().getCube(cubeUniqueName)
                    metaDataBuffer += CubeMeta(
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
    var cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    var schema = OlapMetastoreCatalog.parseStringToSchema(schemaXML)
    //if(aggTablesGen) schema = GenerateAggTables(schema).apply
    //Remove the cube and load again for aggregates.
    MolapMetadata.getInstance().loadSchema(schema)
    cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName)

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
    val cubeMeta = CubeMeta(
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
//    	lookupRelation2(Seq(cubeMeta.schemaName, cubeMeta.cubeName))(sqlContext).asInstanceOf[OlapRelation]
//    val complexTypes = cubeMeta.schema.cubes(0).dimensions.filter(aDim => aDim.asInstanceOf[MolapDef.Dimension].hierarchies(0).levels.length > 1).map(dim => dim.name)
//    val complexTypeStrings = relation.output.filter(attr => complexTypes.contains(attr.name)).map(attr => (attr.name, attr.dataType.simpleString))
//    complexTypeStrings.map(a => println(a._1 +" "+a._2))
    if (!FileFactory.isFileExist(cubeMetaDataPath, fileType)) {
      FileFactory.mkdirs(cubeMetaDataPath, fileType)
    }

    val file = FileFactory.getMolapFile(cubeMetaDataPath, fileType)

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
    val versionNoBytes = MolapVersion.getCubeVersion().getBytes()
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
    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Cube " + cubeName + " for schema " + schemaName + " created successfully.")
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
    val schema = OlapMetastoreCatalog.readSchema(schemaPath, encrypted)
    loadCube(schema, aggTablesGen, partitioner)(sqlContext)
  }

  def updateCube(schemaPath: String, encrypted: Boolean, aggTablesGen: Boolean)(sqlContext: SQLContext) {
    val schema = OlapMetastoreCatalog.readSchema(schemaPath, encrypted)
    updateCube(schema, aggTablesGen)(sqlContext)
  }

  def validateAndGetNewAggregateColsList(schema: MolapDef.Schema, aggregateAttributes: List[AggregateTableAttributes]): List[AggregateTableAttributes] = {
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

  def updateCubeWithAggregatesDetail(schema: MolapDef.Schema, updatedSchema: MolapDef.Schema) = {
    schema.cubes(0).autoAggregationType = "auto"
    schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables = updatedSchema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
    //    return schema
  }

  def updateCubeWithAggregates(schema: MolapDef.Schema, schemaName: String = null, cubeName: String, aggTableName: String, aggColsList: List[AggregateTableAttributes]): MolapDef.Schema = {
    var cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    if (null == cube) {
      throw new Exception("Missing metadata for the aggregate table: " + aggTableName)
    }
    val aggregateAttributes = validateAndGetNewAggregateColsList(schema, aggColsList)
    var aggTableColumns = getDimensions(cube, aggregateAttributes)
    if (aggTableColumns.length == 0) {
      LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Please provide at least one valid dimension name to create aggregate table successfully.")
      sys.error(s"Please provide at least one valid dimension name to create aggregate table successfully")
    }
    aggTableColumns = MolapDataProcessorUtil.getReorderedLevels(schema, schema.cubes(0), aggTableColumns, cube.getFactTableName)
    schema.cubes(0).autoAggregationType = "auto"
    var aggDims = Array[MolapDef.AggMeasure]()
    var msrs = Array[MolapDef.AggMeasure]()
    aggregateAttributes.filter(agg => null != agg.aggType).foreach { measure =>
      val mondAggMsr = new MolapDef.AggMeasure
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

    val mondAgg = new MolapDef.AggName()
    mondAgg.name = aggTableName
    val bufferOfMolapAggName = new ArrayBuffer[MolapDef.AggName]
    val list = MolapMetadata.getInstance().getAggLevelsForAggTable(cube, aggTableName, aggTableColumns.toList)
    val array = list.toBuffer;
    mondAgg.levels = array.toArray
    mondAgg.measures = aggMsrs
    val factCount = new MolapDef.AggFactCount
    factCount.column = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].name + "_count"
    mondAgg.factcount = factCount
    if (MolapLoaderUtil.aggTableAlreadyExistWithSameMeasuresndLevels(mondAgg, schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables)) {
      LOGGER.audit(s"Failed to create the aggregate table $aggTableName for cube $schemaName.$cubeName. Already an aggregate table exists with same columns")
      sys.error(s"Already an aggregate table exists with same columns")
    }
    bufferOfMolapAggName.add(mondAgg)
    schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables = (schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables.toSeq ++ bufferOfMolapAggName).toArray
    return schema
  }

  def getAggregateTableName(schema: MolapDef.Schema, factTableName: String): String = {
    var dbTablesList = Array[String]()
    val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
    aggTables.foreach { aggTable => dbTablesList :+= MolapLoaderUtil.getAggregateTableName(aggTable) }
    val newAggregateTableName = MolapUtil.getNewAggregateTableName(dbTablesList.toList, factTableName)
    return newAggregateTableName
  }

  def  updateCube(schema: MolapDef.Schema, aggTablesGen: Boolean)(sqlContext: SQLContext) {
    val schemaName = schema.name
    val cubeName = schema.cubes(0).name
    val schemaXML: String = schema.toXML
    if (!cubeExists(Seq(schemaName, cubeName))(sqlContext)) {
       sys.error(s"Cube does not exist with $schemaName and $cubeName to update")
     }
    val schemaNew = OlapMetastoreCatalog.parseStringToSchema(schemaXML)
    //if(aggTablesGen) schemaNew = GenerateAggTables(schemaNew).apply
    //Remove the cube and load again for aggregates.
    MolapMetadata.getInstance().loadSchema(schemaNew)
    val cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName)
    val fileType = FileFactory.getFileType(metadataPath)

    val metadataFilePath = if(useUniquePath) {
      metadataPath + '/' +  schemaName + "/" + cubeName + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    } else { 
      metadataPath + "/schemas/" + schemaName + "/" + cubeName + "/metadata"
    }
    val oldMetadataFile = FileFactory.getMolapFile(metadataFilePath, fileType)

    /*if(oldMetadataFile.exists())
    {
      oldMetadataFile.delete()
    }*/
    val fileOperation = new AtomicFileOperationsImpl(metadataFilePath, FileFactory.getFileType(metadataFilePath))
    val tempMetadataFilePath = metadataFilePath + MolapCommonConstants.UPDATING_METADATA

    if (FileFactory.isFileExist(tempMetadataFilePath, fileType)) {
      FileFactory.getMolapFile(tempMetadataFilePath, fileType).delete()
    }

    FileFactory.createNewFile(tempMetadataFilePath, fileType)

    val tempMetadataFile = FileFactory.getMolapFile(tempMetadataFilePath, fileType)

    metadata.cubesMeta.map { c =>
      if (c.schemaName.equalsIgnoreCase(schemaName) && c.cubeName.equalsIgnoreCase(cubeName)) {
        val cubeMeta = CubeMeta(schemaName, cubeName, metadataPath+"/store", schemaNew, cube, updatePartitioner(c.partitioner, cube),c.cubeCreationTime)
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
        val versionNoBytes = MolapVersion.getCubeVersion().getBytes()

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

  def loadCube(schema: MolapDef.Schema, aggTablesGen: Boolean, partitioner: Partitioner)(sqlContext: SQLContext) {
    var partitionerLocal = partitioner
    if (partitionerLocal == null) {
      partitionerLocal = Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", null, 20, null)
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

    val cube = MolapMetadata.getInstance().getCube(schemaName + '_' + cubeName)

    if (null != cube) {
    	val metadatFilePath = MolapMetadata.getInstance().getCube(schemaName + '_' + cubeName).getMetaDataFilepath()
    	val fileType = FileFactory.getFileType(metadatFilePath)

      if (FileFactory.isFileExist(metadatFilePath, fileType)) {
    	    val file = FileFactory.getMolapFile(metadatFilePath, fileType)
          MolapUtil.renameCubeForDeletion(partitionCount, storePath, schemaName, cubeName)
    	    MolapUtil.deleteFoldersAndFilesSilent(file)
    	}

    	val partitionLocation = storePath + File.separator + "partition" + File.separator + schemaName + File.separator + cubeName
    	val partitionFileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, partitionFileType)) {
    	   MolapUtil.deleteFoldersAndFiles(FileFactory.getMolapFile(partitionLocation, partitionFileType))
    	}
    }

    try {
          sqlContext.sql(s"DROP TABLE $schemaName.$cubeName").collect()
    } catch {
        case e: Exception =>
        LOGGER.audit(s"Error While deleting the table $schemaName.$cubeName during drop cube" + e.getMessage)
    }

    metadata.cubesMeta -= metadata.cubesMeta.filter(c => (c.schemaName.equalsIgnoreCase(schemaName) && (c.cubeName.equalsIgnoreCase(cubeName))))(0)
    MolapMetadata.getInstance().removeCube(schemaName + '_' + cubeName)
    logInfo(s"Cube $cubeName of $schemaName schema dropped syccessfully.")
    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Cube " + cubeName + " of " + schemaName + " schema dropped syccessfully.");

  }

  def getTimestampFileAndType(schemaName : String, cubeName : String) = {
    
    var timestampFile =  if(useUniquePath)
    {
       metadataPath + "/" + schemaName + "/" + cubeName + "/schemas/" + MolapCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
  }
    else
    {
       metadataPath + "/schemas/" + MolapCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
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
      cubeModifiedTimeStore.put(schemaName + '_' + cubeName, FileFactory.getMolapFile(timestampFile, timestampFileType).getLastModifiedTime())
    }
    else
	  {
      cubeModifiedTimeStore.put("default", FileFactory.getMolapFile(timestampFile, timestampFileType).getLastModifiedTime())
	  }
	  
  }

  def touchSchemasTimestampFile(schemaName : String, cubeName : String) {
   val (timestampFile, timestampFileType) = getTimestampFileAndType(schemaName, cubeName)
	  FileFactory.getMolapFile(timestampFile, timestampFileType).setLastModifiedTime(System.currentTimeMillis())
  }

  def checkSchemasModifiedTimeAndReloadCubes() {
    if (useUniquePath) {
      metadata.cubesMeta.foreach(c => {
        val (timestampFile, timestampFileType) = getTimestampFileAndType(c.schemaName, c.cubeName)

        if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
          if (!(FileFactory.getMolapFile(timestampFile, timestampFileType).getLastModifiedTime() == cubeModifiedTimeStore.get(c.schemaName + "_" + c.cubeName))) {
		    refreshCache
		  }
	  }
      })
    } else {
      val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
      if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
        if (!(FileFactory.getMolapFile(timestampFile, timestampFileType).
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
      schemaLastUpdatedTime = FileFactory.getMolapFile(timestampFile, timestampFileType).getLastModifiedTime()
    }
    schemaLastUpdatedTime
  }

  private def loadCubeFromMetaData(
    fileType: com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType,
    buffer: scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.hive.CubeMeta],
    cubeFolder: com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile): Unit = {
        if (cubeFolder.isDirectory()) {

         val (schemaName,cubeName,dataPath,schema,partitioner,cubeCreationTime) = readCubeMetaDataFile(cubeFolder,fileType)

            val mondSchema = OlapMetastoreCatalog.parseStringToSchema(schema)
            val cubeUniqueName = schemaName + "_" + cubeName
            MolapMetadata.getInstance().loadSchema(mondSchema)
            val cube = MolapMetadata.getInstance().getCube(cubeUniqueName)
            buffer += CubeMeta(
                schemaName,
                cubeName,
                dataPath,
                mondSchema,
                cube,
                updatePartitioner(partitioner, cube),cubeCreationTime)
          }
        }

  def readCubeMetaDataFile(cubeFolder: com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile, fileType: com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType): (String, String, String, String, Partitioner, Long) = {
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


object OlapMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ShortType |
      "double" ^^^ DoubleType |
      "bigint" ^^^ LongType |
      "binary" ^^^ BinaryType |
      "boolean" ^^^ BooleanType |
      "decimal" ^^^ DecimalType() |
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
    //    case MapType(keyType, valueType) =>
    //      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
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

case class OlapMetaData(dims: Seq[String], msrs: Seq[String], cube: Cube)


//  }
//    
//  val measureAttr = { 
//      val filteredMeasureAttr = cubeMeta.schema.cubes(0).measures.filter { aMsr => aMsr.visible == null ||  aMsr.visible}
//      new LinkedHashSet(filteredMeasureAttr.toSeq).map(x => AttributeReference(
//      x.name,
//      OlapMetastoreTypes.toDataType("double"),
//      nullable = true
//    )(qualifiers = tableName +: alias.toSeq))
//  }
//    
//  override val output = (dimensionsAttr ++ measureAttr).toSeq
//  
//    // TODO: Use data from the footers.
//  override lazy val statistics = Statistics(sizeInBytes = sqlContext.conf.defaultSizeInBytes)
//  
//  override def equals(other: Any) = other match {
//    case p: OlapRelation =>
//      p.schemaName == schemaName && p.output == output && p.cubeName == cubeName
//    case _ => false
//  }
//  
////  /**
////   * Optionally resolves the given string to a
////   * [[catalyst.expressions.NamedExpression NamedExpression]]. The attribute is expressed as
////   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
////   */
////  override def resolve(name: String): Option[NamedExpression] = {
////    val parts = resolve(name)
////    parts
////  }
//
//}
//
//
//
