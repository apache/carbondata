/**
 *
 */
package com.huawei.datasight.spark.rdd

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.OlapContext
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.sql.hive.HiveContext
import com.huawei.datasight.molap.core.load.LoadMetadataDetails
import com.huawei.datasight.molap.load.DeleteLoadFolders
import com.huawei.datasight.molap.load.MolapLoadModel
import com.huawei.datasight.molap.load.MolapLoaderUtil
import com.huawei.datasight.molap.spark.util.LoadMetadataUtil
import com.huawei.datasight.spark.DeletedLoadResultImpl
import com.huawei.datasight.spark.KeyVal
import com.huawei.datasight.spark.KeyValImpl
import com.huawei.datasight.spark.MergeResultImpl
import com.huawei.datasight.spark.RestructureResultImpl
import com.huawei.datasight.spark.ResultImpl
import com.huawei.datasight.spark.processors.OlapUtil
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.unibi.molap.util.MolapSchemaParser
import com.huawei.unibi.molap.locks.MetadataLock
import com.huawei.unibi.molap.metadata.MolapMetadata
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.olap.MolapDef.Schema
import com.huawei.unibi.molap.util.MolapUtil
import org.apache.spark.sql.OlapRelation
import com.huawei.unibi.molap.util.MolapProperties
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import com.huawei.datasight.molap.core.load.LoadMetadataDetails
import com.huawei.datasight.spark.PartitionResultImpl
import com.huawei.unibi.molap.util.MolapDataProcessorUtil
import com.huawei.datasight.molap.merger.MolapDataMergerUtil
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory

/**
 * This is the factory class which can create different RDD depends on user needs.
  *
 * @author R00900208
 */
object MolapDataRDDFactory extends Logging {

 val LOGGER = LogServiceFactory.getLogService(MolapDataRDDFactory.getClass().getName());


  /**
   * It creates the RDD which can access the Hbase file system directly and reads the region files and executes the query on it.
   * It creates split for each region and reads the files. This RDD is best suitable if the result data is very big.
   */
  //  def newMolapDataDirectRDD(sc : SparkContext,molapQueryModel: MolapQueryPlan,
  //    conf: Configuration)  = {
  //
  //     //Need to handle this schema reading properly
  //	 MolapQueryUtil.createDataSource("G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml", null,null);
  //	 MolapProperties.getInstance().addProperty("molap.storelocation", "G:/imlib/store");
  //     val kv:KeyVal[MolapKey,MolapValue] = new KeyValImpl();
  //     val cube =  MolapMetadata.getInstance().getCubeWithCubeName(molapQueryModel.getCubeName());
  //     val model = MolapQueryUtil.createModel(molapQueryModel,null,cube)
  //     val big = new MolapDataRDD(sc,model,MolapSchemaParser.loadXML("G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml"),"G:/imlib/store",kv,conf,null,false)
  //     //Just for testing purpose i am printing the result.
  //     if(!model.isDetailQuery())
  //     {
  //    	 val s = big.map{key=> (key._1,key._2)}.reduceByKey((v1,v2)=>v1.merge(v2)).sortByKey(true).collect.foreach(println(_));
  //     }
  //     else
  //     {
  //    	 val s = big.map{key=> (key._1,key._2)}.sortByKey(true).collect.foreach(println(_));
  //     }
  //  }


  def intializeMolap(sc: SparkContext, schema: MolapDef.Schema, dataPath: String, cubeName: String, schemaName: String, partitioner: Partitioner, columinar: Boolean) {
    val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl();
    //     MolapQueryUtil.loadSchema(schema, null, null)
    //     MolapQueryUtil.loadSchema("G:/mavenlib/PCC_Java.xml", null, null)
    val catalog = CarbonEnv.getInstance(sc.asInstanceOf[SQLContext]).carbonCatalog
    val cubeCreationTime = catalog.getCubeCreationTime(schemaName, cubeName)
    new MolapDataCacheRDD(sc, kv, schema, catalog.metadataPath, cubeName, schemaName, partitioner, columinar, cubeCreationTime).collect
  }

  def loadMolapData(sc: SQLContext,
    molapLoadModel: MolapLoadModel,
    storeLocation: String,
    hdfsStoreLocation: String,
    kettleHomePath: String,
    partitioner: Partitioner,
    columinar: Boolean,
    isAgg: Boolean,
                    partitionStatus: String = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS) {
    val cube = MolapMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    var currentRestructNumber = -1
    //    val molapLock = new MetadataLock(cube.getMetaDataFilepath())
   try {

     LOGGER.audit("The data load request has been received.");

      //      if (molapLock.lock(MolapCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK, MolapCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK)) {
      //        logInfo("Successfully able to get the cube metadata file lock")
      //      }
      //      else
      //      {
      //        sys.error("Not able to acquire lock for data load.")
      //      }

    currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
      if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }

    //Check if any load need to be deleted before loading new data
      deleteLoadsAndUpdateMetadata(molapLoadModel, cube, partitioner, hdfsStoreLocation, false, currentRestructNumber)
      if (null == molapLoadModel.getLoadMetadataDetails) {
      readLoadMetadataDetails(molapLoadModel, hdfsStoreLocation)
    }

    var currentLoadCount = -1
      if (molapLoadModel.getLoadMetadataDetails().size() > 0) {
        for (eachLoadMetaData <- molapLoadModel.getLoadMetadataDetails()) {
          val loadCount = Integer.parseInt(eachLoadMetaData.getLoadName())
          if (currentLoadCount < loadCount)
            currentLoadCount = loadCount
        }
        currentLoadCount += 1;
        MolapLoaderUtil.deletePartialLoadDataIfExist(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getTableName, hdfsStoreLocation, currentRestructNumber, currentLoadCount)
      }
      else {
        currentLoadCount += 1;
      }

    // reading the start time of data load.
    val loadStartTime = MolapLoaderUtil.readCurrentTime();
    val cubeCreationTime = CarbonEnv.getInstance(sc).carbonCatalog.getCubeCreationTime(molapLoadModel.getSchemaName, molapLoadModel.getCubeName)
      val schemaLastUpdatedTime = CarbonEnv.getInstance(sc).carbonCatalog.getSchemaLastUpdatedTime(molapLoadModel.getSchemaName, molapLoadModel.getCubeName)
      val status = new MolapDataLoadRDD(sc.sparkContext, new ResultImpl(), molapLoadModel, storeLocation, hdfsStoreLocation, kettleHomePath, partitioner, columinar, currentRestructNumber, currentLoadCount, cubeCreationTime, schemaLastUpdatedTime).collect()
      val newStatusMap = scala.collection.mutable.Map.empty[String, String]
      status.foreach { eachLoadStatus =>
        val state = newStatusMap.get(eachLoadStatus._2.getPartitionCount)
        if (null == state || None == state || state == MolapCommonConstants.STORE_LOADSTATUS_FAILURE) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        }
        else if (state == MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
          eachLoadStatus._2.getLoadStatus == MolapCommonConstants.STORE_LOADSTATUS_SUCCESS) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        }
      }

    var loadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
      newStatusMap.foreach {
      case (key, value) =>
          if (value == MolapCommonConstants.STORE_LOADSTATUS_FAILURE) {
            loadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
        }
          else if (value == MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS && !loadStatus.equals(MolapCommonConstants.STORE_LOADSTATUS_FAILURE)) {
          loadStatus = MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
        }
      }

      if (loadStatus != MolapCommonConstants.STORE_LOADSTATUS_FAILURE &&
        partitionStatus == MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
      loadStatus = partitionStatus
    }

      //      status.foreach {eachLoadStatus =>
      //        val state = eachLoadStatus._2.getLoadStatus
      //        if (state == MolapCommonConstants.STORE_LOADSTATUS_FAILURE)
      //        {
      //          loadStatus=MolapCommonConstants.STORE_LOADSTATUS_FAILURE
      //        }
      //        else if(state == MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS && !loadStatus.equals(MolapCommonConstants.STORE_LOADSTATUS_FAILURE))
      //        {
      //          loadStatus = MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
      //        }
      //      }

      //      if (status.forall(_._2.getLoadStatus == MolapCommonConstants.STORE_LOADSTATUS_SUCCESS)) {
      //        loadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
      //      }
      //      else
      //        if(status.forall(_._2.getLoadStatus == MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS))
      //        {
      //          loadStatus = MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
      //        }
      if (loadStatus == MolapCommonConstants.STORE_LOADSTATUS_FAILURE) {
        var message: String = ""
        logInfo("********starting clean up**********")
        if (isAgg) {
          MolapLoaderUtil.deleteTable(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getAggTableName, hdfsStoreLocation, currentRestructNumber)
          message = "Aggregate table creation failure"
        }
        else {
          val (result, metadataDetails) = status(0)
          val newSlice = MolapCommonConstants.LOAD_FOLDER + result
          MolapLoaderUtil.deleteSlice(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getTableName, hdfsStoreLocation, currentRestructNumber, newSlice)
          val schema = molapLoadModel.getSchema
          val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
          if (null != aggTables && !aggTables.isEmpty) {
            aggTables.foreach { aggTable =>
              val aggTableName = MolapLoaderUtil.getAggregateTableName(aggTable)
              MolapLoaderUtil.deleteSlice(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, aggTableName, hdfsStoreLocation, currentRestructNumber, newSlice)
            }
          }
          message = "Dataload failure"
        }
        logInfo("********clean up done**********")
        LOGGER.audit("The data loading is failed.")
        logWarning("Unable to write load metadata file")
        throw new Exception(message)
      }
      else {
       val (result, metadataDetails) = status(0)
        if (!isAgg) {
          MolapLoaderUtil.recordLoadMetadata(result, metadataDetails, molapLoadModel, loadStatus, loadStartTime)
        }
        else if (!molapLoadModel.isRetentionRequest()) {
          try {
              CarbonEnv.getInstance(sc).carbonCatalog.updateCube(molapLoadModel.getSchema, false)(sc)
          }
          catch {
          case e: Exception =>
            MolapLoaderUtil.deleteTable(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getAggTableName, hdfsStoreLocation, currentRestructNumber)
           val message = "Aggregation creation failure"
            throw new Exception(message)
          }


          logInfo("********schema updated**********")
        }
       LOGGER.audit("The data loading is successfull.");
      if(MolapDataMergerUtil.checkIfLoadMergingRequired(cube.getMetaDataFilepath(),molapLoadModel, hdfsStoreLocation,  partitioner.partitionCount, currentRestructNumber))
      {

          val loadsToMerge = MolapDataMergerUtil.getLoadsToMergeFromHDFS(
            hdfsStoreLocation, FileFactory.getFileType(hdfsStoreLocation), cube.getMetaDataFilepath(), molapLoadModel, currentRestructNumber, partitioner.partitionCount);
          
          if(loadsToMerge.length == 2)
          {
          
              var MergedLoadName = MolapDataMergerUtil.getMergedLoadName(loadsToMerge)
          
              var finalMergeStatus = true

              val mergeStatus = new MolapMergerRDD(sc.sparkContext, new MergeResultImpl(), molapLoadModel, storeLocation, hdfsStoreLocation, partitioner, currentRestructNumber, cube.getMetaDataFilepath(), loadsToMerge, MergedLoadName,kettleHomePath,cubeCreationTime).collect
          
          
              mergeStatus.foreach {eachMergeStatus =>
              val state = eachMergeStatus._2
              if (state == false)
                  {
                      finalMergeStatus=false
      }
              }
          
              if(finalMergeStatus == true)
              {
                  MolapDataMergerUtil.updateLoadMetadataWithMergeStatus(loadsToMerge,cube.getMetaDataFilepath(),MergedLoadName,molapLoadModel)
              }
          
          }
      }
      }

    }
    //    finally{
    //      if (molapLock != null) {
    //          if (molapLock.unlock()) {
    //            logInfo("unlock the cube metadata file successfully")
    //          } else {
    //            logError("Unable to unlock the metadata lock")
    //          }
    //        }
    //    }


    if (!isAgg) {
      val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl();
      new MolapGlobalSequenceGeneratorRDD(sc.sparkContext, kv, molapLoadModel, storeLocation, hdfsStoreLocation, partitioner, currentRestructNumber).collect
    }
  }

  def readLoadMetadataDetails(model: MolapLoadModel, hdfsStoreLocation: String) = {
    val metadataPath = MolapLoaderUtil.getMetaDataFilePath(model.getSchemaName, model.getCubeName, hdfsStoreLocation)
    val details = MolapUtil.readLoadMetadata(metadataPath);
    model.setLoadMetadataDetails(details.toList)
  }

  def partitionMolapData(sc: SparkContext,
                         schemaName: String,
                         cubeName: String,
                         sourcePath: String,
                         targetFolder: String,
      requiredColumns: Array[String],
      headers: String,
      delimiter: String,
      quoteChar: String,
      escapeChar: String,
      multiLine: Boolean,
      partitioner: Partitioner): String = {
    //     val kv:KeyVal[MolapKey,MolapValue] = new KeyValImpl();

    val status = new MolapDataPartitionRDD(sc, new PartitionResultImpl(), schemaName, cubeName, sourcePath, targetFolder, requiredColumns, headers, delimiter, quoteChar, escapeChar, multiLine, partitioner).collect
    MolapDataProcessorUtil.renameBadRecordsFromInProgressToNormal("partition/" + schemaName + '/' + cubeName);
     var loadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
    status.foreach {
      case (key, value) =>
        if (value == true) {
          loadStatus = MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
        }
      }
     loadStatus
  }

  def mergeMolapData(sc: SQLContext,
                     molapLoadModel: MolapLoadModel,
                     storeLocation: String,
                     hdfsStoreLocation: String,
                     partitioner: Partitioner) {
    val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl();
      val cube = MolapMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    val metaDataPath: String = cube.getMetaDataFilepath()
      var currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
 //    new MolapMergerRDD(sc.sparkContext,kv,molapLoadModel,storeLocation,hdfsStoreLocation, partitioner, currentRestructNumber).collect
     
  }

    def deleteLoadByDate(
      sqlContext: SQLContext,
    schema: Schema,
    schemaName: String,
    cubeName: String,
                        tableName: String,
    hdfsStoreLocation: String,
      dateField: String,
                        dateFieldActualName: String,
      dateValue: String,
      partitioner: Partitioner) {

      val sc = sqlContext;
    //Delete the records based on data
    var cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName);
    if (null == cube) {
      MolapMetadata.getInstance().loadSchema(schema);
      cube = MolapMetadata.getInstance().getCube(schemaName + "_" + cubeName);
    }

    var currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    val loadMetadataDetailsArray = MolapUtil.readLoadMetadata(cube.getMetaDataFilepath()).toList
    val resultMap = new MolapDeleteLoadByDateRDD(
      sc.sparkContext,
      new DeletedLoadResultImpl(),
      schemaName,
      cube.getOnlyCubeName(),
        dateField,
        dateFieldActualName,
      dateValue,
        partitioner,
        cube.getFactTableName,
        tableName,
        hdfsStoreLocation,
        loadMetadataDetailsArray,
        currentRestructNumber).collect.groupBy(_._1).toMap

    //  var updatedLoadMetadataDetailsList = Seq[LoadMetadataDetails]() 
      var updatedLoadMetadataDetailsList = new ListBuffer[LoadMetadataDetails]()
    //get list of updated or deleted load status and update load meta data file     
    if (!resultMap.isEmpty) {

      if (resultMap.size == 1) {

        if (resultMap.contains("")) {
           logError("Delete by Date request is failed")
           sys.error("Delete by Date request is failed, potential causes " + "Empty store or Invalid column type, For more details please refer logs.")

       }
      }

      val updatedloadMetadataDetails = loadMetadataDetailsArray.map { elem => {
          var statusList = resultMap.get(elem.getLoadName())
          // check for the merged load folder.
          if(statusList == None && null != elem.getMergedLoadName()) 
          {
            statusList = resultMap.get(elem.getMergedLoadName())
          }
          
          if (statusList != None) {
            elem.setDeletionTimestamp(MolapLoaderUtil.readCurrentTime())
            //if atleast on MolapCommonConstants.MARKED_FOR_UPDATE status exist, use MARKED_FOR_UPDATE
            if (statusList.get.forall(status => status._2 == MolapCommonConstants.MARKED_FOR_DELETE)) {
              elem.setLoadStatus(MolapCommonConstants.MARKED_FOR_DELETE)
            } else {
              elem.setLoadStatus(MolapCommonConstants.MARKED_FOR_UPDATE)
            updatedLoadMetadataDetailsList += elem
            }
            elem
          } else {
            elem
          }
        }

      }

      //Save the load metadata
      var molapLock = new MetadataLock(cube.getMetaDataFilepath())
      try {
        //        if (molapLock.lock(
        //          MolapCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK,
        //          MolapCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK)) {
        if (molapLock.lockWithRetries()) {
          logInfo("Successfully got the cube metadata file lock")
          if (!updatedLoadMetadataDetailsList.isEmpty) {
            LoadAggregateTabAfterRetention(schemaName, cube.getOnlyCubeName(), cube.getFactTableName, sqlContext, schema, updatedLoadMetadataDetailsList)
          }

          //write
          MolapLoaderUtil.writeLoadMetadata(
            schema,
            schemaName,
            cube.getOnlyCubeName(),
            updatedloadMetadataDetails)
        }
      } finally {
        if (molapLock.unlock()) {
          logInfo("unlock the cube metadata file successfully")
        } else {
          logError("Unable to unlock the metadata lock")
        }
      }
    }
    else {
       logError("Delete by Date request is failed")
       LOGGER.audit("The delete load by date is failed.");
       sys.error("Delete by Date request is failed, potential causes " + "Empty store or Invalid column type, For more details please refer logs.")
    }


  }

  def alterCube(
                 hiveContext: HiveContext,
    sc: SparkContext,
    origUnModifiedSchema: MolapDef.Schema,
    schema: MolapDef.Schema,
    schemaName: String,
    cubeName: String,
    hdfsStoreLocation: String,
    addedDimensions: Seq[MolapDef.CubeDimension],
    addedMeasures: Seq[MolapDef.Measure],
    validDropDimList: ArrayBuffer[String],
    validDropMsrList: ArrayBuffer[String],
    curTime: Long,
    defaultVals: Map[String, String],
                 partitioner: Partitioner): Boolean = {

      val cube = MolapMetadata.getInstance().getCubeWithCubeName(cubeName, schemaName);

      val metaDataPath: String = cube.getMetaDataFilepath()
    //      val molapLock = new MetadataLock(metaDataPath)
    //      try {
    ////        if (molapLock.lock(MolapCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK, MolapCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK)) {
    //        if (molapLock.lockWithRetries()) {
    //          logInfo("Successfully able to get the cube metadata file lock")
    //        } else {
    //          sys.error("Not able to acquire lock for altering cube.")
    //        }

        //if there is no data loading done, no need to create RS folders
        val loadMetadataDetailsArray = MolapUtil.readLoadMetadata(metaDataPath).toList
        if (0 == loadMetadataDetailsArray.size) {
          CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
          return true
        }

        var currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
        if (-1 == currentRestructNumber) {
          currentRestructNumber = 0
        }

        val loadStartTime = MolapLoaderUtil.readCurrentTime();

        val resultMap = new MolapAlterCubeRDD(sc,
          origUnModifiedSchema,
          schema,
          schemaName,
          cubeName,
          hdfsStoreLocation,
          addedDimensions,
          addedMeasures,
          validDropDimList,
          validDropMsrList,
          curTime,
          defaultVals,
          currentRestructNumber,
      metaDataPath,
          partitioner,
          new RestructureResultImpl()).collect

        var restructureStatus: Boolean = resultMap.forall(_._2)

        if (restructureStatus) {
          if (addedDimensions.length > 0 || addedMeasures.length > 0) {
            val molapLoadModel: MolapLoadModel = new MolapLoadModel()
            molapLoadModel.setCubeName(cubeName)
            molapLoadModel.setSchemaName(schemaName)
            molapLoadModel.setSchema(schema);
            val metadataDetails: LoadMetadataDetails = new LoadMetadataDetails()
            MolapLoaderUtil.recordLoadMetadata(resultMap(0)._1, metadataDetails, molapLoadModel, MolapCommonConstants.STORE_LOADSTATUS_SUCCESS, loadStartTime)
            restructureStatus = MolapUtil.createRSMetaFile(metaDataPath, "RS_" + (currentRestructNumber + 1))
          }
          if (restructureStatus) {
            CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
          }
        }

        restructureStatus
    //      } finally {
    //        if (molapLock != null) {
    //          if (molapLock.unlock()) {
    //            logInfo("unlock the cube metadata file successfully")
    //          } else {
    //            logError("Unable to unlock the metadata lock")
    //          }
    //        }
    //      }
    }

   def dropAggregateTable(
      sc: SparkContext,
                          schema: String,
      cube: String,
      partitioner: Partitioner) {
    val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl()
     new MolapDropAggregateTableRDD(sc, kv, schema, cube, partitioner).collect
  }

  def dropCube(
      sc: SparkContext,
                schema: String,
      cube: String,
      partitioner: Partitioner) {
    val kv: KeyVal[MolapKey, MolapValue] = new KeyValImpl()
     new MolapDropCubeRDD(sc, kv, schema, cube, partitioner).collect
  }

  def cleanFiles(
      sc: SparkContext,
                  molapLoadModel: MolapLoadModel,
                  hdfsStoreLocation: String,
                  partitioner: Partitioner) {

    val cube = MolapMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    val metaDataPath: String = cube.getMetaDataFilepath()
    var currentRestructNumber = MolapUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    var molapLock = new MetadataLock(cube.getMetaDataFilepath())
    try {
      //      if (molapLock.lock(
      //        MolapCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK,
      //        MolapCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK)) {
      if (molapLock.lockWithRetries()) {
        deleteLoadsAndUpdateMetadata(molapLoadModel, cube, partitioner, hdfsStoreLocation, true, currentRestructNumber)
      }
    }
    finally {
        if (molapLock.unlock()) {
          logInfo("unlock the cube metadata file successfully")
        } else {
          logError("Unable to unlock the metadata lock")
        }
      }
  }

  //    /**
  //   * It creates the RDD which can access the Hbase file system directly and reads the region files and executes the query on it.
  //   * It creates split for each region and reads the files. This RDD is best suitable if the result data is very big.
  //   */
  //  def newMolapDataDirectSqlRDD(sc : SparkContext,sql: String,
  //    conf: Configuration,schemaPath: String) : Array[(MolapKey,MolapValue)] = {
  //
  //
  //     //Need to handle this schema reading properly
  //     val parser = new SimpleQueryParser()
  //	 val tableName = parser.getTableName(sql)
  ////	 val cube = SparkDataLoadUtil.loadSchema("/opt/ravi/DETAIL_UFDR_Streaming.xml", "DETAIL_UFDR_Streaming", "DETAIL_UFDR_Streaming")
  //     val cube = MolapQueryUtil.loadSchema(schemaPath,null,tableName);
  //	 val plan = parser.parseQuery(sql, tableName, cube);
  //	 val model = MolapQueryUtil.createModel(plan,cube)
  //     val kv:KeyVal[MolapKey,MolapValue] = new KeyValImpl();
  //
  //     val big = new MolapDataRDD(sc,model,kv,conf,null)
  //     var s:Array[(MolapKey,MolapValue)] = null;
  //     //Just for testing purpose i am printing the result.
  //     if(!model.isDetailQuery())
  //     {
  //       if(model.getMsrFilterDimension() != null)
  //       {
  ////         s = new SparkMeasureFilterProcessor().process(big, model.getMsrFilterModels().get(0).getFilterModels(), 1,model.getMsrs()).collect
  //       }
  //
  //       if(model.getTopNModel() != null)
  //       {
  ////          val s = big.map{key=> (key._1,key._2)}.reduceByKey((v1,v2)=>v1.merge(v2)).map({key=> (key._2,key._1)}).top(model.getTopNModel().getCount()).map(key=> (key._2,key._1)).foreach(println(_));
  ////          s = new SparkTopNProcessor().process(big, model.getTopNModel())
  //       }
  //       else
  //       {
  //    	   s = big.map{key=> (key._1,key._2)}.reduceByKey((v1,v2)=>v1.merge(v2)).sortByKey(true).collect;
  //       }
  //     }
  //     else
  //     {
  //       if(model.getTopNModel() != null)
  //       {
  //    	 s = big.map{key=> (key._1,key._2)}.map({key=> (key._2,key._1)}).top(model.getTopNModel().getCount()).map(key=> (key._2,key._1));
  //       }
  //       else
  //       {
  //         s = big.map{key=> (key._1,key._2)}.sortByKey(true).collect;
  //       }
  //     }
  //     return s;
  ////      val s = big.map{key=> (key._1,key._2)}.reduceByKey((v1,v2)=>v1.merge(v2)).map({key=> (key._2,key._1)}).top(5).map(key=> (key._1,key._2)).foreach(println(_))
  //  }


  def main(args: Array[String]) {

	    val d = SparkContext.jarOfClass(this.getClass)
	    val ar = new Array[String](d.size)
	    var i = 0
    d.foreach {
      p => ar(i) = p;
        i = i + 1
    }

    //	    val sc = new SparkContext("spark://master:7077", "Big Data Direct App", "/opt/spark-1.0.0-rc3/",ar)


	        val confs = new SparkConf()
      //      .setMaster("spark://master:7077")
      .setMaster("local")
      .setJars(ar)
      .setAppName("Molap Spark Query")
      .setSparkHome("/opt/spark-1.0.0-rc3/")
      .set("spark.scheduler.mode", "FAIR")
      val sc = new SparkContext(confs)
    //	      val sc = new SparkContext("local", "Big Data App", "G:/spark-1.0.0-rc3",ar)

	    val conf = new Configuration();

    //        val schemaPath = "/opt/ravi/PCC_Java.xml"
    //        val schemaPath = "G:/mavenlib/PCC_Java.xml"
        val schemaPath = "G:\\bibin issues\\SmokeData\\schema\\steelwheels.molap.xml"
    val olapContext = new OlapContext(sc, schemaPath)
    //        val dataPath = "hdfs://master:54310/opt/ravi/store"
        val dataPath = "F:/TRPSVN/store"

        Thread.sleep(5000)

    //        intializeMolap(sc, schemaPath,dataPath,"ODM","PCC")
        val schema = MolapSchemaParser.loadXML(schemaPath)
    intializeMolap(sc, schema, dataPath, "SteelWheelsSales", "MOLAPSteelWheels", null, false)
	    import olapContext._

    val holder = OlapUtil.createBaseRDD(olapContext, MolapMetadata.getInstance().getCubeWithCubeName("SteelWheelsSales", "MOLAPSteelWheels"))
	//    var dd = holder.rdd.asInstanceOf[DataFrame].select('Territory,'Country,'City,'Quantity).groupBy('Territory,'Country)('Territory,'Country,'City,SumMolap('Quantity).as('Q1))
    //
    //	    dd = dd.topN(3, 'Country, 'Q1)

    //	    val dd1 = dd.as('j1)

    //        val dd2 = dd1.groupBy('Territory,'Country)('Territory,'Country,SumMolap('Q1)).as('j2)

    //	    val dd3 = dd1.join(dd2,Inner,Some("j1.Territory".attr === "j2.Territory".attr))//.where("j1.Country".attr === "j2.Country".attr)
    //	    val schemaPath = "/opt/ravi/steelwheels.molap.xml"
    //	    val dataPath = "G:/imlib/store"


    //	    val rdd = olapContext.hSql(sql)

    //	    dd.collect foreach(println(_))
    //        dd2.collect foreach(println(_))
    //	    newMolapDataDirectSqlRDD(sc, sql, conf,schemaPath).foreach(println(_))


    }

  def deleteLoadsAndUpdateMetadata(molapLoadModel: MolapLoadModel, cube: Cube, partitioner: Partitioner,
                                   hdfsStoreLocation: String, isForceDeletion: Boolean, currentRestructNumber: Integer) {
    if (LoadMetadataUtil.isLoadDeletionRequired(molapLoadModel)) {

      val loadMetadataFilePath = MolapLoaderUtil
                .extractLoadMetadataFileLocation(molapLoadModel);

      val details = MolapUtil
                .readLoadMetadata(loadMetadataFilePath);

      //Delete marked loads
      val isUpdationRequired = DeleteLoadFolders.deleteLoadFoldersFromFileSystem(molapLoadModel, partitioner.partitionCount, hdfsStoreLocation, isForceDeletion, currentRestructNumber, details)

      if (isUpdationRequired == true) {
        //Update load metadate file after cleaning deleted nodes
        MolapLoaderUtil.writeLoadMetadata(
          molapLoadModel.getSchema,
          molapLoadModel.getSchemaName,
          molapLoadModel.getCubeName, details.toList)
      }

    }

    MolapDataMergerUtil.cleanUnwantedMergeLoadFolder(molapLoadModel, partitioner.partitionCount, hdfsStoreLocation, isForceDeletion, currentRestructNumber)
    
  }

  def LoadAggregateTabAfterRetention(schemaName: String, cubeName: String, factTableName: String, sqlContext: SQLContext, schema: Schema, list: ListBuffer[LoadMetadataDetails]) {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[OlapRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val molapLoadModel = new MolapLoadModel()
    molapLoadModel.setCubeName(cubeName)
    molapLoadModel.setSchemaName(schemaName)
    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[MolapDef.Table]
    //    if (table.getName == factTableName) {
      val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
      if (null != aggTables && !aggTables.isEmpty) {
        molapLoadModel.setRetentionRequest(true)
        molapLoadModel.setLoadMetadataDetails(list)
        molapLoadModel.setTableName(table.name)
        molapLoadModel.setSchema(relation.cubeMeta.schema);
      //        molapLoadModel.setAggLoadRequest(true)
      var storeLocation = MolapProperties.getInstance.getProperty(MolapCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
        storeLocation = storeLocation + "/molapstore/" + System.currentTimeMillis()
        val columinar = sqlContext.getConf("molap.is.columnar.storage", "true").toBoolean
        var kettleHomePath = sqlContext.getConf("molap.kettle.home", null)
        if (null == kettleHomePath) {
          kettleHomePath = MolapProperties.getInstance.getProperty("molap.kettle.home");
        }
        if (kettleHomePath == null) sys.error(s"molap.kettle.home is not set")
        //    val aggTableName = aggTableNames.get(0)

	        MolapDataRDDFactory.loadMolapData(
	          sqlContext,
	          molapLoadModel,
	          storeLocation,
	          relation.cubeMeta.dataPath,
	          kettleHomePath,
	          relation.cubeMeta.partitioner, columinar, true);

      }

    //    }

  }

}