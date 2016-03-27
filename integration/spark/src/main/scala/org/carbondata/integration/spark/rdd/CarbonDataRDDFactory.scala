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

/**
 *
 */
package org.carbondata.integration.spark.rdd

import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{CarbonEnv, CarbonRelation, SQLContext}
import org.apache.spark.{Logging, SparkContext}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.locks.MetadataLock
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.metadata.CarbonMetadata.Cube
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.carbon.CarbonDef.Schema
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark._
import org.carbondata.integration.spark.load.{DeleteLoadFolders, CarbonLoadModel, CarbonLoaderUtil}
import org.carbondata.integration.spark.merger.CarbonDataMergerUtil
import org.carbondata.integration.spark.util.LoadMetadataUtil
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}

import scala.collection.JavaConversions.{asScalaBuffer, seqAsJavaList}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 * @author R00900208
 */
object CarbonDataRDDFactory extends Logging {

  val logger = LogServiceFactory.getLogService(CarbonDataRDDFactory.getClass().getName());

  def intializeMolap(sc: SparkContext, schema: CarbonDef.Schema, dataPath: String, cubeName: String, schemaName: String, partitioner: Partitioner, columinar: Boolean) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl();
    //     CarbonQueryUtil.loadSchema(schema, null, null)
    //     CarbonQueryUtil.loadSchema("G:/mavenlib/PCC_Java.xml", null, null)
    val catalog = CarbonEnv.getInstance(sc.asInstanceOf[SQLContext]).carbonCatalog
    val cubeCreationTime = catalog.getCubeCreationTime(schemaName, cubeName)
    new CarbonDataCacheRDD(sc, kv, schema, catalog.metadataPath, cubeName, schemaName, partitioner, columinar, cubeCreationTime).collect
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

    val status = new CarbonDataPartitionRDD(sc, new PartitionResultImpl(), schemaName, cubeName, sourcePath, targetFolder, requiredColumns, headers, delimiter, quoteChar, escapeChar, multiLine, partitioner).collect
    CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal("partition/" + schemaName + '/' + cubeName);
    var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    status.foreach {
      case (key, value) =>
        if (value == true) {
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
        }
    }
    loadStatus
  }

  def mergeMolapData(
                      sc: SQLContext,
                      molapLoadModel: CarbonLoadModel,
                      storeLocation: String,
                      hdfsStoreLocation: String,
                      partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl();
    val cube = CarbonMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    val metaDataPath: String = cube.getMetaDataFilepath()
    var currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
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
    var cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName);
    if (null == cube) {
      CarbonMetadata.getInstance().loadSchema(schema);
      cube = CarbonMetadata.getInstance().getCube(schemaName + "_" + cubeName);
    }

    var currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    val loadMetadataDetailsArray = CarbonUtil.readLoadMetadata(cube.getMetaDataFilepath()).toList
    val resultMap = new CarbonDeleteLoadByDateRDD(
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

    var updatedLoadMetadataDetailsList = new ListBuffer[LoadMetadataDetails]()
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
        if (statusList == None && null != elem.getMergedLoadName()) {
          statusList = resultMap.get(elem.getMergedLoadName())
        }

        if (statusList != None) {
          elem.setDeletionTimestamp(CarbonLoaderUtil.readCurrentTime())
          //if atleast on MolapCommonConstants.MARKED_FOR_UPDATE status exist, use MARKED_FOR_UPDATE
          if (statusList.get.forall(status => status._2 == CarbonCommonConstants.MARKED_FOR_DELETE)) {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
          } else {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE)
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
        if (molapLock.lockWithRetries()) {
          logInfo("Successfully got the cube metadata file lock")
          if (!updatedLoadMetadataDetailsList.isEmpty) {
            LoadAggregateTabAfterRetention(schemaName, cube.getOnlyCubeName(), cube.getFactTableName, sqlContext, schema, updatedLoadMetadataDetailsList)
          }

          //write
          CarbonLoaderUtil.writeLoadMetadata(
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
    } else {
      logError("Delete by Date request is failed")
      logger.audit("The delete load by date is failed.");
      sys.error("Delete by Date request is failed, potential causes " + "Empty store or Invalid column type, For more details please refer logs.")
    }
  }

  def LoadAggregateTabAfterRetention(
                                      schemaName: String,
                                      cubeName: String,
                                      factTableName: String,
                                      sqlContext: SQLContext,
                                      schema: Schema,
                                      list: ListBuffer[LoadMetadataDetails]) {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val molapLoadModel = new CarbonLoadModel()
    molapLoadModel.setCubeName(cubeName)
    molapLoadModel.setSchemaName(schemaName)
    val table = relation.cubeMeta.schema.cubes(0).fact.asInstanceOf[CarbonDef.Table]
    val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
    if (null != aggTables && !aggTables.isEmpty) {
      molapLoadModel.setRetentionRequest(true)
      molapLoadModel.setLoadMetadataDetails(list)
      molapLoadModel.setTableName(table.name)
      molapLoadModel.setSchema(relation.cubeMeta.schema);
      var storeLocation = CarbonProperties.getInstance.getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))
      storeLocation = storeLocation + "/molapstore/" + System.currentTimeMillis()
      val columinar = sqlContext.getConf("molap.is.columnar.storage", "true").toBoolean
      var kettleHomePath = sqlContext.getConf("molap.kettle.home", null)
      if (null == kettleHomePath) {
        kettleHomePath = CarbonProperties.getInstance.getProperty("molap.kettle.home");
      }
      if (kettleHomePath == null) sys.error(s"molap.kettle.home is not set")
      CarbonDataRDDFactory.loadMolapData(
        sqlContext,
        molapLoadModel,
        storeLocation,
        relation.cubeMeta.dataPath,
        kettleHomePath,
        relation.cubeMeta.partitioner, columinar, true)
    }
  }

  def loadMolapData(sc: SQLContext,
                    molapLoadModel: CarbonLoadModel,
                    storeLocation: String,
                    hdfsStoreLocation: String,
                    kettleHomePath: String,
                    partitioner: Partitioner,
                    columinar: Boolean,
                    isAgg: Boolean,
                    partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS) {
    val cube = CarbonMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    var currentRestructNumber = -1
    try {
      logger.audit("The data load request has been received.");

      currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
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
        CarbonLoaderUtil.deletePartialLoadDataIfExist(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getTableName, hdfsStoreLocation, currentRestructNumber, currentLoadCount)
      } else {
        currentLoadCount += 1;
      }

      // reading the start time of data load.
      val loadStartTime = CarbonLoaderUtil.readCurrentTime();
      val cubeCreationTime = CarbonEnv.getInstance(sc).carbonCatalog.getCubeCreationTime(molapLoadModel.getSchemaName, molapLoadModel.getCubeName)
      val schemaLastUpdatedTime = CarbonEnv.getInstance(sc).carbonCatalog.getSchemaLastUpdatedTime(molapLoadModel.getSchemaName, molapLoadModel.getCubeName)
      val status = new CarbonDataLoadRDD(sc.sparkContext, new ResultImpl(), molapLoadModel, storeLocation, hdfsStoreLocation, kettleHomePath, partitioner, columinar, currentRestructNumber, currentLoadCount, cubeCreationTime, schemaLastUpdatedTime).collect()
      val newStatusMap = scala.collection.mutable.Map.empty[String, String]
      status.foreach { eachLoadStatus =>
        val state = newStatusMap.get(eachLoadStatus._2.getPartitionCount)
        if (null == state || None == state || state == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        } else if (state == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
          eachLoadStatus._2.getLoadStatus == CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        }
      }

      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      newStatusMap.foreach {
        case (key, value) =>
          if (value == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } else if (value == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS && !loadStatus.equals(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
          }
      }

      if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
        partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
        loadStatus = partitionStatus
      }

      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        var message: String = ""
        logInfo("********starting clean up**********")
        if (isAgg) {
          CarbonLoaderUtil.deleteTable(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getAggTableName, hdfsStoreLocation, currentRestructNumber)
          message = "Aggregate table creation failure"
        } else {
          val (result, metadataDetails) = status(0)
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + result
          CarbonLoaderUtil.deleteSlice(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getTableName, hdfsStoreLocation, currentRestructNumber, newSlice)
          val schema = molapLoadModel.getSchema
          val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
          if (null != aggTables && !aggTables.isEmpty) {
            aggTables.foreach { aggTable =>
              val aggTableName = CarbonLoaderUtil.getAggregateTableName(aggTable)
              CarbonLoaderUtil.deleteSlice(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, aggTableName, hdfsStoreLocation, currentRestructNumber, newSlice)
            }
          }
          message = "Dataload failure"
        }
        logInfo("********clean up done**********")
        logger.audit("The data loading is failed.")
        logWarning("Unable to write load metadata file")
        throw new Exception(message)
      } else {
        val (result, metadataDetails) = status(0)
        if (!isAgg) {
          CarbonLoaderUtil.recordLoadMetadata(result, metadataDetails, molapLoadModel, loadStatus, loadStartTime)
        } else if (!molapLoadModel.isRetentionRequest()) {
          try {
            CarbonEnv.getInstance(sc).carbonCatalog.updateCube(molapLoadModel.getSchema, false)(sc)
          } catch {
            case e: Exception =>
              CarbonLoaderUtil.deleteTable(partitioner.partitionCount, molapLoadModel.getSchemaName, molapLoadModel.getCubeName, molapLoadModel.getAggTableName, hdfsStoreLocation, currentRestructNumber)
              val message = "Aggregation creation failure"
              throw new Exception(message)
          }

          logInfo("********schema updated**********")
        }
        logger.audit("The data loading is successful.")
        if (CarbonDataMergerUtil.checkIfLoadMergingRequired(cube.getMetaDataFilepath(), molapLoadModel, hdfsStoreLocation, partitioner.partitionCount, currentRestructNumber)) {

          val loadsToMerge = CarbonDataMergerUtil.getLoadsToMergeFromHDFS(
            hdfsStoreLocation, FileFactory.getFileType(hdfsStoreLocation),
            cube.getMetaDataFilepath(), molapLoadModel, currentRestructNumber,
            partitioner.partitionCount);

          if (loadsToMerge.length == 2) {
            val MergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
            var finalMergeStatus = true
            val mergeStatus = new CarbonMergerRDD(
              sc.sparkContext,
              new MergeResultImpl(),
              molapLoadModel,
              storeLocation,
              hdfsStoreLocation,
              partitioner,
              currentRestructNumber,
              cube.getMetaDataFilepath(),
              loadsToMerge,
              MergedLoadName,
              kettleHomePath,
              cubeCreationTime).collect

            mergeStatus.foreach { eachMergeStatus =>
              val state = eachMergeStatus._2
              if (!state) {
                finalMergeStatus = false
              }
            }
            if (finalMergeStatus) {
              CarbonDataMergerUtil.updateLoadMetadataWithMergeStatus(loadsToMerge, cube.getMetaDataFilepath(), MergedLoadName, molapLoadModel)
            }
          }
        }
      }
    }

    if (!isAgg) {
      val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl();
      new CarbonGlobalSequenceGeneratorRDD(sc.sparkContext, kv, molapLoadModel, storeLocation, hdfsStoreLocation, partitioner, currentRestructNumber).collect
    }
  }

  def readLoadMetadataDetails(model: CarbonLoadModel, hdfsStoreLocation: String) = {
    val metadataPath = CarbonLoaderUtil.getMetaDataFilePath(model.getSchemaName, model.getCubeName, hdfsStoreLocation)
    val details = CarbonUtil.readLoadMetadata(metadataPath);
    model.setLoadMetadataDetails(details.toList)
  }

  def deleteLoadsAndUpdateMetadata(
                                    molapLoadModel: CarbonLoadModel,
                                    cube: Cube, partitioner: Partitioner,
                                    hdfsStoreLocation: String,
                                    isForceDeletion: Boolean,
                                    currentRestructNumber: Integer) {
    if (LoadMetadataUtil.isLoadDeletionRequired(molapLoadModel)) {
      val loadMetadataFilePath = CarbonLoaderUtil
        .extractLoadMetadataFileLocation(molapLoadModel);
      val details = CarbonUtil
        .readLoadMetadata(loadMetadataFilePath);

      //Delete marked loads
      val isUpdationRequired = DeleteLoadFolders.deleteLoadFoldersFromFileSystem(molapLoadModel, partitioner.partitionCount, hdfsStoreLocation, isForceDeletion, currentRestructNumber, details)

      if (isUpdationRequired == true) {
        //Update load metadate file after cleaning deleted nodes
        CarbonLoaderUtil.writeLoadMetadata(
          molapLoadModel.getSchema,
          molapLoadModel.getSchemaName,
          molapLoadModel.getCubeName, details.toList)
      }
    }

    CarbonDataMergerUtil.cleanUnwantedMergeLoadFolder(molapLoadModel, partitioner.partitionCount, hdfsStoreLocation, isForceDeletion, currentRestructNumber)

  }

  def alterCube(
                 hiveContext: HiveContext,
                 sc: SparkContext,
                 origUnModifiedSchema: CarbonDef.Schema,
                 schema: CarbonDef.Schema,
                 schemaName: String,
                 cubeName: String,
                 hdfsStoreLocation: String,
                 addedDimensions: Seq[CarbonDef.CubeDimension],
                 addedMeasures: Seq[CarbonDef.Measure],
                 validDropDimList: ArrayBuffer[String],
                 validDropMsrList: ArrayBuffer[String],
                 curTime: Long,
                 defaultVals: Map[String, String],
                 partitioner: Partitioner): Boolean = {

    val cube = CarbonMetadata.getInstance().getCubeWithCubeName(cubeName, schemaName);

    val metaDataPath: String = cube.getMetaDataFilepath()

    //if there is no data loading done, no need to create RS folders
    val loadMetadataDetailsArray = CarbonUtil.readLoadMetadata(metaDataPath).toList
    if (0 == loadMetadataDetailsArray.size) {
      CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
      return true
    }

    var currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }

    val loadStartTime = CarbonLoaderUtil.readCurrentTime();

    val resultMap = new CarbonAlterCubeRDD(sc,
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
        val molapLoadModel: CarbonLoadModel = new CarbonLoadModel()
        molapLoadModel.setCubeName(cubeName)
        molapLoadModel.setSchemaName(schemaName)
        molapLoadModel.setSchema(schema);
        val metadataDetails: LoadMetadataDetails = new LoadMetadataDetails()
        CarbonLoaderUtil.recordLoadMetadata(resultMap(0)._1, metadataDetails, molapLoadModel, CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS, loadStartTime)
        restructureStatus = CarbonUtil.createRSMetaFile(metaDataPath, "RS_" + (currentRestructNumber + 1))
      }
      if (restructureStatus) {
        CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
      }
    }

    restructureStatus
  }

  def dropAggregateTable(
                          sc: SparkContext,
                          schema: String,
                          cube: String,
                          partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropAggregateTableRDD(sc, kv, schema, cube, partitioner).collect
  }

  def dropCube(
                sc: SparkContext,
                schema: String,
                cube: String,
                partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropCubeRDD(sc, kv, schema, cube, partitioner).collect
  }

  def cleanFiles(
                  sc: SparkContext,
                  molapLoadModel: CarbonLoadModel,
                  hdfsStoreLocation: String,
                  partitioner: Partitioner) {

    val cube = CarbonMetadata.getInstance().getCubeWithCubeName(molapLoadModel.getCubeName(), molapLoadModel.getSchemaName());
    val metaDataPath: String = cube.getMetaDataFilepath()
    var currentRestructNumber = CarbonUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
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

}