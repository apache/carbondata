
package com.huawei.datasight.spark.rdd

import com.huawei.datasight.molap.core.load.LoadMetadataDetails
import com.huawei.datasight.molap.load.{MolapLoadModel, MolapLoaderUtil}
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.datasight.molap.spark.util.{MolapQueryUtil, MolapSparkInterFaceLogEvent}
import com.huawei.datasight.spark.Result
import com.huawei.iweb.platform.logging.LogServiceFactory
import com.huawei.iweb.platform.logging.impl.StandardLogService
import com.huawei.unibi.molap.constants.{DataProcessorConstants, MolapCommonConstants}
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore
import com.huawei.unibi.molap.etl.DataLoadingException
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.{MolapProperties, MolapUtil}
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._

class MolapLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}

class MolapDataLoadRDD[K, V](
    sc: SparkContext,
    result: Result[K, V], molapLoadModel: MolapLoadModel,
    var storeLocation: String,
    hdfsStoreLocation: String,
    kettleHomePath: String,
    partitioner: Partitioner,
    columinar: Boolean,
    currentRestructNumber: Integer,
    loadCount: Integer,
    cubeCreationTime: Long,
    schemaLastUpdatedTime: Long)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    var splits = Array[com.huawei.datasight.molap.spark.splits.TableSplit]();
    if (molapLoadModel.isDirectLoad()) {
      splits = MolapQueryUtil.getTableSplitsForDirectLoad(molapLoadModel.getFactFilePath(), partitioner.nodeList, partitioner.partitionCount)
    }
    else {
      splits = MolapQueryUtil.getTableSplits(molapLoadModel.getSchemaName(), molapLoadModel.getCubeName(), null, partitioner)
    }
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapLoadPartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

    val iter = new Iterator[(K, V)] {
      var dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
      //var loadCount = 0
      var partitionID = "0"
      var model: MolapLoadModel = _

      try {
        val molapPropertiesFilePath = System.getProperty("molap.properties.filepath", null)
        if (null == molapPropertiesFilePath) {
          System.setProperty("molap.properties.filepath", System.getProperty("user.dir") + '/' + "conf" + '/' + "molap.properties");
        }
        val split = theSplit.asInstanceOf[MolapLoadPartition]
        logInfo("Input split: " + split.serializableHadoopSplit.value)
        MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
        MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
        MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
        MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
        MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
        MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
        MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
        MolapProperties.getInstance().addProperty("molap.leaf.node.size", "120000");
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
          storeLocation = storeLocation + "/molapstore/" + System.nanoTime()
        }

        if (molapLoadModel.isDirectLoad()) {
          model = molapLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID(),
            split.serializableHadoopSplit.value.getPartition().getFilesPath, molapLoadModel.getCsvHeader(), molapLoadModel.getCsvDelimiter())
        }
        else {
          model = molapLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID())
        }
        partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()
        StandardLogService.setThreadName(partitionID, null)
        dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
        if (model.isRetentionRequest()) {
          recreateAggregationTableForRetention
        }
        else if (model.isAggLoadRequest()) {
          dataloadStatus = createManualAggregateTable
        }
        else {
          try {
            MolapLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, currentRestructNumber);
            //loadCount = MolapLoaderUtil.getLoadCount(model, currentRestructNumber);
          } catch {
            case e: DataLoadingException => if (e.getErrorCode == DataProcessorConstants.BAD_REC_FOUND) {
              //loadCount = MolapLoaderUtil.getLoadCount(model, currentRestructNumber);
              dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              logInfo("Bad Record Found")
            } else {
              dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
            }
            case e: Exception =>
              dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
          } finally {
            if (!MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val newSlice = MolapCommonConstants.LOAD_FOLDER + loadCount
              var isCopyFailed = false
              try {
                MolapLoaderUtil.copyCurrentLoadToHDFS(model, currentRestructNumber, newSlice, null, currentRestructNumber);
              } catch {
                case e: Exception =>
                  isCopyFailed = true
                  dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
                  LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
              }
              if (!isCopyFailed)
                dataloadStatus = checkAndLoadAggregationTable
              if (MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
                logInfo("DataLoad failure")
              } else {
                logInfo("DataLoad complete")
                logInfo("Data Loaded successfully with LoadCount:" + loadCount)
              }
            } else {
              logInfo("DataLoad failure")
            }
          }
        }

      } catch {
        case e: Exception =>
          dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
          logInfo("DataLoad failure")
      }

      def checkAndLoadAggregationTable(): String = {
        val schema = model.getSchema
        val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.toSeq.toArray
          val newSlice = MolapCommonConstants.LOAD_FOLDER + loadCount
          var listOfLoadFolders = MolapLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = MolapLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
          val listOfUpdatedLoadFolders = MolapLoaderUtil.getListOfUpdatedSlices(details)
          val copyListOfLoadFolders = listOfLoadFolders.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
          loadCubeSlices(listOfLoadFolders, listOfUpdatedLoadFolders)
          var loadFolders = Array[String]()
          val loadFolder = MolapLoaderUtil.getAggLoadFolderLocation(newSlice, model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, currentRestructNumber)
          if (null != loadFolder) {
            loadFolders :+= loadFolder
          }
          dataloadStatus = iterateOverAggTables(aggTables, copyListOfLoadFolders, copyListOfUpdatedLoadFolders, loadFolders)
          if (MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            MolapLoaderUtil.removeSliceFromMemory(model.getSchemaName, model.getCubeName, newSlice)
            logInfo(s"Aggregate table creation failed")
          }
          else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        return dataloadStatus
      }

      def loadCubeSlices(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String]) = {
        MolapProperties.getInstance().addProperty("molap.cache.used", "false");
        val cube = InMemoryCubeStore.getInstance.loadCubeMetadataIfRequired(model.getSchema, model.getSchema.cubes(0), null, schemaLastUpdatedTime)
        MolapQueryUtil.createDataSource(currentRestructNumber, model.getSchema, cube, null, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName, hdfsStoreLocation, cubeCreationTime)
      }

      def createManualAggregateTable(): String = {
        val details = model.getLoadMetadataDetails.toSeq.toArray
        val listOfLoadFolders = MolapLoaderUtil.getListOfValidSlices(details)
        val listOfUpdatedLoadFolders = MolapLoaderUtil.getListOfUpdatedSlices(details)
        val copyListOfLoadFolders = listOfLoadFolders.toList
        val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
        loadCubeSlices(listOfLoadFolders, listOfUpdatedLoadFolders)
        var loadFolders = Array[String]()
        var restructFolders = Array[String]()
        for (number <- 0 to currentRestructNumber) {
          restructFolders = MolapLoaderUtil.getStorelocs(model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, number)
          loadFolders = loadFolders ++ restructFolders
        }
        val aggTable = model.getAggTableName
        dataloadStatus = loadAggregationTable(copyListOfLoadFolders, copyListOfUpdatedLoadFolders, loadFolders)
        if (MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
          logInfo(s"Aggregate table creation failed :: $aggTable")
        } else {
          logInfo(s"Aggregate table creation successfull :: $aggTable")
        }
        dataloadStatus
      }

      def recreateAggregationTableForRetention() = {
        val schema = model.getSchema
        val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.toSeq.toArray
          val listOfLoadFolders = MolapLoaderUtil.getListOfValidSlices(details)
          val listOfUpdatedLoadFolders = MolapLoaderUtil.getListOfUpdatedSlices(details)
          val copyListOfLoadFolders = listOfLoadFolders.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
          loadCubeSlices(listOfLoadFolders, listOfUpdatedLoadFolders)
          var loadFolders = Array[String]()
          copyListOfUpdatedLoadFolders.foreach { sliceNum =>
            val newSlice = MolapCommonConstants.LOAD_FOLDER + sliceNum
            val loadFolder = MolapLoaderUtil.getAggLoadFolderLocation(newSlice, model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, currentRestructNumber)
            if (null != loadFolder) {
              loadFolders :+= loadFolder
            }
          }
          iterateOverAggTables(aggTables, copyListOfLoadFolders, copyListOfUpdatedLoadFolders, loadFolders)
        }
      }

      def iterateOverAggTables(aggTables: Array[MolapDef.AggTable], listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.foreach { aggTable =>
          val aggTableName = MolapLoaderUtil.getAggregateTableName(aggTable)
          model.setAggTableName(aggTableName)
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
          if (MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: $aggTableName")
            return dataloadStatus
          }
        }
        return dataloadStatus
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        var levelCacheKeys: scala.collection.immutable.List[String] = Nil
        if (InMemoryCubeStore.getInstance.isLevelCacheEnabled()) {
          val columnList = MolapLoaderUtil.getColumnListFromAggTable(model)
          val details = model.getLoadMetadataDetails.toSeq.toArray
          levelCacheKeys = MolapQueryUtil.loadRequiredLevels(MolapQueryUtil.getListOfSlices(details), model.getSchemaName + '_' + model.getCubeName, columnList).toList
        }
        loadFolders.foreach { loadFolder =>
          val restructNumber = MolapUtil.getRestructureNumber(loadFolder, model.getTableName)
          try {
            if (MolapLoaderUtil.isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName)) {
              model.setFactStoreLocation(loadFolder)
              MolapLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, currentRestructNumber)
            } else {
              MolapLoaderUtil.createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
            }
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
              dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
          } finally {
            updateLevelCacheStatus(levelCacheKeys)
            if (!MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val loadName = loadFolder.substring(loadFolder.indexOf(MolapCommonConstants.LOAD_FOLDER))
              try {
                MolapLoaderUtil.copyCurrentLoadToHDFS(model, restructNumber, loadName, listOfUpdatedLoadFolders, currentRestructNumber)
              }
              catch {
                case e: Exception =>
                  LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e)
                  return MolapCommonConstants.STORE_LOADSTATUS_FAILURE;
              }
            } else {
              logInfo(s"Load creation failed :: $loadFolder")
              return dataloadStatus
            }
          }
        }
        return dataloadStatus
      }

      def updateLevelCacheStatus(levelCacheKeys: scala.collection.immutable.List[String]) = {
        levelCacheKeys.foreach { key =>
          InMemoryCubeStore.getInstance.updateLevelAccessCountInLRUCache(key)
        }
      }
      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (K, V) = {
        val loadMetadataDetails = new LoadMetadataDetails()
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(dataloadStatus.toString())
        result.getKey(loadCount, loadMetadataDetails)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Prefered Location for split : " + s(0))
    s
  }
}

