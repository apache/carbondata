package com.huawei.datasight.spark.rdd

import java.text.SimpleDateFormat
import java.util.{Date, List}

import com.huawei.datasight.molap.load.{MolapLoadModel, MolapLoaderUtil}
import com.huawei.datasight.molap.merger.MolapDataMergerUtil
import com.huawei.datasight.molap.spark.splits.TableSplit
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.MergeResult
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.unibi.molap.olap.MolapDef
import com.huawei.unibi.molap.util.{MolapProperties, MolapUtil}
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner

import scala.collection.JavaConversions._

class MolapMergerRDD[K, V](
    sc: SparkContext,
    result: MergeResult[K,V],
    molapLoadModel: MolapLoadModel,
    storeLocation: String,
    hdfsStoreLocation: String,
    partitioner: Partitioner,
    currentRestructNumber: Integer,
    metadataFilePath:String,
    loadsToMerge:List[String],
    mergedLoadName:String,
    kettleHomePath:String,
    cubeCreationTime:Long)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def compute(theSplit: Partition, context: TaskContext) = {
      val iter = new Iterator[(K, V)] {
        var dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
        val split = theSplit.asInstanceOf[MolapLoadPartition]
        logInfo("Input split: " + split.serializableHadoopSplit.value)
        val partitionId = split.serializableHadoopSplit.value.getPartition().getUniqueID()
        val model = molapLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID())
        
        val mergedLoadMetadataDetails = MolapDataMergerUtil.executeMerging(model, storeLocation, hdfsStoreLocation, currentRestructNumber, metadataFilePath,loadsToMerge,mergedLoadName)
        
        model.setLoadMetadataDetails(MolapUtil
                .readLoadMetadata(metadataFilePath).toList);
        
        if(mergedLoadMetadataDetails == true)
        {
            MolapLoaderUtil.copyMergedLoadToHDFS(model, currentRestructNumber,mergedLoadName)
            dataloadStatus = checkAndLoadAggregationTable
            
        }

        // Register an on-task-completion callback to close the input stream.
        context.addOnCompleteCallback(() => close())
        var havePair = false
        var finished = false

        override def hasNext: Boolean = {
          if (!finished && !havePair) {
            finished = !false
            havePair = !finished
          }
          !finished
        }

        override def next(): (K, V) = {
          if (!hasNext) {
            throw new java.util.NoSuchElementException("End of stream")
          }
          havePair = false
        /*  val row = new MolapKey(null)
          val value = new MolapValue(null)*/
          result.getKey(0, mergedLoadMetadataDetails)
        }

        private def close() {
          try {
            //          reader.close()
          } catch {
            case e: Exception => logWarning("Exception in RecordReader.close()", e)
          }
        }
        
          def checkAndLoadAggregationTable():String = {
          var dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
          val schema = model.getSchema
          val aggTables = schema.cubes(0).fact.asInstanceOf[MolapDef.Table].aggTables
          if(null != aggTables && !aggTables.isEmpty)
          {
            val details = model.getLoadMetadataDetails.toSeq.toArray
            val newSlice = MolapCommonConstants.LOAD_FOLDER + mergedLoadName
            var listOfLoadFolders = MolapLoaderUtil.getListOfValidSlices(details)
            listOfLoadFolders = MolapLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
            val listOfUpdatedLoadFolders = MolapLoaderUtil.getListOfUpdatedSlices(details)
            val copyListOfLoadFolders = listOfLoadFolders.toList
            val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
            loadCubeSlices(listOfLoadFolders,listOfUpdatedLoadFolders)
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
            } else {
              logInfo("Aggregate tables creation successfull")
            }
          }
          dataloadStatus
        }
         
          
         def loadCubeSlices(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String]) = {
          MolapProperties.getInstance().addProperty("molap.cache.used", "false");
          MolapQueryUtil.createDataSource(currentRestructNumber, model.getSchema, null,partitionId, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName, hdfsStoreLocation,cubeCreationTime)
         }
          
         def iterateOverAggTables(aggTables: Array[MolapDef.AggTable], listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]):String = {
           model.setAggLoadRequest(true)
           aggTables.foreach { aggTable =>
              val aggTableName = MolapLoaderUtil.getAggregateTableName(aggTable)
              model.setAggTableName(aggTableName)
              dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
              if (MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
                logInfo(s"Aggregate table creation failed :: $aggTableName")
                return MolapCommonConstants.STORE_LOADSTATUS_FAILURE
              }
            }
          MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
        }
          
        def loadAggregationTable(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
          loadFolders.foreach { loadFolder =>
            val restructNumber = MolapUtil.getRestructureNumber(loadFolder, model.getTableName)
            try {
              if (MolapLoaderUtil.isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName)) {
                model.setFactStoreLocation(loadFolder)
                MolapLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, restructNumber)
                dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
              } else {
                MolapLoaderUtil.createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
              }
            } catch {
              case e: Exception => dataloadStatus = MolapCommonConstants.STORE_LOADSTATUS_FAILURE
            } finally {
              if (!MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
                val loadName = loadFolder.substring(loadFolder.indexOf(MolapCommonConstants.LOAD_FOLDER))
                MolapLoaderUtil.copyCurrentLoadToHDFS(model, restructNumber, loadName, listOfUpdatedLoadFolders,restructNumber)
              } else {
                logInfo(s"Load creation failed :: $loadFolder")
                return MolapCommonConstants.STORE_LOADSTATUS_FAILURE
              }
            }
          }
          return MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
        }  
        
        
      }
      iter
    }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def getPartitions: Array[Partition] = {
    val splits = MolapQueryUtil.getTableSplits(molapLoadModel.getSchemaName(), molapLoadModel.getCubeName(), null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapLoadPartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  class MolapLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
    extends Partition {

    val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

    override def hashCode(): Int = 41 * (41 + rddId) + idx

    override val index: Int = idx
  }

}