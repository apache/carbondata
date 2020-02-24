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

package org.apache.carbondata.datamap

import java.io.IOException
import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{CarbonInputMetrics, Partition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.Decimal

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.dev.DataMapBuilder
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, DataTypeUtil, TaskMetricsMap}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.datamap.bloom.DataConvertUtil
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit, CarbonProjection, CarbonRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport
import org.apache.carbondata.spark.{RefreshResult, RefreshResultImpl}
import org.apache.carbondata.spark.rdd.{CarbonRDDWithTableInfo, CarbonSparkPartition}


/**
 * Helper object to rebuild the index DataMap
 */
object IndexDataMapRebuildRDD {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Rebuild the datamap for all existing data in the table
   */
  def rebuildDataMap(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      schema: DataMapSchema
  ): Unit = {
    val tableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val segmentStatusManager = new SegmentStatusManager(tableIdentifier)
    val validAndInvalidSegments = segmentStatusManager
      .getValidAndInvalidSegments(carbonTable.isChildTableForMV)
    val validSegments = validAndInvalidSegments.getValidSegments
    val indexedCarbonColumns = carbonTable.getIndexedColumns(schema)
    val operationContext = new OperationContext()
    val buildDataMapPreExecutionEvent = new BuildDataMapPreExecutionEvent(sparkSession,
      tableIdentifier,
      mutable.Seq[String](schema.getDataMapName))
    OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent, operationContext)

    val segments2DmStorePath = validSegments.asScala.map { segment =>
      val dataMapStorePath = CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath,
        segment.getSegmentNo, schema.getDataMapName)
      segment -> dataMapStorePath
    }.filter(p => !FileFactory.isFileExist(p._2)).toMap

    segments2DmStorePath.foreach { case (_, dmPath) =>
      if (!FileFactory.mkdirs(dmPath)) {
        throw new IOException(
          s"Failed to create directory $dmPath for rebuilding datamap ${ schema.getDataMapName }")
      }
    }

    val status = new IndexDataMapRebuildRDD[String, (String, Boolean)](
      sparkSession,
      new RefreshResultImpl(),
      carbonTable.getTableInfo,
      schema.getDataMapName,
      indexedCarbonColumns.asScala.toArray,
      segments2DmStorePath.keySet
    ).collect

    // for failed segments, clean the result
    val failedSegments = status
      .find { case (taskId, (segmentId, rebuildStatus)) =>
        !rebuildStatus
      }
      .map { task =>
        val segmentId = task._2._1
        val dmPath = segments2DmStorePath.filter(p => p._1.getSegmentNo.equals(segmentId)).values
        val cleanResult = dmPath.map(p =>
          FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(p)))
        if (cleanResult.exists(!_)) {
          LOGGER.error(s"Failed to clean up datamap store for segment_$segmentId")
          false
        } else {
          true
        }
      }

    if (failedSegments.nonEmpty) {
      throw new Exception(s"Failed to refresh datamap ${ schema.getDataMapName }")
    }

    val buildDataMapPostExecutionEvent = new BuildDataMapPostExecutionEvent(sparkSession,
      tableIdentifier, schema.getDataMapName, validSegments.asScala.map(_.getSegmentNo), true)
    OperationListenerBus.getInstance().fireEvent(buildDataMapPostExecutionEvent, operationContext)
  }
}

class OriginalReadSupport(dataTypes: Array[DataType]) extends CarbonReadSupport[Array[Object]] {
  override def initialize(carbonColumns: Array[CarbonColumn],
      carbonTable: CarbonTable): Unit = {
  }

  override def readRow(data: Array[Object]): Array[Object] = {
    dataTypes.zipWithIndex.foreach { case (dataType, i) =>
      if (dataType == DataTypes.STRING && data(i) != null) {
        data(i) = data(i).toString
      }
    }
    data
  }

  override def close(): Unit = {
  }
}

/**
 * This class will generate row value which is raw bytes for the dimensions.
 */
class RawBytesReadSupport(segmentProperties: SegmentProperties, indexColumns: Array[CarbonColumn])
  extends CarbonReadSupport[Array[Object]] {
  // for the dictionary dimensions
  var indexCol2IdxInDictArray: Map[String, Int] = Map()
  // for the non dictionary dimensions
  var indexCol2IdxInNoDictArray: Map[String, Int] = Map()
  // for the measures
  var indexCol2IdxInMeasureArray: Map[String, Int] = Map()

  override def initialize(carbonColumns: Array[CarbonColumn],
      carbonTable: CarbonTable): Unit = {

    val dictIndexColumns = new ListBuffer[CarbonColumn]()

    // prepare index info to extract data from query result
    indexColumns.foreach { col =>
      if (col.isDimension) {
        val dim = carbonTable.getDimensionByName(col.getColName)
        if (dim.getDataType != DataTypes.DATE) {
          indexCol2IdxInNoDictArray =
            indexCol2IdxInNoDictArray + (col.getColName -> indexCol2IdxInNoDictArray.size)
        } else {
          dictIndexColumns.append(col)
          indexCol2IdxInDictArray =
            indexCol2IdxInDictArray + (col.getColName -> indexCol2IdxInDictArray.size)
        }
      } else {
        indexCol2IdxInMeasureArray =
          indexCol2IdxInMeasureArray + (col.getColName -> indexCol2IdxInMeasureArray.size)
      }
    }

  }

  /**
   * input: all the dimensions are bundled in one ByteArrayWrapper in position 0,
   * then comes the measures one by one; last 3 elements are block/page/row id
   * output: all the dimensions and measures comes one after another
   */
  override def readRow(data: Array[Object]): Array[Object] = {

    val surrogatKeys = if (segmentProperties.getNumberOfDictDimensions > 0) {
      ByteUtil.convertBytesToLongArray(
        data(0).asInstanceOf[ByteArrayWrapper].getDictionaryKey)
    } else {
      new Array[Long](0)
    }

    // fill return row from data
    val rtn = new Array[Object](indexColumns.length + 3)
    indexColumns.zipWithIndex.foreach { case (col, i) =>
      rtn(i) = if (indexCol2IdxInDictArray.contains(col.getColName)) {
        surrogatKeys(indexCol2IdxInDictArray(col.getColName)).toInt.asInstanceOf[Integer]
      } else if (indexCol2IdxInNoDictArray.contains(col.getColName)) {
        val bytes = data(0).asInstanceOf[ByteArrayWrapper].getNoDictionaryKeyByIndex(
          indexCol2IdxInNoDictArray(col.getColName))
        // no dictionary primitive columns are expected to be in original data while loading,
        // so convert it to original data
        if (DataTypeUtil.isPrimitiveColumn(col.getDataType)) {
          var dataFromBytes = DataTypeUtil
            .getDataBasedOnDataTypeForNoDictionaryColumn(bytes, col.getDataType)
          if (dataFromBytes == null) {
            dataFromBytes = DataConvertUtil
              .getNullValueForMeasure(col.getDataType, col.getColumnSchema.getScale)
          }
          // for timestamp the above method will give the original data, so it should be
          // converted again to the format to be loaded (without micros)
          if (null != dataFromBytes && col.getDataType == DataTypes.TIMESTAMP) {
            dataFromBytes = (dataFromBytes.asInstanceOf[Long] / 1000L).asInstanceOf[Object];
          }
          dataFromBytes
        } else {
          bytes
        }
      } else {
        // measures start from 1
        val value = data(1 + indexCol2IdxInMeasureArray(col.getColName))
        if (null == value) {
          DataConvertUtil.getNullValueForMeasure(col.getDataType,
            col.getColumnSchema.getScale)
        } else if (DataTypes.isDecimal(col.getDataType)) {
          // In rebuild process, value is built for spark
          // here we need to convert it to java BigDecimal for carbon
          value.asInstanceOf[Decimal].toBigDecimal.bigDecimal
        } else {
          value
        }
      }
    }
    rtn(indexColumns.length) = data(data.length - 3)
    rtn(indexColumns.length + 1) = data(data.length - 2)
    rtn(indexColumns.length + 2) = data(data.length - 1)
    rtn
  }

  override def close(): Unit = {
  }
}

class IndexDataMapRebuildRDD[K, V](
    @transient private val session: SparkSession,
    result: RefreshResult[K, V],
    @transient private val tableInfo: TableInfo,
    dataMapName: String,
    indexColumns: Array[CarbonColumn],
    segments: Set[Segment])
  extends CarbonRDDWithTableInfo[(K, V)](session, Nil, tableInfo.serialize()) {

  private val dataMapSchema = DataMapStoreManager.getInstance().getDataMapSchema(dataMapName)
  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new util.Date())
  }

  private var readCommittedScope: ReadCommittedScope = _

  override def internalCompute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = CarbonTable.buildFromTableInfo(getTableInfo)
    val dataMapFactory = DataMapManager.get().getDataMapProvider(
      carbonTable, dataMapSchema, session).getDataMapFactory
    var status = false
    val inputMetrics = new CarbonInputMetrics
    TaskMetricsMap.getInstance().registerThreadCallback()
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    val segmentId = inputSplit.getAllSplits.get(0).getSegment.getSegmentNo
    val segment = segments.find(p => p.getSegmentNo.equals(segmentId))
    if (segment.isDefined) {
      inputMetrics.initBytesReadCallback(context, inputSplit, inputMetricsInterval)

      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val attemptContext = new TaskAttemptContextImpl(FileFactory.getConfiguration, attemptId)
      val format = createInputFormat(segment.get, attemptContext)
      segment.get.setReadCommittedScope(readCommittedScope)
      val model = format.createQueryModel(inputSplit, attemptContext)
      // one query id per table
      model.setQueryId(queryId)
      model.setVectorReader(false)
      model.setRequiredRowId(true)

      var reader: CarbonRecordReader[Array[Object]] = null
      var refresher: DataMapBuilder = null
      try {
        val segmentPropertiesFetcher = DataMapStoreManager.getInstance().getDataMap(carbonTable,
          BlockletDataMapFactory.DATA_MAP_SCHEMA).getDataMapFactory
          .asInstanceOf[SegmentPropertiesFetcher]
        val segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segment.get)

        // we use task name as shard name to create the folder for this datamap
        val shardName = CarbonTablePath.getShardName(inputSplit.getAllSplits.get(0).getBlockPath)
        refresher = dataMapFactory.createBuilder(segment.get, shardName, segmentProperties)
        refresher.initialize()

        model.setForcedDetailRawQuery(refresher.isIndexForCarbonRawBytes)
        val readSupport = if (refresher.isIndexForCarbonRawBytes) {
          new RawBytesReadSupport(segmentProperties, indexColumns)
        } else {
          new OriginalReadSupport(indexColumns.map(_.getDataType))
        }
        reader = new CarbonRecordReader[Array[Object]](model, readSupport, inputMetrics,
          attemptContext.getConfiguration)
        reader.initialize(inputSplit, attemptContext)
        // skip clear datamap and we will do this adter rebuild
        reader.setSkipClearDataMapAtClose(true)

        // Note that datamap rebuilding is based on query, the blockletId in rowWithPosition
        // is set to relative number in carbondata file in query process.
        // In order to get absolute blockletId in shard like the one filled in loading process,
        // here we use another way to generate it.
        var blockletId = 0
        var firstRow = true
        while (reader.nextKeyValue()) {
          val rowWithPosition = reader.getCurrentValue
          val size = rowWithPosition.length
          val pageId = rowWithPosition(size - 2).asInstanceOf[Int]
          val rowId = rowWithPosition(size - 1).asInstanceOf[Int]

          if (!firstRow && pageId == 0 && rowId == 0) {
            blockletId = blockletId + 1
          } else {
            firstRow = false
          }
          refresher.addRow(blockletId, pageId, rowId, rowWithPosition)
        }

        refresher.finish()

        status = true
      } finally {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case ex: Throwable =>
              LOGGER.error("Failed to close reader", ex)
          }
        }

        if (refresher != null) {
          try {
            refresher.close()
          } catch {
            case ex: Throwable =>
              LOGGER.error("Failed to close index writer", ex)
          }
        }
      }
    }

    new Iterator[(K, V)] {

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(split.index.toString, (segmentId, status))
      }
    }
  }


  private def createInputFormat(segment: Segment,
      attemptContext: TaskAttemptContextImpl) = {
    val format = new CarbonTableInputFormat[Object]
    val tableInfo1 = getTableInfo
    val conf = attemptContext.getConfiguration
    CarbonInputFormat.setTableInfo(conf, tableInfo1)
    CarbonInputFormat.setDatabaseName(conf, tableInfo1.getDatabaseName)
    CarbonInputFormat.setTableName(conf, tableInfo1.getFactTable.getTableName)
    CarbonInputFormat.setDataTypeConverter(conf, classOf[SparkDataTypeConverterImpl])

    val identifier = tableInfo1.getOrCreateAbsoluteTableIdentifier()
    CarbonInputFormat.setTablePath(
      conf,
      identifier.appendWithLocalPrefix(identifier.getTablePath))

    CarbonInputFormat.setSegmentsToAccess(
      conf,
      List(segment).asJava)

    CarbonInputFormat.setColumnProjection(
      conf,
      new CarbonProjection(indexColumns.map(_.getColName)))
    format
  }

  override protected def internalGetPartitions = {
    if (!dataMapSchema.isIndexDataMap) {
      throw new UnsupportedOperationException
    }
    val conf = FileFactory.getConfiguration
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    job.getConfiguration.set("query.id", queryId)

    val format = new CarbonTableInputFormat[Object]

    CarbonInputFormat.setSegmentsToAccess(
      job.getConfiguration,
      segments.toList.asJava)

    CarbonInputFormat.setTableInfo(
      job.getConfiguration,
      tableInfo)
    CarbonInputFormat.setTablePath(
      job.getConfiguration,
      tableInfo.getOrCreateAbsoluteTableIdentifier().getTablePath)
    CarbonInputFormat.setDatabaseName(
      job.getConfiguration,
      tableInfo.getDatabaseName)
    CarbonInputFormat.setTableName(
      job.getConfiguration,
      tableInfo.getFactTable.getTableName)

    // make the partitions based on block path so that all the CarbonInputSplits in a
    // MultiBlockSplit are used for bloom reading. This means 1 task for 1 shard(unique block path).
    val splits = format.getSplits(job)
    readCommittedScope = format.getReadCommitted(job, null)
    splits.asScala
      .map(_.asInstanceOf[CarbonInputSplit])
      .groupBy(p => (p.getSegmentId, p.taskId, p.getBlockPath))
      .map { group =>
        new CarbonMultiBlockSplit(
          group._2.asJava,
          group._2.flatMap(_.getLocations).toArray)
      }
      .zipWithIndex
      .map { split =>
        new CarbonSparkPartition(id, split._2, split._1)
      }
      .toArray
  }
}
