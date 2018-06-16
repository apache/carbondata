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

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{CarbonInputMetrics, Partition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.{DataMapRegistry, DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.dev.DataMapBuilder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.TaskMetricsMap
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit, CarbonProjection, CarbonRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport
import org.apache.carbondata.spark.{RefreshResult, RefreshResultImpl}
import org.apache.carbondata.spark.rdd.{CarbonRDDWithTableInfo, CarbonSparkPartition}
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl

/**
 * Helper object to rebuild the index DataMap
 */
object IndexDataMapRebuildRDD {

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
    val validAndInvalidSegments = segmentStatusManager.getValidAndInvalidSegments()
    val validSegments = validAndInvalidSegments.getValidSegments
    val indexedCarbonColumns = carbonTable.getIndexedColumns(schema)

    // loop all segments to rebuild DataMap
    validSegments.asScala.foreach { segment =>
      // if lucene datamap folder is exists, not require to build lucene datamap again
      refreshOneSegment(sparkSession, carbonTable, schema.getDataMapName,
        indexedCarbonColumns, segment.getSegmentNo);
    }
  }

  private def refreshOneSegment(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      dataMapName: String,
      indexColumns: java.util.List[CarbonColumn],
      segmentId: String): Unit = {

    val dataMapStorePath =
      CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath, segmentId, dataMapName)

    if (!FileFactory.isFileExist(dataMapStorePath)) {
      if (FileFactory.mkdirs(dataMapStorePath, FileFactory.getFileType(dataMapStorePath))) {
        try {
          val status = new IndexDataMapRebuildRDD[String, Boolean](
            sparkSession,
            new RefreshResultImpl(),
            carbonTable.getTableInfo,
            dataMapName,
            indexColumns.asScala.toArray,
            segmentId
          ).collect()

          status.find(_._2 == false).foreach { task =>
            throw new Exception(
              s"Task Failed to rebuild datamap $dataMapName on segment_$segmentId")
          }
        } catch {
          case ex: Throwable =>
            // process failure
            FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(dataMapStorePath))
            throw new Exception(
              s"Failed to refresh datamap $dataMapName on segment_$segmentId", ex)
        }
      } else {
        throw new IOException(s"Failed to create directory $dataMapStorePath")
      }
    }
  }

}

class OriginalReadSupport(dataTypes: Array[DataType]) extends CarbonReadSupport[Array[Object]] {
  override def initialize(carbonColumns: Array[CarbonColumn],
      carbonTable: CarbonTable): Unit = {
  }

  override def readRow(data: Array[Object]): Array[Object] = {
    dataTypes.zipWithIndex.foreach { case (dataType, i) =>
      if (dataType == DataTypes.STRING) {
        data(i) = data(i).toString
      }
    }
    data
  }

  override def close(): Unit = {
  }
}

class IndexDataMapRebuildRDD[K, V](
    session: SparkSession,
    result: RefreshResult[K, V],
    @transient tableInfo: TableInfo,
    dataMapName: String,
    indexColumns: Array[CarbonColumn],
    segmentId: String
) extends CarbonRDDWithTableInfo[(K, V)](
  session.sparkContext, Nil, tableInfo.serialize()) {

  private val dataMapSchema = DataMapStoreManager.getInstance().getDataMapSchema(dataMapName)
  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new util.Date())
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val dataMapFactory =
      DataMapManager.get().getDataMapProvider(
        CarbonTable.buildFromTableInfo(getTableInfo), dataMapSchema, session).getDataMapFactory
    var status = false
    val inputMetrics = new CarbonInputMetrics
    TaskMetricsMap.getInstance().registerThreadCallback()
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    inputMetrics.initBytesReadCallback(context, inputSplit)

    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val format = createInputFormat(attemptContext)

    val model = format.createQueryModel(inputSplit, attemptContext)
    // one query id per table
    model.setQueryId(queryId)
    model.setVectorReader(false)
    model.setForcedDetailRawQuery(false)
    model.setRequiredRowId(true)

    var reader: CarbonRecordReader[Array[Object]] = null
    var refresher: DataMapBuilder = null
    try {
      reader = new CarbonRecordReader(
        model, new OriginalReadSupport(indexColumns.map(_.getDataType)), inputMetrics)
      reader.initialize(inputSplit, attemptContext)

      // we use task name as shard name to create the folder for this datamap
      val shardName = CarbonTablePath.getShardName(inputSplit.getAllSplits.get(0).getBlockPath)
      refresher = dataMapFactory.createBuilder(new Segment(segmentId), shardName)
      refresher.initialize()

      var blockletId = 0
      var firstRow = true
      while (reader.nextKeyValue()) {
        val rowWithPosition = reader.getCurrentValue
        val size = rowWithPosition.length
        val pageId = rowWithPosition(size - 2).asInstanceOf[Int]
        val rowId = rowWithPosition(size - 1).asInstanceOf[Int]

        if (!firstRow && pageId == 0 && rowId == 0) {
          // new blocklet started, increase blockletId
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
            LOGGER.error(ex, "Failed to close reader")
        }
      }

      if (refresher != null) {
        try {
          refresher.close()
        } catch {
          case ex: Throwable =>
            LOGGER.error(ex, "Failed to close index writer")
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
        result.getKey(split.index.toString, status)
      }
    }
  }


  private def createInputFormat(
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
      Segment.toSegmentList(Array(segmentId), null))

    CarbonInputFormat.setColumnProjection(
      conf,
      new CarbonProjection(indexColumns.map(_.getColName)))
    format
  }

  override protected def getPartitions = {
    if (!dataMapSchema.isIndexDataMap) {
      throw new UnsupportedOperationException
    }
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    job.getConfiguration.set("query.id", queryId)

    val format = new CarbonTableInputFormat[Object]

    CarbonInputFormat.setSegmentsToAccess(
      job.getConfiguration,
      Segment.toSegmentList(Array(segmentId), null))

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

    format
      .getSplits(job)
      .asScala
      .map(_.asInstanceOf[CarbonInputSplit])
      .groupBy(_.taskId)
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
