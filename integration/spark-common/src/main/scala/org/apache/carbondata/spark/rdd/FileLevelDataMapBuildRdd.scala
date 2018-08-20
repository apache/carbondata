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

package org.apache.carbondata.spark.rdd

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.{RecordReader, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.dev.DataMapBuilder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, TableInfo}
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit, CsvRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.spark.{RefreshResult, RefreshResultImpl}
import org.apache.carbondata.spark.format.CsvToCarbonReadSupport
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl

object FileLevelDataMapBuildRdd {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * for directly building datamap in a data loading procedure
   */
  def directlyBuildDataMap(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segment: Segment,
      factFilePath: String,
      dmSchemas: List[DataMapSchema]): Unit = {
    val segment2FactFilePath = new util.HashMap[Segment, String]()
    segment2FactFilePath.put(segment, factFilePath)
    buildDataMapInternal(sparkSession, carbonTable, segment2FactFilePath, dmSchemas)
  }

  /**
   * for building datamap on existed data
   */
  def buildDataMapOnExistedTable(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      dmSchemas: java.util.List[DataMapSchema]): Unit = {
    val validSegments = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
      .getValidAndInvalidSegments().getValidSegments.asScala
    val segment2FactFilePath = new util.HashMap[Segment, String]()
    validSegments.foreach { p =>
      segment2FactFilePath.put(p, p.getLoadMetadataDetails.getFactFilePath)
    }
    buildDataMapInternal(sparkSession, carbonTable, segment2FactFilePath, dmSchemas.asScala.toList)
  }

  /**
   * build specified datamap for specified segment
   * @param segment2FactFilePath fact files for corresponding segment
   */
  private def buildDataMapInternal(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segment2FactFilePath: java.util.Map[Segment, String],
      dmSchemas: List[DataMapSchema]): Unit = {
    LOGGER.error(s"building datamap" +
                 s" ${ dmSchemas.map(p => p.getDataMapName).mkString(", ") }" +
                 s" for table ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    val operationContext = new OperationContext()
    dmSchemas.foreach { dmSchema =>
      val buildDataMapPreExecutionEvent = BuildDataMapPreExecutionEvent(sparkSession,
        carbonTable.getAbsoluteTableIdentifier,
        mutable.Seq[String](dmSchema.getDataMapName))
      OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent, operationContext)
    }

    def firePostEvent(): Unit = {
      dmSchemas.foreach { dmSchema =>
        val buildDataMapPostExecutionEvent = BuildDataMapPostExecutionEvent(sparkSession,
          carbonTable.getAbsoluteTableIdentifier)
        OperationListenerBus.getInstance().fireEvent(buildDataMapPostExecutionEvent,
          operationContext)
      }
    }

    // segment -> (dmName, dmStorePath)
    val segment2DmPath = segment2FactFilePath.asScala.keySet.map { segment =>
      val dm2StorePath = dmSchemas.map { dmSchema =>
        val dmStorePath = CarbonTablePath.getDataMapStorePath(carbonTable.getTablePath,
          segment.getSegmentNo, dmSchema.getDataMapName)
       dmSchema.getDataMapName -> dmStorePath
      }.filter(p => !FileFactory.isFileExist(p._2)).toMap
      segment.getSegmentNo -> dm2StorePath
    }.toMap

    segment2DmPath.foreach { p =>
      p._2.values.foreach { dmPath =>
        if (!FileFactory.mkdirs(dmPath, FileFactory.getFileType(dmPath))) {
          firePostEvent()
          throw new IOException(
            s"Failed to create directory $dmPath for building datamap ${p._1}")
        }
      }
    }

    val status = new FileLevelDataMapBuildRdd[String, (String, Boolean)](
      sparkSession,
      new RefreshResultImpl(),
      segment2FactFilePath,
      dmSchemas,
      carbonTable.getTableInfo
    ).collect()

    val failedSegments = status
      .filter { case (segmentId, (filePath, buildStatus)) =>
        !buildStatus
      }
      .map { case (segmentId, _) =>
        val dmPath = segment2DmPath.apply(segmentId).values
        val cleanResult = dmPath.map(p =>
          FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(p)))
        if (cleanResult.exists(!_)) {
          LOGGER.error(s"Failed to clean up datamap store for segment_$segmentId")
          false
        } else {
          true
        }
      }

    firePostEvent()

    if (failedSegments.nonEmpty) {
      val msg = s"Failed to build all datamaps for table" +
                s" ${carbonTable.getDatabaseName}.${carbonTable.getTableName}"
      if (failedSegments.exists(!_)) {
        throw new Exception(s"$msg and clean up failed.")
      } else {
        throw new Exception(s"$msg and clean up successfully.")
      }
    }
  }
}

class FileLevelDataMapBuildRdd[K, V](
    sparkSession: SparkSession,
    result: RefreshResult[K, V],
    segment2FactFilePath: java.util.Map[Segment, String],
    dmSchemas: List[DataMapSchema],
    @transient tableInfo: TableInfo) extends CarbonRDDWithTableInfo[(K, V)](
  sparkSession.sparkContext, Nil, tableInfo.serialize()) {
  val FILE_LEVEL_INDEX_SHARD_PREFIX: String = "FileLevel_"

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  private val storageFormat = tableInfo.getFormat

  /**
   * each segment each fact file is a partition
   */
  override def getPartitions: Array[Partition] = {
    segment2FactFilePath.asScala
      .flatMap { case (segment, filePathStr) =>
        filePathStr.split(",")
          .map { filePath =>
            val carbonFile = FileFactory.getCarbonFile(filePath)
            val split = new CarbonInputSplit(segment.getSegmentNo,
              new Path(carbonFile.getPath),
              0,
              carbonFile.getSize,
              carbonFile.getLocations,
              carbonFile.getLocations,
              FileFormat.EXTERNAL)
            new CarbonMultiBlockSplit(Seq(split).asJava, carbonFile.getLocations)
          }
      }
      .zipWithIndex
      .map(split => new CarbonSparkPartition(id, split._2, split._1))
      .toArray
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = CarbonTable.buildFromTableInfo(getTableInfo)
    var status = false

    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    val inputFilePath = inputSplit.getAllSplits.get(0).getPath.toString

    val segmentId = inputSplit.getAllSplits.get(0).getSegment.getSegmentNo
    LOGGER.error(s"processing segment_$segmentId with file path: $inputFilePath}")
    val segment = segment2FactFilePath.keySet().asScala.find(p => p.getSegmentNo.equals(segmentId))
    if (segment.isDefined) {
      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
      attemptContext.getConfiguration.set(
        CarbonCommonConstants.CARBON_EXTERNAL_FORMAT_CONF_KEY, storageFormat)

      var dataMapbuilders: Seq[DataMapBuilder] = null
      var recordReader: RecordReader[Void, Object] = null
      try {
        dataMapbuilders = dmSchemas.map { dmSchema =>
          val dmFactory =
            DataMapStoreManager.getInstance().getDataMapFactoryClass(carbonTable, dmSchema)
          // the name of the shard here is base64 encoded fact file path
          val builder = dmFactory.createBuilder(segment.get,
            FILE_LEVEL_INDEX_SHARD_PREFIX + CarbonUtil.encodeToString(inputFilePath.getBytes), null)
          builder.initialize()
          builder
        }

        val allIndexColumns = dmSchemas.flatten(p => p.getIndexColumns).distinct.toArray
        val format = prepareInputFormat(attemptContext.getConfiguration, allIndexColumns)
        recordReader = format.createRecordReader(inputSplit, attemptContext)
        recordReader.asInstanceOf[CsvRecordReader[Object]].setVectorReader(false)
        // here we will convert raw value from file to carbon,
        // so that we can reuse the query procedure of the datamap
        recordReader.asInstanceOf[CsvRecordReader[Object]].setReadSupport(
          new CsvToCarbonReadSupport[Object])

        recordReader.initialize(inputSplit, attemptContext)

        // since the return  here contains all the index columns from all the datamaps,
        // we need to pick exactly the same columns as the datamap wanted, so here we will get the
        // column index among all the columns for each datamap.
        // Also we want to reuse the tmp row for each datamap during processing,
        // so here we allocate the space ahead and will reuse it later.
        val col2Idx = allIndexColumns.zipWithIndex.toMap
        val dmColIndicesAndTmpRowPair = dmSchemas.indices.map { dmIdx =>
          val indice = dmSchemas(dmIdx).getIndexColumns.map(col => col2Idx(col))
          val tmpRow = new Array[Object](indice.length)
          (indice, tmpRow)
        }

        while (recordReader.nextKeyValue()) {
          val row = recordReader.getCurrentValue.asInstanceOf[Array[Object]]
          // pick corresponding values for the index columns
          dmSchemas.indices.foreach { dmIdx =>
            val dmColIdxArray = dmColIndicesAndTmpRowPair(dmIdx)._1
            val builder = dataMapbuilders(dmIdx)
            val tmpRow = dmColIndicesAndTmpRowPair(dmIdx)._2
            tmpRow.indices.foreach { idx =>
              tmpRow(idx) = row(dmColIdxArray(idx))
            }
            // we do not have and care about the block/blocklet/page id here
            builder.addRow(0, 0, 0, tmpRow)
          }
        }

        dataMapbuilders.foreach(_.finish())
        status = true
      } finally {
        CarbonUtil.closeStream(recordReader)
        dataMapbuilders.filter(null != _).foreach(_.close())
      }
    }

    new Iterator[(K, V)] {
      var finished = false
      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(segmentId, (inputFilePath, status))
      }
    }
  }

  private def prepareInputFormat(conf: Configuration,
      projectionCols: Array[String]): CarbonInputFormat[Object] = {
    val format: CarbonInputFormat[Object] = new CarbonTableInputFormat[Object]

    CarbonInputFormat.setTableInfo(conf, getTableInfo)
    CarbonInputFormat.setDatabaseName(conf, getTableInfo.getDatabaseName)
    CarbonInputFormat.setTableName(conf, getTableInfo.getFactTable.getTableName)
    CarbonInputFormat.setDataTypeConverter(conf, classOf[SparkDataTypeConverterImpl])
    val identifier = getTableInfo.getOrCreateAbsoluteTableIdentifier()
    CarbonInputFormat.setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonInputFormat.setColumnProjection(conf, projectionCols)

    format
  }
}
