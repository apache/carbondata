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

package org.apache.spark.sql

import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.index.SegmentIndexMeta
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager, StageInputCollector}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class CarbonCountStar(
    attributesRaw: Seq[Attribute],
    relation: LogicalRelation,
    carbonTable: CarbonTable,
    sparkSession: SparkSession,
    predicates: Seq[Expression],
    outUnsafeRows: Boolean = true) extends LeafExecNode {
  private val LOGGER = LogServiceFactory.getLogService(classOf[CarbonCountStar].getCanonicalName)

  override def doExecute(): RDD[InternalRow] = {
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val (job, tableInputFormat) = createCarbonInputFormat(absoluteTableIdentifier)
    CarbonInputFormat.setQuerySegment(job.getConfiguration, carbonTable)

    var totalRowCount: Long = 0
    if (predicates.nonEmpty) {
      totalRowCount = getRowCountWithOnlyPartitionColFilter
    } else {
      // get row count
      totalRowCount = CarbonUpdateUtil.getRowCount(
        tableInputFormat.getBlockRowCount(
          job,
          carbonTable,
          CarbonFilters.getPartitions(
            Seq.empty,
            sparkSession,
            TableIdentifier(
              carbonTable.getTableName,
              Some(carbonTable.getDatabaseName))).map(_.asJava).orNull, false),
        carbonTable)
    }

    if (CarbonProperties.isQueryStageInputEnabled) {
      // check for number of row for stage input
      val splits = StageInputCollector.createInputSplits(carbonTable, job.getConfiguration)
      if (!splits.isEmpty) {
        val df = DataLoadProcessBuilderOnSpark.createInputDataFrame(
          sparkSession, carbonTable, splits.asScala)
        totalRowCount += df.count()
      }
    }

    val valueRaw =
      attributesRaw.head.dataType match {
        case StringType => Seq(UTF8String.fromString(Long.box(totalRowCount).toString)).toArray
          .asInstanceOf[Array[Any]]
        case _ => Seq(Long.box(totalRowCount)).toArray.asInstanceOf[Array[Any]]
      }
    val value = new GenericInternalRow(valueRaw)
    val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val row = if (outUnsafeRows) unsafeProjection(value) else value
    sparkContext.parallelize(Seq(row))
  }

  override def output: Seq[Attribute] = {
    attributesRaw
  }

  private def createCarbonInputFormat(
      absoluteTableIdentifier: AbsoluteTableIdentifier
  ): (Job, CarbonTableInputFormat[Array[Object]]) = {
    val job = CarbonSparkUtil.createHadoopJob()
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    CarbonInputFormat.setTransactionalTable(job.getConfiguration,
      carbonTable.getTableInfo.isTransactionalTable)
    CarbonInputFormatUtil.setIndexJobIfConfigured(job.getConfiguration)
    (job, new CarbonTableInputFormat[Array[Object]]())
  }

  // The detail of query flow as following for count star with only partition column filter:
  // Step 1. check whether it is count star with only partition column filter
  // Step 2. read tablestatus to get all valid segments, remove the segment file cache of invalid
  // segment and expired segment
  // Step 3. use multi-thread to read segment files which not in cache and cache index files list
  // of each segment into memory. If its index files already exist in cache, not required to
  // read again.
  // Step 4. use multi-thread to prune segment and partition to get pruned index file list, which
  // can prune most index files and reduce the files num.
  // Step 5. read the count from pruned index file directly and cache it, get from cache if exist
  // in the index_file <-> rowCount map.
  private def getRowCountWithOnlyPartitionColFilter: Long = {
    var rowCount: Long = 0
    val prunedPartitionPaths = new java.util.HashSet[String]()
    // Get the current partitions from table.
    val partitions = CarbonFilters.getPrunedPartitions(relation, predicates)
    if (partitions != null) {
      for (partition <- partitions) {
        prunedPartitionPaths.add(partition.getLocation.toString)
      }
      val details = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
      // TODO handle the old store whose segment file not exists
      val validSegmentPaths = details.filter(segment =>
        ((segment.getSegmentStatus == SegmentStatus.SUCCESS) ||
          (segment.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS))
          && segment.getSegmentFile != null).map(segment => segment.getSegmentFile)
      val tableSegmentIndexes = IndexStoreManager.getInstance().getAllSegmentIndexes(
        carbonTable.getTableId)
      if (!tableSegmentIndexes.isEmpty) {
        // clear invalid cache
        for (segmentFilePathInCache <- tableSegmentIndexes.keySet().asScala) {
          if (!validSegmentPaths.contains(segmentFilePathInCache)) {
            // means invalid cache
            tableSegmentIndexes.remove(segmentFilePathInCache)
          }
        }
      }
      // init and put the valid cache
      for (validSegmentPath <- validSegmentPaths) {
        if (tableSegmentIndexes.get(validSegmentPath) == null) {
          val segmentIndexMeta = new SegmentIndexMeta(validSegmentPath)
          tableSegmentIndexes.put(validSegmentPath, segmentIndexMeta)
        }
      }

      val numThreads = Math.min(Math.max(validSegmentPaths.length, 1), 4)
      val executorService = Executors.newFixedThreadPool(numThreads)
      // to get the index files of valid segments from cache or scan through each segment
      val futures = new java.util.ArrayList[Future[Long]]
      for (segmentPath <- validSegmentPaths) {
        futures.add(executorService.submit(new Callable[Long] {
          override def call(): Long = {
            var rowCountCurrentSeg: Long = 0
            var indexFilesCurrentSeg: java.util.List[CarbonFile] = null;
            // tableSegmentIndexes already init.
            if (tableSegmentIndexes.get(segmentPath) != null &&
              tableSegmentIndexes.get(segmentPath).getSegmentIndexFiles.size() > 0) {
              indexFilesCurrentSeg =
                tableSegmentIndexes.get(segmentPath).getSegmentIndexFiles
            } else {
              // read from seg file
              val segmentFileStore = new SegmentFileStore(carbonTable.getTablePath, segmentPath)
              indexFilesCurrentSeg = segmentFileStore.getIndexCarbonFiles
              // tableSegmentIndexes already init and segmentIndexMeta not null.
              val segmentIndexMeta = tableSegmentIndexes.get(segmentPath)
              segmentIndexMeta.addSegmentIndexFiles(indexFilesCurrentSeg)
              // cache all the index files of this segment
              tableSegmentIndexes.put(segmentPath, segmentIndexMeta)
            }

            // use partition prune to get row count of each pruned index file and
            // cache it for this segment
            var prunedIndexFilesCurrentSeg: java.util.List[String] =
            new java.util.ArrayList[String]()
            for (indexFile <- indexFilesCurrentSeg.asScala) {
              // check whether the index files located in pruned partition
              val formattedPath = indexFile.getAbsolutePath
                .replace("\\", "/")
              if (prunedPartitionPaths.contains(
                formattedPath.substring(0, formattedPath.lastIndexOf("/")))) {
                prunedIndexFilesCurrentSeg.add(formattedPath)
              }
            }
            // get the row count from cache or read it from pruned index files
            var toReadIndexFiles: java.util.List[String] = new java.util.ArrayList[String]()
            prunedIndexFilesCurrentSeg.asScala.foreach(prunedIndexFilePath => {
                var count = tableSegmentIndexes.get(segmentPath).getPrunedIndexFileToRowCountMap
                  .get(prunedIndexFilePath)
                if (count != null) {
                  rowCountCurrentSeg += count
                } else {
                  toReadIndexFiles.add(prunedIndexFilePath)
                }
              }
            )

            if (toReadIndexFiles.size() > 0) {
              val numThreads = Math.min(Math.max(toReadIndexFiles.size(), 1), 4)
              val executor = Executors.newFixedThreadPool(numThreads)
              val readFutures = new java.util.ArrayList[Future[Long]]
              for (toReadIndexFile <- toReadIndexFiles.asScala) {
                readFutures.add(executor.submit(new Callable[Long] {
                  override def call(): Long = {
                    var rowCountCurrentIndexFile: Long = 0
                    if (toReadIndexFile.endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
                      rowCountCurrentIndexFile =
                        CarbonUtil.getRowCountFromIndexFile(toReadIndexFile)
                    } else if (toReadIndexFile.endsWith(
                      CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
                      rowCountCurrentIndexFile =
                        CarbonUtil.getRowCountFromMergeIndexFile(toReadIndexFile)
                    }
                    tableSegmentIndexes.get(segmentPath).getPrunedIndexFileToRowCountMap
                      .put(toReadIndexFile, rowCountCurrentIndexFile)
                    rowCountCurrentIndexFile
                  }
                }))
              }
              for (readFuture <- readFutures.asScala) {
                rowCountCurrentSeg += readFuture.get()
              }
            }

            rowCountCurrentSeg
          }
        }))
      }
      try {
        executorService.shutdown()
        executorService.awaitTermination(1, TimeUnit.DAYS)
      } catch {
        case e: InterruptedException =>
          LOGGER.error("Error while getting row count " + e.getMessage, e)
      }
      for (future <- futures.asScala) {
        rowCount += future.get()
      }
    }
    rowCount
  }

}
