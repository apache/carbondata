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

package org.apache.spark.sql.secondaryindex.joins

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, JobContext}
import org.apache.log4j.Logger
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadCastFilterPushJoin, BuildLeft, BuildRight, BuildSide, HashJoin}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{IndexChooser, IndexFilter, IndexInputFormat, IndexStoreManager, IndexUtil, Segment}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastSIFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode with HashJoin {

  override def output: Seq[Attribute] = carbonScan.output

  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  lazy val partitions: Array[Segment] = if (mainTableRDD.isDefined && mainTableRDD.get
    .isInstanceOf[CarbonScanRDD[InternalRow]]) {
    BroadCastSIFilterPushJoin.getFilteredSegments(mainTableRDD.get
      .asInstanceOf[CarbonScanRDD[InternalRow]])
  } else {
    Array.empty[Segment]
  }

  val secondaryIndexRDD: Seq[RDD[InternalRow]] = buildPlan.collect {
    case batchData: CarbonDataSourceScan =>
      batchData.rdd
    case rowData: RowDataSourceScanExec =>
      rowData.rdd
  }

  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    val secondaryIndexRDD = buildPlan.collect {
      case batchData: CarbonDataSourceScan =>
        batchData.rdd
      case rowData: RowDataSourceScanExec =>
        rowData.rdd
    }
    if (partitions.nonEmpty && secondaryIndexRDD.nonEmpty) {
      secondaryIndexRDD.foreach {
        case value: CarbonScanRDD[InternalRow] =>
          val siSegments = SegmentStatusManager
            .readLoadMetadata(CarbonTablePath.getMetadataPath(value
              .getTableInfo
              .getTablePath))
            .map(_.getLoadName)
            .toList
          value.setSegmentsToAccess(partitions.filter(segment => siSegments.contains(segment
            .getSegmentNo)))
        case _ =>
      }
    }
    // If the partitions that are recognized from the main table are empty then no need to
    // execute the SI plan.
    if (partitions.nonEmpty) {
      val input: Array[InternalRow] = buildPlan.execute.map(_.copy()).collect()
      val inputCopy: Array[InternalRow] = input.clone()
      (input, inputCopy)
    } else {
      (Array.empty[InternalRow], Array.empty[InternalRow])
    }
  }
  val carbonScan: SparkPlan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }
  val mainTableRDD: Option[RDD[InternalRow]] = carbonScan.collectFirst {
    case batchData: CarbonDataSourceScan =>
      batchData.rdd
    case rowData: RowDataSourceScanExec =>
      rowData.rdd
  }

  override def doExecute(): RDD[InternalRow] = {
    BroadCastFilterPushJoin.addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide,
      isIndexTable = true)
    carbonScan.execute
  }
}

object BroadCastSIFilterPushJoin {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Used to get the valid segments after applying the following conditions.
   * 1. if user has specified segments for the parent table then those segments would be
   * considered
   * and valid segments would be filtered.
   * 2. if user has not specified segments then all valid segments would be considered for
   * scanning.
   *
   * @return
   * @throws IOException
   */
  def getFilteredSegments(job: JobContext,
      carbonTableInputFormat: CarbonTableInputFormat[Object]): util.List[Segment] = {
    val carbonTable: CarbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job
      .getConfiguration)
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.")
    } // copy dynamic set segment property from parent table to child index table
    setQuerySegmentForIndexTable(job.getConfiguration, carbonTable)
    val identifier: AbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val readCommittedScope: ReadCommittedScope = carbonTableInputFormat.getReadCommitted(job,
      identifier)
    val segmentsToAccess: Array[Segment] = carbonTableInputFormat.getSegmentsToAccess(job,
      readCommittedScope)
    val segmentsToAccessSet: util.Set[Segment] = new util.HashSet[Segment]
    for (segId <- segmentsToAccess) {
      segmentsToAccessSet.add(segId)
    }
    // get all valid segments and set them into the configuration
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(identifier)
    val segments: SegmentStatusManager.ValidAndInvalidSegmentsInfo = segmentStatusManager
      .getValidAndInvalidSegments(carbonTable.isMV)
    val validSegments: util.List[Segment] = segments.getValidSegments
    // if no segments in table
    val validSegmentsToAccess: util.List[Segment] = new util.ArrayList[Segment]
    if (validSegments.size == 0) {
      return new util.ArrayList[Segment](0)
    }
    if (segmentsToAccess.length == 0 ||
        segmentsToAccess(0).getSegmentNo.equalsIgnoreCase("*")) {
      validSegmentsToAccess.addAll(
        validSegments)
    } else {
      val filteredSegmentToAccess: util.List[Segment] = new util.ArrayList[Segment]
      for (segment <- validSegments.asScala) {
        if (segmentsToAccessSet.contains(segment)) {
          filteredSegmentToAccess.add(segment)
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentsToAccessSet)) {
        val filteredSegmentToAccessTemp: util.List[Segment] = new util.ArrayList[Segment]
        filteredSegmentToAccessTemp.addAll(filteredSegmentToAccess)
        filteredSegmentToAccessTemp.removeAll(segmentsToAccessSet)
        LOGGER.info(
          "Segments ignored are : " + util.Arrays.toString(filteredSegmentToAccessTemp.toArray))
      }
      // if no valid segments after filteration
      if (filteredSegmentToAccess.size == 0) {
        return new util.ArrayList[Segment](0)
      } else {
        validSegmentsToAccess.addAll(filteredSegmentToAccess)
      }
    }
    CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, validSegmentsToAccess)
    //    return getSplitsInternal(job, true);
    // process and resolve the expression
    val filter: IndexFilter = carbonTableInputFormat.getFilterPredicates(job.getConfiguration)
    val filteredSegments: util.List[Segment] = new util.ArrayList[Segment]
    if (filter != null) {
      filter.processFilterExpression()
      // refresh the segments if needed
      val loadMetadataDetails: Array[LoadMetadataDetails] = readCommittedScope.getSegmentList
      val updateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
        carbonTable,
        loadMetadataDetails)
      val setSegID: util.List[Segment] = new util.ArrayList[Segment]
      if (CarbonProperties.getInstance
        .isDistributedPruningEnabled(carbonTable.getDatabaseName, carbonTable.getTableName)) {
        val segmentsToBeRefreshed: util.List[String] = IndexStoreManager.getInstance
          .getSegmentsToBeRefreshed(carbonTable, validSegmentsToAccess)
        try {
          val indexFormat: IndexInputFormat =
            new IndexInputFormat(carbonTable,
              filter.getResolver,
              validSegmentsToAccess,
              segmentsToBeRefreshed,
              null,
              false,
              null,
              false, false)
          indexFormat.setTaskGroupId(SparkSQLUtil.getTaskGroupId(SparkSQLUtil.getSparkSession))
          indexFormat.setTaskGroupDesc(SparkSQLUtil.getTaskGroupDesc(SparkSQLUtil
            .getSparkSession))
          setSegID.addAll(IndexServer.getClient.getPrunedSegments(indexFormat).getSegments)
        } catch {
          case e: Exception =>
            LOGGER.warn("Distributed Segment Pruning failed, initiating embedded pruning", e)
            try {
              val indexFormat: IndexInputFormat = new IndexInputFormat(
                carbonTable,
                filter.getResolver,
                validSegmentsToAccess,
                segmentsToBeRefreshed,
                null,
                false,
                null,
                true, false)
              setSegID.addAll(IndexServer.getPrunedSegments(indexFormat).getSegments)
              val segmentsToBeCleaned: Array[String] = new Array[String](validSegments.size)
              for (i <- 0 until validSegments.size) {
                segmentsToBeCleaned(i) = validSegments.get(i).getSegmentNo
              }
              IndexServer.invalidateSegmentCache(carbonTable,
                segmentsToBeCleaned,
                SparkSQLUtil.getTaskGroupId(SparkSQLUtil.getSparkSession))
            } catch {
              case ex: Exception =>
                LOGGER.warn("Embedded Segment Pruning failed, initiating driver pruning", ex)
                IndexStoreManager.getInstance
                  .refreshSegmentCacheIfRequired(carbonTable,
                    updateStatusManager,
                    validSegmentsToAccess)
                setSegID.addAll(
                  isSegmentValidAfterFilter(job.getConfiguration, carbonTable, filter.getResolver,
                    validSegmentsToAccess));
            }
        }
      } else {
        IndexStoreManager.getInstance
          .refreshSegmentCacheIfRequired(carbonTable, updateStatusManager, validSegmentsToAccess)
        setSegID.addAll(
          isSegmentValidAfterFilter(job.getConfiguration, carbonTable, filter.getResolver,
            validSegmentsToAccess));
      }
      filteredSegments.addAll(setSegID)
    } else {
      filteredSegments.addAll(validSegmentsToAccess)
    }
    filteredSegments
  }

  /**
   * returns true if the filter expression lies between any one of the AbstractIndex min max values.
   */
  @throws[IOException]
  def isSegmentValidAfterFilter(configuration: Configuration,
      carbonTable: CarbonTable,
      filterResolverIntf: FilterResolverIntf,
      segmentIds: util.List[Segment]): util.List[Segment] = {
    val blockletMap = IndexStoreManager.getInstance.getDefaultIndex(carbonTable)
    val indexExprWrapper = IndexChooser.getDefaultIndex(carbonTable, filterResolverIntf)
    IndexUtil.loadIndexes(
      carbonTable,
      indexExprWrapper,
      segmentIds)
    blockletMap.pruneSegments(segmentIds, filterResolverIntf)
  }

  /**
   * Return's the valid segments for the query based on the filter condition
   * present in carbonScanRdd.
   *
   * @param carbonScanRdd
   * @return Array of valid segments
   */
  def getFilteredSegments(carbonScanRdd: CarbonScanRDD[InternalRow]): Array[Segment] = {
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val format = carbonScanRdd.prepareInputFormatForDriver(job.getConfiguration)
    val startTime = System.currentTimeMillis()
    val segmentsToAccess = getFilteredSegments(job, format).asScala.toArray
    LOGGER.info(
      "Time taken for getting the Filtered segments"
      + (System.currentTimeMillis - startTime) + " ,Total segments: " + segmentsToAccess.length)
    segmentsToAccess
  }

  /**
   * To copy dynamic set segment property form parent table to index table
   */
  def setQuerySegmentForIndexTable(conf: Configuration, carbonTable: CarbonTable) {
    if (carbonTable.isIndexTable) {
      val dbName = carbonTable.getDatabaseName;
      val tbName = carbonTable.getParentTableName;
      val segmentNumbersFromProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tbName, "*");
      if (!segmentNumbersFromProperty.trim().equals("*")) {
        CarbonInputFormat.setSegmentsToAccess(conf,
          Segment.toSegmentList(segmentNumbersFromProperty.split(","), null))
      }
    }
  }

  /**
   * Return's an array of valid segment numbers based on the filter condition provided in the sql
   *
   * @return Array of valid segments
   * @throws UnsupportedOperationException because Get Filter Segments API supports if and only
   *                                       if only one carbon main table is present in query.
   */
  def getFilteredSegments(query: String, sparkSession: SparkSession): Array[String] = {
    val dataFrame = sparkSession.sql(s"$query")

    def isMainTableRDD(rdd: CarbonScanRDD[InternalRow]): Boolean = {
      val tableInfo = rdd.getTableInfo
      val carbonTable = CarbonTable.buildFromTableInfo(tableInfo)
      if (!carbonTable.isIndexTable) {
        return true
      }
      false
    }

    val scanRDD =
      try {
        val collectRDD: Seq[CarbonScanRDD[InternalRow]] = dataFrame.queryExecution.sparkPlan
          .collect {
            case scan: CarbonDataSourceScan
              if scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] &&
                 isMainTableRDD(scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]) =>
              scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
            case scan: RowDataSourceScanExec
              if scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] &&
                 isMainTableRDD(scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]) =>
              scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
          }
        // If collectRDD length is 0 or greater than 1 then throw exception
        if (1 != collectRDD.length) {
          sys.error("Get Filter Segments API supports if and only if only " +
                    "one carbon main table is present in query.")
        }
        collectRDD
      } catch {
        case ex: Exception =>
          throw new UnsupportedOperationException(ex.getMessage)
      }
    val segmentToAccess = getFilteredSegments(scanRDD.head)
    val segmentIdtoAccess = new Array[String](segmentToAccess.length)
    for (i <- segmentToAccess.indices) {
      segmentIdtoAccess(i) = segmentToAccess(i).getSegmentNo
    }
    segmentIdtoAccess
  }
}
