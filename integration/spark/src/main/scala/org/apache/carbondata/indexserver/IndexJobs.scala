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
package org.apache.carbondata.indexserver

import java.util

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.SizeEstimator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.index.{AbstractIndexJob, IndexInputFormat}
import org.apache.carbondata.core.indexstore.ExtendedBlocklet
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.scan.expression.BinaryExpression
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.scan.filter.resolver.{FilterResolverIntf, LogicalFilterResolverImpl, RowLevelFilterResolverImpl}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.spark.util.CarbonScalaUtil.logTime

/**
 * Spark job to execute index job and prune all the indexes distributable. This job will prune
 * and cache the appropriate indexes in indexes LRUCache.
 */
class DistributedIndexJob extends AbstractIndexJob {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def execute(indexFormat: IndexInputFormat): util.List[ExtendedBlocklet] = {
    if (LOGGER.isDebugEnabled) {
      val messageSize = SizeEstimator.estimate(indexFormat)
      LOGGER.debug(s"Size of message sent to Index Server: $messageSize")
    }
    val splitFolderPath = CarbonUtil
      .createTempFolderForIndexServer(indexFormat.getQueryId)
    LOGGER
      .info("Temp folder path for Query ID: " + indexFormat.getQueryId + " is " + splitFolderPath)
    val (resonse, time) = logTime {
      try {
        val spark = SparkSQLUtil.getSparkSession
        indexFormat.setTaskGroupId(SparkSQLUtil.getTaskGroupId(spark))
        indexFormat.setTaskGroupDesc(SparkSQLUtil.getTaskGroupDesc(spark))
        var filterInf = indexFormat.getFilterResolverIntf
        val filterProcessor = new FilterExpressionProcessor
        filterInf = removeSparkUnknown(filterInf,
          indexFormat.getCarbonTable.getAbsoluteTableIdentifier, filterProcessor)
        indexFormat.setFilterResolverIntf(filterInf)
        IndexServer.getClient.getSplits(indexFormat)
          .getExtendedBlockets(indexFormat.getCarbonTable.getTablePath, indexFormat
            .getQueryId, indexFormat.isCountStarJob)
      } finally {
        if (null != splitFolderPath && !splitFolderPath.deleteFile()) {
          LOGGER.error("Problem while deleting the temp directory:"
            + splitFolderPath.getAbsolutePath)
        }
      }
    }
    LOGGER.info(s"Time taken to get response from server: $time ms")
    resonse
  }

  /**
   * Iterate over FiltersReslover,
   *   a. Change only RowLevelFilterResolverImpl because SparkUnkown is part of it
   * and others FilterReslover like ConditionalFilterResolverImpl so directly return.
   *     b. Change SparkUnkownExpression to TrueExpression so that isScanRequired
   * selects block/blocklet.
   *
   * @param filterInf       FiltersReslover to be changed
   * @param tableIdentifer  AbsoluteTableIdentifier object
   * @param filterProcessor changed FiltersReslover.
   * @return
   */
  def removeSparkUnknown(filterInf: FilterResolverIntf,
      tableIdentifer: AbsoluteTableIdentifier,
                         filterProcessor: FilterExpressionProcessor): FilterResolverIntf = {
    if (filterInf.isInstanceOf[LogicalFilterResolverImpl]) {
      return new LogicalFilterResolverImpl(
        removeSparkUnknown(filterInf.getLeft, tableIdentifer, filterProcessor),
        removeSparkUnknown(filterInf.getRight, tableIdentifer, filterProcessor),
        filterProcessor.removeUnknownExpression(filterInf.getFilterExpression).
          asInstanceOf[BinaryExpression])
    }
    if (filterInf.isInstanceOf[RowLevelFilterResolverImpl] &&
        filterInf.getFilterExpression.getFilterExpressionType == ExpressionType.UNKNOWN) {
      return filterProcessor.changeUnknownResloverToTrue(tableIdentifer)
    }
    filterInf
  }

  override def executeCountJob(indexFormat: IndexInputFormat): java.lang.Long = {
    IndexServer.getClient.getCount(indexFormat).get()
  }
}

/**
 * Spark job to execute index job and prune all the indexes distributable. This job will just
 * prune the indexes but will not cache in executors.
 */
class EmbeddedIndexJob extends AbstractIndexJob {

  override def execute(indexFormat: IndexInputFormat): util.List[ExtendedBlocklet] = {
    val spark = SparkSQLUtil.getSparkSession
    val originalJobDesc = spark.sparkContext.getLocalProperty("spark.job.description")
    indexFormat.setIsWriteToFile(false)
    indexFormat.setFallbackJob()
    val splits = IndexServer.getSplits(indexFormat).getExtendedBlockets(indexFormat
      .getCarbonTable.getTablePath, indexFormat.getQueryId, indexFormat.isCountStarJob)
    // Fire a job to clear the cache from executors as Embedded mode does not maintain the cache.
    if (!indexFormat.isJobToClearIndexes) {
      IndexServer.invalidateSegmentCache(indexFormat.getCarbonTable, indexFormat
        .getValidSegmentIds.asScala.toArray, isFallBack = true)
    }
    spark.sparkContext.setLocalProperty("spark.job.description", originalJobDesc)
    splits
  }

  override def executeCountJob(inputFormat: IndexInputFormat): java.lang.Long = {
    inputFormat.setFallbackJob()
    IndexServer.getCount(inputFormat).get()
  }

}
