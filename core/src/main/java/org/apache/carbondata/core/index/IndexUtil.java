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

package org.apache.carbondata.core.index;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.index.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.index.dev.secondaryindex.SIExpressionTree.CarbonSIExpression;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class IndexUtil {

  private static final String INDEX_DSTR = "mapreduce.input.carboninputformat.indexdstr";
  public static final String CARBON_INDEX_TABLES_SCAN_TREE =
      "mapreduce.input.carboninputformat.indextablesscantree";
  public static final String EMBEDDED_JOB_NAME =
      "org.apache.carbondata.indexserver.EmbeddedIndexJob";

  public static final String DISTRIBUTED_JOB_NAME =
      "org.apache.carbondata.indexserver.DistributedIndexJob";

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(IndexUtil.class.getName());

  /**
   * Creates instance for the Index Job class
   *
   * @param className
   * @return
   */
  public static Object createIndexJob(String className) {
    try {
      return Class.forName(className).getDeclaredConstructors()[0].newInstance();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * This method sets the indexJob in the configuration
   * @param configuration
   * @param IndexJob
   * @throws IOException
   */
  public static void setIndexJob(Configuration configuration, Object IndexJob)
      throws IOException {
    if (IndexJob != null) {
      String toString = ObjectSerializationUtil.convertObjectToString(IndexJob);
      configuration.set(INDEX_DSTR, toString);
    }
  }

  /**
   * get index job from the configuration
   * @param configuration job configuration
   * @return Index Job
   * @throws IOException
   */
  public static IndexJob getIndexJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(INDEX_DSTR);
    if (jobString != null) {
      return (IndexJob) ObjectSerializationUtil.convertStringToObject(jobString);
    }
    return null;
  }

  public static CarbonSIExpression getIndexTablesScanTree(Configuration configuration) {
    String indexTablesScanTree = configuration.get(CARBON_INDEX_TABLES_SCAN_TREE);
    try {
      if (indexTablesScanTree != null) {
        return (CarbonSIExpression) ObjectSerializationUtil
            .convertStringToObject(indexTablesScanTree);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to convert string to object", e);
    }
    return null;
  }

  /**
   * This method gets the indexJob and call execute , this job will be launched before clearing
   * indexes from driver side during drop table and drop index and clears the index in executor
   * side
   * @param carbonTable
   * @throws IOException
   */
  private static void executeClearIndexJob(IndexJob indexJob,
      CarbonTable carbonTable, String indexToClear) throws IOException {
    SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
            getValidAndInvalidSegments(carbonTable, FileFactory.getConfiguration());
    List<String> invalidSegment = new ArrayList<>();
    for (Segment segment : validAndInvalidSegmentsInfo.getInvalidSegments()) {
      invalidSegment.add(segment.getSegmentNo());
    }
    IndexInputFormat indexInputFormat =
        new IndexInputFormat(carbonTable, validAndInvalidSegmentsInfo.getValidSegments(),
            invalidSegment, true, indexToClear);
    try {
      indexJob.execute(indexInputFormat, null);
    } catch (Exception e) {
      // Consider a scenario where clear index job is called from drop table
      // and index server crashes, in this no exception should be thrown and
      // drop table should complete.
      LOGGER.error("Failed to execute Index clear Job", e);
    }
  }

  public static void executeClearIndexJob(CarbonTable carbonTable, String jobClassName)
      throws IOException {
    executeClearIndexJob(carbonTable, jobClassName, "");
  }

  static void executeClearIndexJob(CarbonTable carbonTable, String jobClassName,
      String indexToClear) throws IOException {
    IndexJob indexJob = (IndexJob) createIndexJob(jobClassName);
    if (indexJob == null) {
      return;
    }
    executeClearIndexJob(indexJob, carbonTable, indexToClear);
  }

  public static IndexJob getEmbeddedJob() {
    IndexJob indexJob = (IndexJob) IndexUtil.createIndexJob(EMBEDDED_JOB_NAME);
    if (indexJob == null) {
      throw new ExceptionInInitializerError("Unable to create EmbeddedIndexJob");
    }
    return indexJob;
  }

  /**
   * Prune the segments from the already pruned blocklets.
   */
  public static void pruneSegments(List<Segment> segments, List<ExtendedBlocklet> prunedBlocklets) {
    Map<Segment, Set<String>> validSegments = new HashMap<>();
    for (ExtendedBlocklet blocklet : prunedBlocklets) {
      // Set the pruned index file to the segment
      // for further pruning.
      String shardName = CarbonTablePath.getShardName(blocklet.getFilePath());
      // Add the existing shards to corresponding segments
      Set<String> existingShards = validSegments.get(blocklet.getSegment());
      if (existingShards == null) {
        existingShards = new HashSet<>();
        validSegments.put(blocklet.getSegment(), existingShards);
      }
      existingShards.add(shardName);
    }
    // override the shards list in the segments.
    for (Map.Entry<Segment, Set<String>> entry : validSegments.entrySet()) {
      entry.getKey().setFilteredIndexShardNames(entry.getValue());
    }
    segments.clear();
    // add the new segments to the segments list.
    segments.addAll(validSegments.keySet());
  }

  /**

   Loads the indexes in parallel by utilizing executor
   *
   @param carbonTable
   @param indexExprWrapper
   @param validSegments
   */
  public static void loadIndexes(CarbonTable carbonTable, IndexExprWrapper indexExprWrapper,
      List<Segment> validSegments) {
    if (!CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName())
        && BlockletIndexUtil.loadIndexesParallel(carbonTable)) {
      String clsName = "org.apache.spark.sql.secondaryindex.jobs.SparkBlockletIndexLoaderJob";
      IndexJob indexJob = (IndexJob) createIndexJob(clsName);
      String className = "org.apache.spark.sql.secondaryindex.jobs.BlockletIndexInputFormat";
      FileInputFormat indexFormat =
          createIndexJob(carbonTable, indexExprWrapper, validSegments, className);
      indexJob.execute(carbonTable, indexFormat);
    }
  }

  private static FileInputFormat createIndexJob(CarbonTable carbonTable,
      IndexExprWrapper indexExprWrapper, List<Segment> validSegments, String clsName) {
    try {
      Constructor<?> cons = Class.forName(clsName).getDeclaredConstructors()[0];
      return (FileInputFormat) cons
          .newInstance(carbonTable, indexExprWrapper, validSegments);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static List<ExtendedBlocklet> pruneIndexes(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets,
      IndexChooser indexChooser) throws IOException {
    if (null == indexChooser) {
      return blocklets;
    }
    pruneSegments(segmentsToLoad, blocklets);
    List<ExtendedBlocklet> cgIndexes = pruneIndexes(table, filterResolverIntf, segmentsToLoad,
        partitions, blocklets,
        IndexLevel.CG, indexChooser);
    pruneSegments(segmentsToLoad, cgIndexes);
    return pruneIndexes(table, filterResolverIntf, segmentsToLoad,
        partitions, cgIndexes,
        IndexLevel.FG, indexChooser);
  }

  static List<ExtendedBlocklet> pruneIndexes(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets, IndexLevel indexLevel,
      IndexChooser indexChooser)
      throws IOException {
    IndexExprWrapper indexExprWrapper =
        indexChooser.chooseIndex(indexLevel, filterResolverIntf);
    if (indexExprWrapper != null) {
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      // Prune segments from already pruned blocklets
      for (IndexInputSplitWrapper wrapper : indexExprWrapper
          .toDistributable(segmentsToLoad)) {
        TableIndex index = IndexStoreManager.getInstance()
            .getIndex(table, wrapper.getDistributable().getIndexSchema());
        List<Index> indices = index.getTableIndexes(wrapper.getDistributable());
        List<ExtendedBlocklet> prunedBlocklet = new ArrayList<>();
        if (table.isTransactionalTable()) {
          prunedBlocklet.addAll(index.prune(indices, wrapper.getDistributable(),
              indexExprWrapper.getFilterResolverIntf(wrapper.getUniqueId()), partitions));
        } else {
          prunedBlocklet
              .addAll(index.prune(segmentsToLoad, new IndexFilter(filterResolverIntf),
                  partitions));
        }
        // For all blocklets initialize the detail info so that it can be serialized to the driver.
        for (ExtendedBlocklet blocklet : prunedBlocklet) {
          blocklet.getDetailInfo();
          blocklet.setIndexUniqueId(wrapper.getUniqueId());
        }
        extendedBlocklets.addAll(prunedBlocklet);
      }
      return indexExprWrapper.pruneBlocklets(extendedBlocklets);
    }
    return blocklets;
  }

  /**
   * this method gets the indexJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeIndexJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, IndexJob indexJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, IndexLevel level,
      List<String> segmentsToBeRefreshed, Configuration configuration) {
    return executeIndexJob(carbonTable, resolver, indexJob, partitionsToPrune, validSegments,
        invalidSegments, level, false, segmentsToBeRefreshed, false,
        configuration);
  }

  /**
   * this method gets the indexJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeIndexJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, IndexJob indexJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, IndexLevel level,
      Boolean isFallbackJob, List<String> segmentsToBeRefreshed, boolean isCountJob,
      Configuration configuration) {
    List<String> invalidSegmentNo = new ArrayList<>();
    for (Segment segment : invalidSegments) {
      invalidSegmentNo.add(segment.getSegmentNo());
    }
    invalidSegmentNo.addAll(segmentsToBeRefreshed);
    IndexInputFormat indexInputFormat =
        new IndexInputFormat(carbonTable, resolver, validSegments, invalidSegmentNo,
            partitionsToPrune, false, level, isFallbackJob, false);
    indexInputFormat.setIndexTablesScanTree(IndexUtil.getIndexTablesScanTree(configuration));
    if (isCountJob) {
      indexInputFormat.setCountStarJob();
      indexInputFormat.setIsWriteToFile(false);
    }
    return indexJob.execute(indexInputFormat, configuration);
  }

  public static SegmentStatusManager.ValidAndInvalidSegmentsInfo getValidAndInvalidSegments(
      CarbonTable carbonTable, Configuration configuration) throws IOException {
    SegmentStatusManager ssm =
        new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier(), configuration);
    return ssm.getValidAndInvalidSegments(carbonTable.isMV());
  }

  /**
   * Returns valid segment list for a given RelationIdentifier
   *
   * @param relationIdentifier get list of segments for relation identifier
   * @return list of valid segment id's
   * @throws IOException
   */
  public static List<String> getMainTableValidSegmentList(RelationIdentifier relationIdentifier)
      throws IOException {
    List<String> segmentList = new ArrayList<>();
    List<Segment> validSegments = new SegmentStatusManager(AbsoluteTableIdentifier
        .from(relationIdentifier.getTablePath(), relationIdentifier.getDatabaseName(),
            relationIdentifier.getTableName())).getValidAndInvalidSegments().getValidSegments();
    for (Segment segment : validSegments) {
      segmentList.add(segment.getSegmentNo());
    }
    return segmentList;
  }
}
