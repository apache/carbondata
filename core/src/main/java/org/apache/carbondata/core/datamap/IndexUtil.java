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

package org.apache.carbondata.core.datamap;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.Index;
import org.apache.carbondata.core.datamap.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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

  private static final String DATA_MAP_DSTR = "mapreduce.input.carboninputformat.datamapdstr";

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
  public static Object createDataMapJob(String className) {
    try {
      return Class.forName(className).getDeclaredConstructors()[0].newInstance();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * This method sets the datamapJob in the configuration
   * @param configuration
   * @param dataMapJob
   * @throws IOException
   */
  public static void setDataMapJob(Configuration configuration, Object dataMapJob)
      throws IOException {
    if (dataMapJob != null) {
      String toString = ObjectSerializationUtil.convertObjectToString(dataMapJob);
      configuration.set(DATA_MAP_DSTR, toString);
    }
  }

  /**
   * get datamap job from the configuration
   * @param configuration job configuration
   * @return Index Job
   * @throws IOException
   */
  public static IndexJob getDataMapJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(DATA_MAP_DSTR);
    if (jobString != null) {
      return (IndexJob) ObjectSerializationUtil.convertStringToObject(jobString);
    }
    return null;
  }

  /**
   * This method gets the datamapJob and call execute , this job will be launched before clearing
   * datamaps from driver side during drop table and drop datamap and clears the datamap in executor
   * side
   * @param carbonTable
   * @throws IOException
   */
  private static void executeClearIndexJob(IndexJob indexJob,
      CarbonTable carbonTable, String dataMapToClear) throws IOException {
    SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
            getValidAndInvalidSegments(carbonTable, FileFactory.getConfiguration());
    List<String> invalidSegment = new ArrayList<>();
    for (Segment segment : validAndInvalidSegmentsInfo.getInvalidSegments()) {
      invalidSegment.add(segment.getSegmentNo());
    }
    IndexInputFormat indexInputFormat =
        new IndexInputFormat(carbonTable, validAndInvalidSegmentsInfo.getValidSegments(),
            invalidSegment, true, dataMapToClear);
    try {
      indexJob.execute(indexInputFormat);
    } catch (Exception e) {
      // Consider a scenario where clear datamap job is called from drop table
      // and index server crashes, in this no exception should be thrown and
      // drop table should complete.
      LOGGER.error("Failed to execute Datamap clear Job", e);
    }
  }

  public static void executeClearIndexJob(CarbonTable carbonTable, String jobClassName)
      throws IOException {
    executeClearIndexJob(carbonTable, jobClassName, "");
  }

  static void executeClearIndexJob(CarbonTable carbonTable, String jobClassName,
      String dataMapToClear) throws IOException {
    IndexJob indexJob = (IndexJob) createDataMapJob(jobClassName);
    if (indexJob == null) {
      return;
    }
    executeClearIndexJob(indexJob, carbonTable, dataMapToClear);
  }

  public static IndexJob getEmbeddedJob() {
    IndexJob indexJob = (IndexJob) IndexUtil.createDataMapJob(EMBEDDED_JOB_NAME);
    if (indexJob == null) {
      throw new ExceptionInInitializerError("Unable to create EmbeddedDataMapJob");
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

   Loads the datamaps in parallel by utilizing executor
   *
   @param carbonTable
   @param indexExprWrapper
   @param validSegments
   @param partitionsToPrune
   @throws IOException
   */
  public static void loadDataMaps(CarbonTable carbonTable, IndexExprWrapper indexExprWrapper,
      List<Segment> validSegments, List<PartitionSpec> partitionsToPrune) throws IOException {
    if (!CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName())
        && BlockletIndexUtil.loadDataMapsParallel(carbonTable)) {
      String clsName = "org.apache.spark.sql.secondaryindex.Jobs.SparkBlockletIndexLoaderJob";
      IndexJob indexJob = (IndexJob) createDataMapJob(clsName);
      String className = "org.apache.spark.sql.secondaryindex.Jobs.BlockletIndexInputFormat";
      SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
          getValidAndInvalidSegments(carbonTable, FileFactory.getConfiguration());
      List<Segment> invalidSegments = validAndInvalidSegmentsInfo.getInvalidSegments();
      FileInputFormat dataMapFormat =
          createDataMapJob(carbonTable, indexExprWrapper, validSegments, invalidSegments,
              partitionsToPrune, className, false);
      indexJob.execute(carbonTable, dataMapFormat);
    }
  }

  private static FileInputFormat createDataMapJob(CarbonTable carbonTable,
      IndexExprWrapper indexExprWrapper, List<Segment> validsegments,
      List<Segment> invalidSegments, List<PartitionSpec> partitionsToPrune, String clsName,
      boolean isJobToClearDataMaps) {
    try {
      Constructor<?> cons = Class.forName(clsName).getDeclaredConstructors()[0];
      return (FileInputFormat) cons
          .newInstance(carbonTable, indexExprWrapper, validsegments, invalidSegments,
              partitionsToPrune, isJobToClearDataMaps);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static List<ExtendedBlocklet> pruneDataMaps(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets,
      IndexChooser indexChooser) throws IOException {
    if (null == indexChooser) {
      return blocklets;
    }
    pruneSegments(segmentsToLoad, blocklets);
    List<ExtendedBlocklet> cgDataMaps = pruneDataMaps(table, filterResolverIntf, segmentsToLoad,
        partitions, blocklets,
        IndexLevel.CG, indexChooser);
    pruneSegments(segmentsToLoad, cgDataMaps);
    return pruneDataMaps(table, filterResolverIntf, segmentsToLoad,
        partitions, cgDataMaps,
        IndexLevel.FG, indexChooser);
  }

  static List<ExtendedBlocklet> pruneDataMaps(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets, IndexLevel indexLevel,
      IndexChooser indexChooser)
      throws IOException {
    IndexExprWrapper indexExprWrapper =
        indexChooser.chooseDataMap(indexLevel, filterResolverIntf);
    if (indexExprWrapper != null) {
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      // Prune segments from already pruned blocklets
      for (IndexInputSplitWrapper wrapper : indexExprWrapper
          .toDistributable(segmentsToLoad)) {
        TableIndex dataMap = DataMapStoreManager.getInstance()
            .getIndex(table, wrapper.getDistributable().getDataMapSchema());
        List<Index> indices = dataMap.getTableDataMaps(wrapper.getDistributable());
        List<ExtendedBlocklet> prunnedBlocklet = new ArrayList<>();
        if (table.isTransactionalTable()) {
          prunnedBlocklet.addAll(dataMap.prune(indices, wrapper.getDistributable(),
              indexExprWrapper.getFilterResolverIntf(wrapper.getUniqueId()), partitions));
        } else {
          prunnedBlocklet
              .addAll(dataMap.prune(segmentsToLoad, new IndexFilter(filterResolverIntf),
                  partitions));
        }
        // For all blocklets initialize the detail info so that it can be serialized to the driver.
        for (ExtendedBlocklet blocklet : prunnedBlocklet) {
          blocklet.getDetailInfo();
          blocklet.setDataMapUniqueId(wrapper.getUniqueId());
        }
        extendedBlocklets.addAll(prunnedBlocklet);
      }
      return indexExprWrapper.pruneBlocklets(extendedBlocklets);
    }
    return blocklets;
  }

  /**
   * this method gets the datamapJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeDataMapJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, IndexJob indexJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, IndexLevel level,
      List<String> segmentsToBeRefreshed) {
    return executeDataMapJob(carbonTable, resolver, indexJob, partitionsToPrune, validSegments,
        invalidSegments, level, false, segmentsToBeRefreshed, false);
  }

  /**
   * this method gets the datamapJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeDataMapJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, IndexJob indexJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, IndexLevel level,
      Boolean isFallbackJob, List<String> segmentsToBeRefreshed, boolean isCountJob) {
    List<String> invalidSegmentNo = new ArrayList<>();
    for (Segment segment : invalidSegments) {
      invalidSegmentNo.add(segment.getSegmentNo());
    }
    invalidSegmentNo.addAll(segmentsToBeRefreshed);
    IndexInputFormat dataMapFormat =
        new IndexInputFormat(carbonTable, resolver, validSegments, invalidSegmentNo,
            partitionsToPrune, false, level, isFallbackJob, false);
    if (isCountJob) {
      dataMapFormat.setCountStarJob();
      dataMapFormat.setIsWriteToFile(false);
    }
    return indexJob.execute(dataMapFormat);
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

  public static String getMaxSegmentID(List<String> segmentList) {
    double[] segment = new double[segmentList.size()];
    int i = 0;
    for (String id : segmentList) {
      segment[i] = Double.parseDouble(id);
      i++;
    }
    Arrays.sort(segment);
    String maxId = Double.toString(segment[segmentList.size() - 1]);
    if (maxId.endsWith(".0")) {
      maxId = maxId.substring(0, maxId.indexOf("."));
    }
    return maxId;
  }

}
