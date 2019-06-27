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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DataMapUtil {

  private static final String DATA_MAP_DSTR = "mapreduce.input.carboninputformat.datamapdstr";

  public static final String EMBEDDED_JOB_NAME =
      "org.apache.carbondata.indexserver.EmbeddedDataMapJob";

  public static final String DISTRIBUTED_JOB_NAME =
      "org.apache.carbondata.indexserver.DistributedDataMapJob";

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapUtil.class.getName());

  /**
   * Creates instance for the DataMap Job class
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
   * @return DataMap Job
   * @throws IOException
   */
  public static DataMapJob getDataMapJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(DATA_MAP_DSTR);
    if (jobString != null) {
      return (DataMapJob) ObjectSerializationUtil.convertStringToObject(jobString);
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
  private static void executeClearDataMapJob(DataMapJob dataMapJob,
      CarbonTable carbonTable, String dataMapToClear) throws IOException {
    SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
            getValidAndInvalidSegments(carbonTable, FileFactory.getConfiguration());
    List<String> invalidSegment = new ArrayList<>();
    for (Segment segment : validAndInvalidSegmentsInfo.getInvalidSegments()) {
      invalidSegment.add(segment.getSegmentNo());
    }
    DistributableDataMapFormat dataMapFormat = new DistributableDataMapFormat(carbonTable,
        validAndInvalidSegmentsInfo.getValidSegments(), invalidSegment, true,
        dataMapToClear);
    try {
      dataMapJob.execute(dataMapFormat);
    } catch (Exception e) {
      if (dataMapJob.getClass().getName().equalsIgnoreCase(DISTRIBUTED_JOB_NAME)) {
        LOGGER.warn("Failed to clear distributed cache.", e);
      } else {
        throw e;
      }
    }
  }

  public static void executeClearDataMapJob(CarbonTable carbonTable, String jobClassName)
      throws IOException {
    executeClearDataMapJob(carbonTable, jobClassName, "");
  }

  static void executeClearDataMapJob(CarbonTable carbonTable, String jobClassName,
      String dataMapToClear) throws IOException {
    DataMapJob dataMapJob = (DataMapJob) createDataMapJob(jobClassName);
    if (dataMapJob == null) {
      return;
    }
    executeClearDataMapJob(dataMapJob, carbonTable, dataMapToClear);
  }

  public static DataMapJob getEmbeddedJob() {
    DataMapJob dataMapJob = (DataMapJob) DataMapUtil.createDataMapJob(EMBEDDED_JOB_NAME);
    if (dataMapJob == null) {
      throw new ExceptionInInitializerError("Unable to create EmbeddedDataMapJob");
    }
    return dataMapJob;
  }

  /**
   * Prune the segments from the already pruned blocklets.
   */
  public static void pruneSegments(List<Segment> segments, List<ExtendedBlocklet> prunedBlocklets) {
    Set<Segment> validSegments = new HashSet<>();
    for (ExtendedBlocklet blocklet : prunedBlocklets) {
      // Clear the old pruned index files if any present
      blocklet.getSegment().getFilteredIndexShardNames().clear();
      // Set the pruned index file to the segment
      // for further pruning.
      String shardName = CarbonTablePath.getShardName(blocklet.getFilePath());
      blocklet.getSegment().setFilteredIndexShardName(shardName);
      validSegments.add(blocklet.getSegment());
    }
    segments.clear();
    segments.addAll(validSegments);
  }

  static List<ExtendedBlocklet> pruneDataMaps(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets,
      DataMapChooser dataMapChooser) throws IOException {
    if (null == dataMapChooser) {
      return blocklets;
    }
    pruneSegments(segmentsToLoad, blocklets);
    List<ExtendedBlocklet> cgDataMaps = pruneDataMaps(table, filterResolverIntf, segmentsToLoad,
        partitions, blocklets,
        DataMapLevel.CG, dataMapChooser);
    pruneSegments(segmentsToLoad, cgDataMaps);
    return pruneDataMaps(table, filterResolverIntf, segmentsToLoad,
        partitions, cgDataMaps,
        DataMapLevel.FG, dataMapChooser);
  }

  static List<ExtendedBlocklet> pruneDataMaps(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<Segment> segmentsToLoad,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets, DataMapLevel dataMapLevel,
      DataMapChooser dataMapChooser)
      throws IOException {
    DataMapExprWrapper dataMapExprWrapper =
        dataMapChooser.chooseDataMap(dataMapLevel, filterResolverIntf);
    if (dataMapExprWrapper != null) {
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      // Prune segments from already pruned blocklets
      for (DataMapDistributableWrapper wrapper : dataMapExprWrapper
          .toDistributable(segmentsToLoad)) {
        TableDataMap dataMap = DataMapStoreManager.getInstance()
            .getDataMap(table, wrapper.getDistributable().getDataMapSchema());
        List<DataMap> dataMaps = dataMap.getTableDataMaps(wrapper.getDistributable());
        List<ExtendedBlocklet> prunnedBlocklet = new ArrayList<>();
        if (table.isTransactionalTable()) {
          prunnedBlocklet.addAll(dataMap.prune(dataMaps, wrapper.getDistributable(),
              dataMapExprWrapper.getFilterResolverIntf(wrapper.getUniqueId()), partitions));
        } else {
          prunnedBlocklet
              .addAll(dataMap.prune(segmentsToLoad, new DataMapFilter(filterResolverIntf),
                  partitions));
        }
        // For all blocklets initialize the detail info so that it can be serialized to the driver.
        for (ExtendedBlocklet blocklet : prunnedBlocklet) {
          blocklet.getDetailInfo();
          blocklet.setDataMapUniqueId(wrapper.getUniqueId());
        }
        extendedBlocklets.addAll(prunnedBlocklet);
      }
      return dataMapExprWrapper.pruneBlocklets(extendedBlocklets);
    }
    return blocklets;
  }

  /**
   * this method gets the datamapJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeDataMapJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, DataMapJob dataMapJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, DataMapLevel level,
      List<String> segmentsToBeRefreshed) throws IOException {
    return executeDataMapJob(carbonTable, resolver, dataMapJob, partitionsToPrune, validSegments,
        invalidSegments, level, false, segmentsToBeRefreshed);
  }

  /**
   * this method gets the datamapJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeDataMapJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, DataMapJob dataMapJob, List<PartitionSpec> partitionsToPrune,
      List<Segment> validSegments, List<Segment> invalidSegments, DataMapLevel level,
      Boolean isFallbackJob, List<String> segmentsToBeRefreshed) throws IOException {
    List<String> invalidSegmentNo = new ArrayList<>();
    for (Segment segment : invalidSegments) {
      invalidSegmentNo.add(segment.getSegmentNo());
    }
    invalidSegmentNo.addAll(segmentsToBeRefreshed);
    DistributableDataMapFormat dataMapFormat =
        new DistributableDataMapFormat(carbonTable, resolver, validSegments, invalidSegmentNo,
            partitionsToPrune, false, level, isFallbackJob);
    List<ExtendedBlocklet> prunedBlocklets = dataMapJob.execute(dataMapFormat);
    // Apply expression on the blocklets.
    return prunedBlocklets;
  }

  public static SegmentStatusManager.ValidAndInvalidSegmentsInfo getValidAndInvalidSegments(
      CarbonTable carbonTable, Configuration configuration) throws IOException {
    SegmentStatusManager ssm =
        new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier(), configuration);
    return ssm.getValidAndInvalidSegments(carbonTable.isChildTable());
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
