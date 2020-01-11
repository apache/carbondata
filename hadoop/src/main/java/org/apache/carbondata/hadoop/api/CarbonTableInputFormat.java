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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.carbondata.common.exceptions.DeprecatedFeatureException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonCommonConstantsInternal;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.DataMapUtil;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.statusmanager.StageInputCollector;
import org.apache.carbondata.core.stream.StreamFile;
import org.apache.carbondata.core.stream.StreamPruner;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * InputFormat for reading carbondata files with table level metadata support,
 * such as segment and explicit schema metadata.
 *
 * @param <T>
 */
public class CarbonTableInputFormat<T> extends CarbonInputFormat<T> {

  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  // comma separated list of input files
  public static final String INPUT_FILES = "mapreduce.input.carboninputformat.files";
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonTableInputFormat.class.getName());
  public static final String CARBON_TRANSACTIONAL_TABLE =
      "mapreduce.input.carboninputformat.transactional";
  public static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databaseName";
  public static final String TABLE_NAME = "mapreduce.input.carboninputformat.tableName";
  public static final String UPDATE_DELTA_VERSION = "updateDeltaVersion";
  // a cache for carbon table, it will be used in task side
  private CarbonTable carbonTable;
  private ReadCommittedScope readCommittedScope;

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR
   * are used to get table path to read.
   *
   * @param job
   * @return List<InputSplit> list of CarbonInputSplit
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    carbonTable = getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    // global dictionary is not supported since 2.0
    if (carbonTable.getTableInfo().getFactTable().getTableProperties().containsKey(
        CarbonCommonConstants.DICTIONARY_INCLUDE)) {
      DeprecatedFeatureException.globalDictNotSupported();
    }

    List<InputSplit> splits = new LinkedList<>();

    if (CarbonProperties.isQueryStageInputEnabled()) {
      // If there are stage files, collect them and create splits so that they are
      // included for the query
      try {
        List<InputSplit> stageInputSplits =
            StageInputCollector.createInputSplits(carbonTable, job.getConfiguration());
        splits.addAll(stageInputSplits);
      } catch (ExecutionException | InterruptedException e) {
        LOG.error("Failed to create input splits from stage files", e);
        throw new IOException(e);
      }
    }
    this.readCommittedScope = getReadCommitted(job, carbonTable.getAbsoluteTableIdentifier());
    LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();
    String updateDeltaVersion = job.getConfiguration().get(UPDATE_DELTA_VERSION);
    SegmentUpdateStatusManager updateStatusManager;
    if (updateDeltaVersion != null) {
      updateStatusManager =
          new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails, updateDeltaVersion);
    } else {
      updateStatusManager =
          new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails);
    }
    List<String> invalidSegmentIds = new ArrayList<>();
    List<Segment> streamSegments = null;
    // get all valid segments and set them into the configuration
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier(),
            readCommittedScope.getConfiguration());
    SegmentStatusManager.ValidAndInvalidSegmentsInfo segments = segmentStatusManager
        .getValidAndInvalidSegments(carbonTable.isChildTableForMV(), loadMetadataDetails,
            this.readCommittedScope);

    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      List<Segment> validSegments = segments.getValidSegments();
      streamSegments = segments.getStreamSegments();
      streamSegments = getFilteredSegment(job, streamSegments, true, readCommittedScope);
      if (validSegments.size() == 0) {
        splits.addAll(getSplitsOfStreaming(job, streamSegments, carbonTable));
        return splits;
      }
      List<Segment> filteredSegmentToAccess =
          getFilteredSegment(job, segments.getValidSegments(), true, readCommittedScope);
      if (filteredSegmentToAccess.size() == 0) {
        splits.addAll(getSplitsOfStreaming(job, streamSegments, carbonTable));
        return splits;
      } else {
        setSegmentsToAccess(job.getConfiguration(), filteredSegmentToAccess);
      }

      // remove entry in the segment index if there are invalid segments
      for (Segment segment : segments.getInvalidSegments()) {
        invalidSegmentIds.add(segment.getSegmentNo());
      }
      if (invalidSegmentIds.size() > 0) {
        DataMapStoreManager.getInstance()
            .clearInvalidSegments(getOrCreateCarbonTable(job.getConfiguration()),
                invalidSegmentIds);
      }
    }

    List<Segment> validAndInProgressSegments = new ArrayList<>(segments.getValidSegments());
    // Add in progress segments also to filter it as in case of Secondary Index table load it loads
    // data from in progress table.
    validAndInProgressSegments.addAll(segments.getListOfInProgressSegments());

    List<Segment> segmentToAccess =
        getFilteredSegment(job, validAndInProgressSegments, false, readCommittedScope);

    // process and resolve the expression
    DataMapFilter dataMapFilter = getFilterPredicates(job.getConfiguration());

    if (dataMapFilter != null) {
      dataMapFilter.resolve(false);
    }

    // do block filtering and get split
    List<InputSplit> batchSplits = getSplits(
        job, dataMapFilter, segmentToAccess,
        updateStatusManager, segments.getInvalidSegments());
    splits.addAll(batchSplits);

    // add all splits of streaming
    List<InputSplit> splitsOfStreaming = getSplitsOfStreaming(job, streamSegments, carbonTable);
    if (!splitsOfStreaming.isEmpty()) {
      splits.addAll(splitsOfStreaming);
    }
    return splits;
  }

  /**
   * Method to check and refresh segment cache
   *
   * @param job
   * @param carbonTable
   * @param updateStatusManager
   * @param filteredSegmentToAccess
   * @throws IOException
   */

  /**
   * Return segment list after filtering out valid segments and segments set by user by
   * `INPUT_SEGMENT_NUMBERS` in job configuration
   */
  private List<Segment> getFilteredSegment(JobContext job, List<Segment> validSegments,
      boolean validationRequired, ReadCommittedScope readCommittedScope) {
    Segment[] segmentsToAccess = getSegmentsToAccess(job, readCommittedScope);
    List<Segment> segmentToAccessSet =
        new ArrayList<>(new HashSet<>(Arrays.asList(segmentsToAccess)));
    List<Segment> filteredSegmentToAccess = new ArrayList<>();
    if (segmentsToAccess.length == 0 || segmentsToAccess[0].getSegmentNo().equalsIgnoreCase("*")) {
      filteredSegmentToAccess.addAll(validSegments);
    } else {
      for (Segment validSegment : validSegments) {
        int index = segmentToAccessSet.indexOf(validSegment);
        if (index > -1) {
          // In case of in progress reading segment, segment file name is set to the property itself
          if (segmentToAccessSet.get(index).getSegmentFileName() != null
              && validSegment.getSegmentFileName() == null) {
            filteredSegmentToAccess.add(segmentToAccessSet.get(index));
          } else {
            filteredSegmentToAccess.add(validSegment);
          }
        }
      }
      if (filteredSegmentToAccess.size() != segmentToAccessSet.size() && !validationRequired) {
        for (Segment segment : segmentToAccessSet) {
          if (!filteredSegmentToAccess.contains(segment)) {
            filteredSegmentToAccess.add(segment);
          }
        }
      }
      // TODO: add validation for set segments access based on valid segments in table status
      if (filteredSegmentToAccess.size() != segmentToAccessSet.size() && !validationRequired) {
        for (Segment segment : segmentToAccessSet) {
          if (!filteredSegmentToAccess.contains(segment)) {
            filteredSegmentToAccess.add(segment);
          }
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentToAccessSet)) {
        List<Segment> filteredSegmentToAccessTemp = new ArrayList<>(filteredSegmentToAccess);
        filteredSegmentToAccessTemp.removeAll(segmentToAccessSet);
        LOG.info(
            "Segments ignored are : " + Arrays.toString(filteredSegmentToAccessTemp.toArray()));
      }
    }
    return filteredSegmentToAccess;
  }

  public List<InputSplit> getSplitsOfStreaming(JobContext job, List<Segment> streamSegments,
      CarbonTable carbonTable) throws IOException {
    return getSplitsOfStreaming(job, streamSegments, carbonTable, null);
  }

  /**
   * use file list in .carbonindex file to get the split of streaming.
   */
  public List<InputSplit> getSplitsOfStreaming(JobContext job, List<Segment> streamSegments,
      CarbonTable carbonTable, FilterResolverIntf filterResolverIntf) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (streamSegments != null && !streamSegments.isEmpty()) {
      numStreamSegments = streamSegments.size();
      long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
      long maxSize = getMaxSplitSize(job);
      if (filterResolverIntf == null) {
        if (carbonTable != null) {
          DataMapFilter filter = getFilterPredicates(job.getConfiguration());
          if (filter != null) {
            filter.processFilterExpression();
            filterResolverIntf = filter.getResolver();
          }
        }
      }
      StreamPruner streamPruner = new StreamPruner(carbonTable);
      streamPruner.init(filterResolverIntf);
      List<StreamFile> streamFiles = streamPruner.prune(streamSegments);
      // record the hit information of the streaming files
      this.hitedStreamFiles = streamFiles.size();
      this.numStreamFiles = streamPruner.getTotalFileNums();
      for (StreamFile streamFile : streamFiles) {
        Path path = new Path(streamFile.getFilePath());
        long length = streamFile.getFileSize();
        if (length != 0) {
          BlockLocation[] blkLocations;
          FileSystem fs = FileFactory.getFileSystem(path);
          FileStatus file = fs.getFileStatus(path);
          blkLocations = fs.getFileBlockLocations(path, 0, length);
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);
          long bytesRemaining = length;
          // split the stream file to small splits
          // there is 10% slop to avoid to generate very small split in the end
          while (((double) bytesRemaining) / splitSize > 1.1) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(streamFile.getSegmentNo(), streamFile.getFilePath(),
                length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts(),
                    blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
            bytesRemaining -= splitSize;
          }
          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(streamFile.getSegmentNo(), streamFile.getFilePath(),
                length - bytesRemaining, bytesRemaining, blkLocations[blkIndex].getHosts(),
                blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
          }
        }
      }
    }
    return splits;
  }

  protected FileSplit makeSplit(String segmentId, String filePath, long start, long length,
      String[] hosts, String[] inMemoryHosts, FileFormat fileFormat) {
    return new CarbonInputSplit(segmentId, filePath, start, length, hosts, inMemoryHosts,
        fileFormat);
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, DataMapFilter expression,
      List<Segment> validSegments, SegmentUpdateStatusManager updateStatusManager,
      List<Segment> invalidSegments) throws IOException {

    List<String> segmentsToBeRefreshed = new ArrayList<>();
    if (!CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName())) {
      // Clean the updated segments from memory if the update happens on segments
      DataMapStoreManager.getInstance().refreshSegmentCacheIfRequired(carbonTable,
          updateStatusManager,
          validSegments);
    } else {
      segmentsToBeRefreshed = DataMapStoreManager.getInstance()
          .getSegmentsToBeRefreshed(carbonTable, updateStatusManager, validSegments);
    }

    numSegments = validSegments.size();
    List<InputSplit> result = new LinkedList<InputSplit>();
    UpdateVO invalidBlockVOForSegmentId = null;
    boolean isIUDTable = false;

    isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    // for each segment fetch blocks matching filter in Driver BTree
    List<org.apache.carbondata.hadoop.CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, carbonTable, expression, validSegments,
            invalidSegments, segmentsToBeRefreshed);
    numBlocks = dataBlocksOfSegment.size();
    for (org.apache.carbondata.hadoop.CarbonInputSplit inputSplit : dataBlocksOfSegment) {

      // Get the UpdateVO for those tables on which IUD operations being performed.
      if (isIUDTable) {
        invalidBlockVOForSegmentId =
            updateStatusManager.getInvalidTimestampRange(inputSplit.getSegmentId());
      }
      String[] deleteDeltaFilePath = null;
      if (isIUDTable) {
        // In case IUD is not performed in this table avoid searching for
        // invalidated blocks.
        if (CarbonUtil
            .isInvalidTableBlock(inputSplit.getSegmentId(), inputSplit.getFilePath(),
                invalidBlockVOForSegmentId, updateStatusManager)) {
          continue;
        }
        // When iud is done then only get delete delta files for a block
        try {
          deleteDeltaFilePath = updateStatusManager
              .getDeleteDeltaFilePath(inputSplit.getPath().toString(), inputSplit.getSegmentId());
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      inputSplit.setDeleteDeltaFiles(deleteDeltaFilePath);
      result.add(inputSplit);
    }
    return result;
  }

  /**
   * return valid segment to access
   */
  public Segment[] getSegmentsToAccess(JobContext job, ReadCommittedScope readCommittedScope) {
    String segmentString = job.getConfiguration().get(INPUT_SEGMENT_NUMBERS, "");
    if (segmentString.trim().isEmpty()) {
      return new Segment[0];
    }
    List<Segment> segments = Segment.toSegmentList(segmentString.split(","), readCommittedScope);
    return segments.toArray(new Segment[segments.size()]);
  }

  /**
   * Get the row count of the Block and mapping of segment and Block count.
   */
  public BlockMappingVO getBlockRowCount(Job job, CarbonTable table,
      List<PartitionSpec> partitions, boolean isUpdateFlow) throws IOException {
    // Normal query flow goes to CarbonInputFormat#getPrunedBlocklets and initialize the
    // pruning info for table we queried. But here count star query without filter uses a different
    // query plan, and no pruning info is initialized. When it calls default data map to
    // prune(with a null filter), exception will occur during setting pruning info.
    // Considering no useful information about block/blocklet pruning for such query
    // (actually no pruning), so we disable explain collector here
    ExplainCollector.remove();

    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();

    ReadCommittedScope readCommittedScope = getReadCommitted(job, identifier);
    LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();

    SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(
        table, loadMetadataDetails);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo allSegments =
        new SegmentStatusManager(identifier, readCommittedScope.getConfiguration())
            .getValidAndInvalidSegments(table.isChildTableForMV(), loadMetadataDetails,
                readCommittedScope);
    Map<String, Long> blockRowCountMapping = new HashMap<>();
    Map<String, Long> segmentAndBlockCountMapping = new HashMap<>();

    // TODO: currently only batch segment is supported, add support for streaming table
    List<Segment> filteredSegment =
        getFilteredSegment(job, allSegments.getValidSegments(), false, readCommittedScope);
    boolean isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);
    /* In the select * flow, getSplits() method was clearing the segmentMap if,
    segment needs refreshing. same thing need for select count(*) flow also.
    For NonTransactional table, one of the reason for a segment refresh is below scenario.
    SDK is written one set of files with UUID, with same UUID it can write again.
    So, latest files content should reflect the new count by refreshing the segment */
    List<String> toBeCleanedSegments = new ArrayList<>();
    for (Segment eachSegment : filteredSegment) {
      boolean refreshNeeded = DataMapStoreManager.getInstance()
          .getTableSegmentRefresher(getOrCreateCarbonTable(job.getConfiguration()))
          .isRefreshNeeded(eachSegment,
              updateStatusManager.getInvalidTimestampRange(eachSegment.getSegmentNo()));
      if (refreshNeeded) {
        toBeCleanedSegments.add(eachSegment.getSegmentNo());
      }
    }
    for (Segment segment : allSegments.getInvalidSegments()) {
      // remove entry in the segment index if there are invalid segments
      toBeCleanedSegments.add(segment.getSegmentNo());
    }
    if (toBeCleanedSegments.size() > 0) {
      DataMapStoreManager.getInstance()
          .clearInvalidSegments(getOrCreateCarbonTable(job.getConfiguration()),
              toBeCleanedSegments);
    }
    DataMapExprWrapper dataMapExprWrapper =
        DataMapChooser.getDefaultDataMap(getOrCreateCarbonTable(job.getConfiguration()), null);
    DataMapUtil.loadDataMaps(table, dataMapExprWrapper, filteredSegment, partitions);
    if (isIUDTable || isUpdateFlow) {
      Map<String, Long> blockletToRowCountMap = new HashMap<>();
      if (CarbonProperties.getInstance()
          .isDistributedPruningEnabled(table.getDatabaseName(), table.getTableName())) {
        try {
          List<ExtendedBlocklet> extendedBlocklets =
              getDistributedBlockRowCount(table, partitions, filteredSegment,
                  allSegments.getInvalidSegments(), toBeCleanedSegments);
          for (ExtendedBlocklet blocklet : extendedBlocklets) {
            String filePath = blocklet.getFilePath().replace("\\", "/");
            String blockName = filePath.substring(filePath.lastIndexOf("/") + 1);
            blockletToRowCountMap.put(blocklet.getSegmentId() + "," + blockName,
                blocklet.getRowCount());
          }
        } catch (Exception e) {
          // Check if fallback is disabled then directly throw exception otherwise try driver
          // pruning.
          if (CarbonProperties.getInstance().isFallBackDisabled()) {
            throw e;
          }
          TableDataMap defaultDataMap = DataMapStoreManager.getInstance().getDefaultDataMap(table);
          blockletToRowCountMap
              .putAll(defaultDataMap.getBlockRowCount(filteredSegment, partitions, defaultDataMap));
        }
      } else {
        TableDataMap defaultDataMap = DataMapStoreManager.getInstance().getDefaultDataMap(table);
        blockletToRowCountMap
            .putAll(defaultDataMap.getBlockRowCount(filteredSegment, partitions, defaultDataMap));
      }
      // key is the (segmentId","+blockletPath) and key is the row count of that blocklet
      for (Map.Entry<String, Long> eachBlocklet : blockletToRowCountMap.entrySet()) {
        String[] segmentIdAndPath = eachBlocklet.getKey().split(",", 2);
        String segmentId = segmentIdAndPath[0];
        String blockName = segmentIdAndPath[1];

        long rowCount = eachBlocklet.getValue();

        String key = CarbonUpdateUtil.getSegmentBlockNameKey(segmentId, blockName);

        // if block is invalid then don't add the count
        SegmentUpdateDetails details = updateStatusManager.getDetailsForABlock(key);

        if (null == details || !CarbonUpdateUtil.isBlockInvalid(details.getSegmentStatus())) {
          Long blockCount = blockRowCountMapping.get(key);
          if (blockCount == null) {
            blockCount = 0L;
            Long count = segmentAndBlockCountMapping.get(segmentId);
            if (count == null) {
              count = 0L;
            }
            segmentAndBlockCountMapping.put(segmentId, count + 1);
          }
          blockCount += rowCount;
          blockRowCountMapping.put(key, blockCount);
        }
      }
    } else {
      long totalRowCount;
      if (CarbonProperties.getInstance()
          .isDistributedPruningEnabled(table.getDatabaseName(), table.getTableName())) {
        totalRowCount =
            getDistributedCount(table, partitions, filteredSegment);
      } else {
        TableDataMap defaultDataMap = DataMapStoreManager.getInstance().getDefaultDataMap(table);
        totalRowCount = defaultDataMap.getRowCount(filteredSegment, partitions, defaultDataMap);
      }
      blockRowCountMapping.put(CarbonCommonConstantsInternal.ROW_COUNT, totalRowCount);
    }
    return new BlockMappingVO(blockRowCountMapping, segmentAndBlockCountMapping);
  }

  public ReadCommittedScope getReadCommitted(JobContext job, AbsoluteTableIdentifier identifier)
      throws IOException {
    if (readCommittedScope == null) {
      ReadCommittedScope readCommittedScope;
      if (job.getConfiguration().getBoolean(CARBON_TRANSACTIONAL_TABLE, true)) {
        readCommittedScope = new TableStatusReadCommittedScope(identifier, job.getConfiguration());
      } else {
        readCommittedScope = getReadCommittedScope(job.getConfiguration());
        if (readCommittedScope == null) {
          readCommittedScope =
              new LatestFilesReadCommittedScope(identifier.getTablePath(), job.getConfiguration());
        }
      }
      this.readCommittedScope = readCommittedScope;
    }
    return readCommittedScope;
  }
}