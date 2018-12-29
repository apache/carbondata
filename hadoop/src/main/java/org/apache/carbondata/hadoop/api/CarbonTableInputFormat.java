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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.stream.StreamFile;
import org.apache.carbondata.core.stream.StreamPruner;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.conf.Configuration;
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
  private static final String ALTER_PARTITION_ID = "mapreduce.input.carboninputformat.partitionid";
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonTableInputFormat.class.getName());
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  private static final String CARBON_CONVERTER = "mapreduce.input.carboninputformat.converter";
  private static final String CARBON_TRANSACTIONAL_TABLE =
      "mapreduce.input.carboninputformat.transactional";
  public static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databaseName";
  public static final String TABLE_NAME = "mapreduce.input.carboninputformat.tableName";
  // a cache for carbon table, it will be used in task side
  private CarbonTable carbonTable;
  private ReadCommittedScope readCommittedScope;

  /**
   * Get the cached CarbonTable or create it by TableInfo in `configuration`
   */
  public CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
    if (carbonTable == null) {
      // carbon table should be created either from deserialized table info (schema saved in
      // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
      TableInfo tableInfo = getTableInfo(configuration);
      CarbonTable carbonTable;
      if (tableInfo != null) {
        carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      } else {
        carbonTable = SchemaReader.readCarbonTableFromStore(
            getAbsoluteTableIdentifier(configuration));
      }
      this.carbonTable = carbonTable;
      return carbonTable;
    } else {
      return this.carbonTable;
    }
  }

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
    AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(job.getConfiguration());
    CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    this.readCommittedScope = getReadCommitted(job, identifier);
    LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();

    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails);
    List<Segment> invalidSegments = new ArrayList<>();
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();
    List<Segment> streamSegments = null;
    // get all valid segments and set them into the configuration
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier,
        readCommittedScope.getConfiguration());
    SegmentStatusManager.ValidAndInvalidSegmentsInfo segments = segmentStatusManager
        .getValidAndInvalidSegments(loadMetadataDetails, this.readCommittedScope);

    // to check whether only streaming segments access is enabled or not,
    // if access streaming segment is true then data will be read from streaming segments
    boolean accessStreamingSegments = getAccessStreamingSegments(job.getConfiguration());
    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      if (!accessStreamingSegments) {
        List<Segment> validSegments = segments.getValidSegments();
        streamSegments = segments.getStreamSegments();
        streamSegments = getFilteredSegment(job, streamSegments, true, readCommittedScope);
        if (validSegments.size() == 0) {
          return getSplitsOfStreaming(job, streamSegments, carbonTable);
        }
        List<Segment> filteredSegmentToAccess =
            getFilteredSegment(job, segments.getValidSegments(), true, readCommittedScope);
        if (filteredSegmentToAccess.size() == 0) {
          return getSplitsOfStreaming(job, streamSegments, carbonTable);
        } else {
          setSegmentsToAccess(job.getConfiguration(), filteredSegmentToAccess);
        }
      } else {
        List<Segment> filteredNormalSegments =
            getFilteredNormalSegments(job, segments.getValidSegments(),
                getSegmentsToAccess(job, readCommittedScope));
        streamSegments = segments.getStreamSegments();
        if (filteredNormalSegments.size() == 0) {
          return getSplitsOfStreaming(job, streamSegments, carbonTable);
        }
        setSegmentsToAccess(job.getConfiguration(),filteredNormalSegments);
      }
      // remove entry in the segment index if there are invalid segments
      invalidSegments.addAll(segments.getInvalidSegments());
      invalidTimestampsList.addAll(updateStatusManager.getInvalidTimestampRange());
      if (invalidSegments.size() > 0) {
        DataMapStoreManager.getInstance()
            .clearInvalidSegments(getOrCreateCarbonTable(job.getConfiguration()), invalidSegments);
      }
    }
    List<Segment> validAndInProgressSegments = new ArrayList<>(segments.getValidSegments());
    // Add in progress segments also to filter it as in case of aggregate table load it loads
    // data from in progress table.
    validAndInProgressSegments.addAll(segments.getListOfInProgressSegments());
    // get updated filtered list
    List<Segment> filteredSegmentToAccess =
        getFilteredSegment(job, new ArrayList<>(validAndInProgressSegments), false,
            readCommittedScope);
    // Clean the updated segments from memory if the update happens on segments
    refreshSegmentCacheIfRequired(job, carbonTable, updateStatusManager, filteredSegmentToAccess);

    // process and resolve the expression
    Expression filter = getFilterPredicates(job.getConfiguration());
    // this will be null in case of corrupt schema file.
    PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());

    // prune partitions for filter query on partition table
    BitSet matchedPartitions = null;
    if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
      carbonTable.processFilterExpression(filter, null, null);
      matchedPartitions = setMatchedPartitions(null, filter, partitionInfo, null);
      if (matchedPartitions != null) {
        if (matchedPartitions.cardinality() == 0) {
          return new ArrayList<InputSplit>();
        } else if (matchedPartitions.cardinality() == partitionInfo.getNumPartitions()) {
          matchedPartitions = null;
        }
      }
    }

    // do block filtering and get split
    List<InputSplit> splits =
        getSplits(job, filter, filteredSegmentToAccess, matchedPartitions, partitionInfo,
            null, updateStatusManager);
    // pass the invalid segment to task side in order to remove index entry in task side
    if (invalidSegments.size() > 0) {
      for (InputSplit split : splits) {
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split).setInvalidSegments(invalidSegments);
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split)
            .setInvalidTimestampRange(invalidTimestampsList);
      }
    }

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
  public void refreshSegmentCacheIfRequired(JobContext job, CarbonTable carbonTable,
      SegmentUpdateStatusManager updateStatusManager, List<Segment> filteredSegmentToAccess)
      throws IOException {
    List<Segment> toBeCleanedSegments = new ArrayList<>();
    for (Segment filteredSegment : filteredSegmentToAccess) {
      boolean refreshNeeded =
          DataMapStoreManager.getInstance().getTableSegmentRefresher(carbonTable)
              .isRefreshNeeded(filteredSegment,
                  updateStatusManager.getInvalidTimestampRange(filteredSegment.getSegmentNo()));
      if (refreshNeeded) {
        toBeCleanedSegments.add(filteredSegment);
      }
    }
    // Clean segments if refresh is needed
    for (Segment segment : filteredSegmentToAccess) {
      if (DataMapStoreManager.getInstance().getTableSegmentRefresher(carbonTable)
          .isRefreshNeeded(segment.getSegmentNo())) {
        toBeCleanedSegments.add(segment);
      }
    }
    if (toBeCleanedSegments.size() > 0) {
      DataMapStoreManager.getInstance()
          .clearInvalidSegments(getOrCreateCarbonTable(job.getConfiguration()),
              toBeCleanedSegments);
    }
  }

  /**
   * Below method will be used to get the filter segments when query is fired on pre Aggregate
   * and main table in case of streaming.
   * For Pre Aggregate rules it will set all the valid segments for both streaming and
   * and normal for fact table, so if any handoff happened in between it will
   * select only new hand off segments segments for fact.
   * @param job
   * @param validSegments
   * @param segmentsToAccess
   * @return
   */
  private List<Segment> getFilteredNormalSegments(JobContext job, List<Segment> validSegments,
      Segment[] segmentsToAccess) {
    List<Segment> segmentToAccessSet = Arrays.asList(segmentsToAccess);
    List<Segment> filteredSegment = new ArrayList<>();
    for (Segment seg : validSegments) {
      if (!segmentToAccessSet.contains(seg)) {
        filteredSegment.add(seg);
      }
    }
    return filteredSegment;
  }

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
          Expression filter = getFilterPredicates(job.getConfiguration());
          if (filter != null) {
            carbonTable.processFilterExpression(filter, null, null);
            filterResolverIntf =
                CarbonTable.resolveFilter(filter, carbonTable.getAbsoluteTableIdentifier());
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
            splits.add(
                makeSplit(streamFile.getSegmentNo(), path, length - bytesRemaining,
                    splitSize, blkLocations[blkIndex].getHosts(),
                    blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
            bytesRemaining -= splitSize;
          }
          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
            splits.add(makeSplit(streamFile.getSegmentNo(), path, length - bytesRemaining,
                bytesRemaining, blkLocations[blkIndex].getHosts(),
                blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
          }
        }
      }
    }
    return splits;
  }

  protected FileSplit makeSplit(String segmentId, Path file, long start, long length,
      String[] hosts, FileFormat fileFormat) {
    return new CarbonInputSplit(segmentId, file, start, length, hosts, fileFormat);
  }


  protected FileSplit makeSplit(String segmentId, Path file, long start, long length,
      String[] hosts, String[] inMemoryHosts, FileFormat fileFormat) {
    return new CarbonInputSplit(segmentId, file, start, length, hosts, inMemoryHosts, fileFormat);
  }

  /**
   * Read data in one segment. For alter table partition statement
   * @param job
   * @param targetSegment
   * @param oldPartitionIdList  get old partitionId before partitionInfo was changed
   * @return
   */
  public List<InputSplit> getSplitsOfOneSegment(JobContext job, String targetSegment,
      List<Integer> oldPartitionIdList, PartitionInfo partitionInfo) {
    List<Segment> invalidSegments = new ArrayList<>();
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();


    try {
      carbonTable = getOrCreateCarbonTable(job.getConfiguration());
      ReadCommittedScope readCommittedScope =
          getReadCommitted(job, carbonTable.getAbsoluteTableIdentifier());
      this.readCommittedScope = readCommittedScope;

      List<Segment> segmentList = new ArrayList<>();
      Segment segment = Segment.getSegment(targetSegment, carbonTable.getTablePath());
      segmentList.add(
          new Segment(segment.getSegmentNo(), segment.getSegmentFileName(), readCommittedScope));
      setSegmentsToAccess(job.getConfiguration(), segmentList);

      // process and resolve the expression
      Expression filter = getFilterPredicates(job.getConfiguration());
      CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
      // this will be null in case of corrupt schema file.
      if (null == carbonTable) {
        throw new IOException("Missing/Corrupt schema file for table.");
      }
      carbonTable.processFilterExpression(filter, null, null);
      // prune partitions for filter query on partition table
      String partitionIds = job.getConfiguration().get(ALTER_PARTITION_ID);
      // matchedPartitions records partitionIndex, not partitionId
      BitSet matchedPartitions = null;
      if (partitionInfo != null) {
        matchedPartitions =
            setMatchedPartitions(partitionIds, filter, partitionInfo, oldPartitionIdList);
        if (matchedPartitions != null) {
          if (matchedPartitions.cardinality() == 0) {
            return new ArrayList<InputSplit>();
          } else if (matchedPartitions.cardinality() == partitionInfo.getNumPartitions()) {
            matchedPartitions = null;
          }
        }
      }

      // do block filtering and get split
      List<InputSplit> splits = getSplits(job, filter, segmentList, matchedPartitions,
          partitionInfo, oldPartitionIdList, new SegmentUpdateStatusManager(carbonTable));
      // pass the invalid segment to task side in order to remove index entry in task side
      if (invalidSegments.size() > 0) {
        for (InputSplit split : splits) {
          ((CarbonInputSplit) split).setInvalidSegments(invalidSegments);
          ((CarbonInputSplit) split).setInvalidTimestampRange(invalidTimestampsList);
        }
      }
      return splits;
    } catch (IOException e) {
      throw new RuntimeException("Can't get splits of the target segment ", e);
    }
  }

  /**
   * set the matched partition indices into a BitSet
   * @param partitionIds  from alter table command, for normal query, it's null
   * @param filter   from query
   * @param partitionInfo
   * @param oldPartitionIdList  only used in alter table command
   * @return
   */
  private BitSet setMatchedPartitions(String partitionIds, Expression filter,
      PartitionInfo partitionInfo, List<Integer> oldPartitionIdList) {
    BitSet matchedPartitions = null;
    if (null != partitionIds) {
      String[] partList = partitionIds.replace("[", "").replace("]", "").split(",");
      // partList[0] -> use the first element to initiate BitSet, will auto expand later
      matchedPartitions = new BitSet(Integer.parseInt(partList[0].trim()));
      for (String partitionId : partList) {
        Integer index = oldPartitionIdList.indexOf(Integer.parseInt(partitionId.trim()));
        matchedPartitions.set(index);
      }
    } else {
      if (null != filter) {
        matchedPartitions =
            new FilterExpressionProcessor().getFilteredPartitions(filter, partitionInfo);
      }
    }
    return matchedPartitions;
  }
  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, Expression expression,
      List<Segment> validSegments, BitSet matchedPartitions, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList, SegmentUpdateStatusManager updateStatusManager)
      throws IOException {

    numSegments = validSegments.size();
    List<InputSplit> result = new LinkedList<InputSplit>();
    UpdateVO invalidBlockVOForSegmentId = null;
    Boolean isIUDTable = false;

    isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    // for each segment fetch blocks matching filter in Driver BTree
    List<org.apache.carbondata.hadoop.CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, carbonTable, expression, matchedPartitions,
            validSegments, partitionInfo, oldPartitionIdList);
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
            .isInvalidTableBlock(inputSplit.getSegmentId(), inputSplit.getPath().toString(),
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
      List<PartitionSpec> partitions) throws IOException {
    // Normal query flow goes to CarbonInputFormat#getPrunedBlocklets and initialize the
    // pruning info for table we queried. But here count star query without filter uses a different
    // query plan, and no pruning info is initialized. When it calls default data map to
    // prune(with a null filter), exception will occur during setting pruning info.
    // Considering no useful information about block/blocklet pruning for such query
    // (actually no pruning), so we disable explain collector here
    ExplainCollector.remove();

    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();
    TableDataMap blockletMap = DataMapStoreManager.getInstance().getDefaultDataMap(table);

    ReadCommittedScope readCommittedScope = getReadCommitted(job, identifier);
    LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();

    SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(
        table, loadMetadataDetails);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo allSegments =
        new SegmentStatusManager(identifier, readCommittedScope.getConfiguration())
            .getValidAndInvalidSegments(loadMetadataDetails, readCommittedScope);
    Map<String, Long> blockRowCountMapping = new HashMap<>();
    Map<String, Long> segmentAndBlockCountMapping = new HashMap<>();

    // TODO: currently only batch segment is supported, add support for streaming table
    List<Segment> filteredSegment =
        getFilteredSegment(job, allSegments.getValidSegments(), false, readCommittedScope);
    /* In the select * flow, getSplits() method was clearing the segmentMap if,
    segment needs refreshing. same thing need for select count(*) flow also.
    For NonTransactional table, one of the reason for a segment refresh is below scenario.
    SDK is written one set of files with UUID, with same UUID it can write again.
    So, latest files content should reflect the new count by refreshing the segment */
    List<Segment> toBeCleanedSegments = new ArrayList<>();
    for (Segment eachSegment : filteredSegment) {
      boolean refreshNeeded = DataMapStoreManager.getInstance()
          .getTableSegmentRefresher(getOrCreateCarbonTable(job.getConfiguration()))
          .isRefreshNeeded(eachSegment,
              updateStatusManager.getInvalidTimestampRange(eachSegment.getSegmentNo()));
      if (refreshNeeded) {
        toBeCleanedSegments.add(eachSegment);
      }
    }
    if (toBeCleanedSegments.size() > 0) {
      DataMapStoreManager.getInstance()
          .clearInvalidSegments(getOrCreateCarbonTable(job.getConfiguration()),
              toBeCleanedSegments);
    }
    List<ExtendedBlocklet> blocklets =
        blockletMap.prune(filteredSegment, (FilterResolverIntf) null, partitions);
    for (ExtendedBlocklet blocklet : blocklets) {
      String blockName = blocklet.getPath();
      blockName = CarbonTablePath.getCarbonDataFileName(blockName);
      blockName = blockName + CarbonTablePath.getCarbonDataExtension();

      long rowCount = blocklet.getDetailInfo().getRowCount();

      String segmentId = Segment.toSegment(blocklet.getSegmentId()).getSegmentNo();
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