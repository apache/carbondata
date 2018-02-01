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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 * Input format of CarbonData file.
 *
 * @param <T>
 */
public class CarbonTableInputFormat<T> extends CarbonInputFormat<T> {

  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  private static final String VALIDATE_INPUT_SEGMENT_IDs =
      "mapreduce.input.carboninputformat.validsegments";
  // comma separated list of input files
  public static final String INPUT_FILES = "mapreduce.input.carboninputformat.files";
  private static final String ALTER_PARTITION_ID = "mapreduce.input.carboninputformat.partitionid";
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  private static final String CARBON_CONVERTER = "mapreduce.input.carboninputformat.converter";
  private static final String DATA_MAP_DSTR = "mapreduce.input.carboninputformat.datamapdstr";
  private static final String PARTITIONS_TO_PRUNE =
      "mapreduce.input.carboninputformat.partitions.to.prune";
  public static final String UPADTE_T =
      "mapreduce.input.carboninputformat.partitions.to.prune";

  // For map reduce reflection
  protected CarbonTableInputFormat() { }

  // Use [[CarbonInputFormat.newTableFormat]] to create new instance
  protected CarbonTableInputFormat(Configuration configuration,
      AbsoluteTableIdentifier identifier) {
    super();
    CarbonTableInputFormat.setTableName(configuration, identifier.getTableName());
    CarbonTableInputFormat.setDatabaseName(configuration, identifier.getDatabaseName());
    CarbonTableInputFormat.setTablePath(configuration, identifier.getTablePath());
  }

  @Override
  TableInfo getTableInfo(Configuration configuration) throws IOException {
    TableInfo tableInfo = super.getTableInfo(configuration);
    if (tableInfo == null) {
      AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(configuration);
      tableInfo = SchemaReader.getTableInfo(identifier);
    }
    return tableInfo;
  }

  private static void setTablePath(Configuration configuration, String tablePath) {
    configuration.set(FileInputFormat.INPUT_DIR, tablePath);
  }

  public static void setPartitionIdList(Configuration configuration, List<String> partitionIds) {
    configuration.set(ALTER_PARTITION_ID, partitionIds.toString());
  }

  public static void setDataMapJob(Configuration configuration, DataMapJob dataMapJob)
      throws IOException {
    if (dataMapJob != null) {
      String toString = ObjectSerializationUtil.convertObjectToString(dataMapJob);
      configuration.set(DATA_MAP_DSTR, toString);
    }
  }

  private static DataMapJob getDataMapJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(DATA_MAP_DSTR);
    if (jobString != null) {
      return (DataMapJob) ObjectSerializationUtil.convertStringToObject(jobString);
    }
    return null;
  }

  // TODO: move this to proper class
  public static void setUpdateCache(InputSplit inputSplit, QueryModel queryModel) {
    // update the file level index store if there are invalid segment
    if (inputSplit instanceof CarbonMultiBlockSplit) {
      CarbonMultiBlockSplit split = (CarbonMultiBlockSplit) inputSplit;
      List<String> invalidSegments = split.getAllSplits().get(0).getInvalidSegments();
      if (invalidSegments.size() > 0) {
        queryModel.setInvalidSegmentIds(invalidSegments);
      }
      List<UpdateVO> invalidTimestampRangeList =
          split.getAllSplits().get(0).getInvalidTimestampRange();
      if ((null != invalidTimestampRangeList) && (invalidTimestampRangeList.size() > 0)) {
        queryModel.setInvalidBlockForSegmentId(invalidTimestampRangeList);
      }
    }
  }

  private CarbonTable getCarbonTable(Configuration configuration)
      throws IOException {
    // carbon table should be created either from deserialized table info (in configuration)
    // or by reading schema in HDFS (schema saved in HDFS)
    CarbonTable carbonTable = buildCarbonTable(configuration);
    if (carbonTable == null) {
      carbonTable = SchemaReader.readCarbonTableFromStore(
          getAbsoluteTableIdentifier(configuration));
    }
    return carbonTable;
  }

  /**
   * Set list of segments to access
   */
  public static void setSegmentsToAccess(Configuration configuration, List<String> validSegments) {
    configuration.set(INPUT_SEGMENT_NUMBERS, CarbonUtil.convertToString(validSegments));
  }

  /**
   * Set `CARBON_INPUT_SEGMENTS` from property to configuration
   */
  public void setQuerySegment(Configuration conf, AbsoluteTableIdentifier identifier) {
    String dbName = identifier.getCarbonTableIdentifier().getDatabaseName().toLowerCase();
    String tbName = identifier.getCarbonTableIdentifier().getTableName().toLowerCase();
    String segmentNumbersFromProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tbName, "*");
    if (!segmentNumbersFromProperty.trim().equals("*")) {
      CarbonTableInputFormat
          .setSegmentsToAccess(conf, Arrays.asList(segmentNumbersFromProperty.split(",")));
    }
  }

  /**
   * set list of segment to access
   */
  public static void setValidateSegmentsToAccess(Configuration configuration, Boolean validate) {
    configuration.set(CarbonTableInputFormat.VALIDATE_INPUT_SEGMENT_IDs, validate.toString());
  }

  /**
   * get list of segment to access
   */
  private static boolean getValidateSegmentsToAccess(Configuration configuration) {
    return configuration.get(CarbonTableInputFormat.VALIDATE_INPUT_SEGMENT_IDs, "true")
        .equalsIgnoreCase("true");
  }

  /**
   * set list of partitions to prune
   */
  public static void setPartitionsToPrune(Configuration configuration, List<String> partitions) {
    if (partitions == null) {
      return;
    }
    try {
      String partitionString = ObjectSerializationUtil.convertObjectToString(partitions);
      configuration.set(PARTITIONS_TO_PRUNE, partitionString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting patition information to Job", e);
    }
  }

  /**
   * get list of partitions to prune
   */
  private static List<String> getPartitionsToPrune(Configuration configuration) throws IOException {
    String partitionString = configuration.get(PARTITIONS_TO_PRUNE);
    if (partitionString != null) {
      return (List<String>) ObjectSerializationUtil.convertStringToObject(partitionString);
    }
    return null;
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
    SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(identifier);
    CarbonTable carbonTable = getCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    TableDataMap blockletMap =
        DataMapStoreManager.getInstance().getDataMap(identifier, BlockletDataMap.NAME,
            BlockletDataMapFactory.class.getName());
    List<String> invalidSegments = new ArrayList<>();
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();
    List<String> streamSegments = null;

    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      // get all valid segments and set them into the configuration
      SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
      SegmentStatusManager.ValidAndInvalidSegmentsInfo segments =
          segmentStatusManager.getValidAndInvalidSegments();
      List<String> validSegments = segments.getValidSegments();
      streamSegments = segments.getStreamSegments();
      if (validSegments.size() == 0) {
        return getSplitsOfStreaming(job, identifier, streamSegments);
      }

      List<String> filteredSegmentToAccess = getFilteredSegment(job, validSegments);
      if (filteredSegmentToAccess.size() == 0) {
        return new ArrayList<>(0);
      } else {
        setSegmentsToAccess(job.getConfiguration(), filteredSegmentToAccess);
      }
      // remove entry in the segment index if there are invalid segments
      invalidSegments.addAll(segments.getInvalidSegments());
      for (String invalidSegmentId : invalidSegments) {
        invalidTimestampsList.add(updateStatusManager.getInvalidTimestampRange(invalidSegmentId));
      }
      if (invalidSegments.size() > 0) {
        blockletMap.clear(invalidSegments);
      }
    }

    // get updated filtered list
    List<String> filteredSegmentToAccess = Arrays.asList(getSegmentsToAccess(job));
    // Clean the updated segments from memory if the update happens on segments
    List<String> toBeCleanedSegments = new ArrayList<>();
    for (SegmentUpdateDetails segmentUpdateDetail : updateStatusManager
        .getUpdateStatusDetails()) {
      boolean refreshNeeded =
          DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
              .isRefreshNeeded(segmentUpdateDetail.getSegmentName(), updateStatusManager);
      if (refreshNeeded) {
        toBeCleanedSegments.add(segmentUpdateDetail.getSegmentName());
      }
    }
    // Clean segments if refresh is needed
    for (String segment : filteredSegmentToAccess) {
      if (DataMapStoreManager.getInstance().getTableSegmentRefresher(identifier)
          .isRefreshNeeded(segment)) {
        toBeCleanedSegments.add(segment);
      }
    }
    blockletMap.clear(toBeCleanedSegments);

    // process and resolve the expression
    Expression filter = getFilterPredicates(job.getConfiguration());
    // this will be null in case of corrupt schema file.
    PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());
    QueryModel.processFilterExpression(carbonTable, filter, null, null);

    // prune partitions for filter query on partition table
    BitSet matchedPartitions = null;
    if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
      matchedPartitions = setMatchedPartitions(null, filter, partitionInfo, null);
      if (matchedPartitions != null) {
        if (matchedPartitions.cardinality() == 0) {
          return new ArrayList<InputSplit>();
        } else if (matchedPartitions.cardinality() == partitionInfo.getNumPartitions()) {
          matchedPartitions = null;
        }
      }
    }

    FilterResolverIntf filterInterface = carbonTable.resolveFilter(filter);

    // do block filtering and get split
    List<InputSplit> splits =
        getSplits(job, filterInterface, filteredSegmentToAccess, matchedPartitions, partitionInfo,
            null);
    // pass the invalid segment to task side in order to remove index entry in task side
    if (invalidSegments.size() > 0) {
      for (InputSplit split : splits) {
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split).setInvalidSegments(invalidSegments);
        ((org.apache.carbondata.hadoop.CarbonInputSplit) split)
            .setInvalidTimestampRange(invalidTimestampsList);
      }
    }

    // add all splits of streaming
    List<InputSplit> splitsOfStreaming = getSplitsOfStreaming(job, identifier, streamSegments);
    if (!splitsOfStreaming.isEmpty()) {
      splits.addAll(splitsOfStreaming);
    }
    return splits;
  }

  /**
   * Return segment list after filtering out valid segments and segments set by user by
   * `INPUT_SEGMENT_NUMBERS` in job configuration
   */
  private List<String> getFilteredSegment(JobContext job, List<String> validSegments) {
    String[] segmentsToAccess = getSegmentsToAccess(job);
    Set<String> segmentToAccessSet = new HashSet<>(Arrays.asList(segmentsToAccess));
    List<String> filteredSegmentToAccess = new ArrayList<>();
    if (segmentsToAccess.length == 0 || segmentsToAccess[0].equalsIgnoreCase("*")) {
      filteredSegmentToAccess.addAll(validSegments);
    } else {
      for (String validSegment : validSegments) {
        if (segmentToAccessSet.contains(validSegment)) {
          filteredSegmentToAccess.add(validSegment);
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentToAccessSet)) {
        List<String> filteredSegmentToAccessTemp = new ArrayList<>(filteredSegmentToAccess);
        filteredSegmentToAccessTemp.removeAll(segmentToAccessSet);
        LOGGER.info(
            "Segments ignored are : " + Arrays.toString(filteredSegmentToAccessTemp.toArray()));
      }
    }
    return filteredSegmentToAccess;
  }

  /**
   * use file list in .carbonindex file to get the split of streaming.
   */
  public List<InputSplit> getSplitsOfStreaming(JobContext job, AbsoluteTableIdentifier identifier,
      List<String> streamSegments) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (streamSegments != null && !streamSegments.isEmpty()) {

      long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
      long maxSize = getMaxSplitSize(job);
      for (String segmentId : streamSegments) {
        String segmentDir = CarbonTablePath.getSegmentPath(identifier.getTablePath(), segmentId);
        FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
        if (FileFactory.isFileExist(segmentDir, fileType)) {
          String indexName = CarbonTablePath.getCarbonStreamIndexFileName();
          String indexPath = segmentDir + File.separator + indexName;
          CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
          // index file exists
          if (index.exists()) {
            // data file exists
            CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
            try {
              // map block index
              indexReader.openThriftReader(indexPath);
              while (indexReader.hasNext()) {
                BlockIndex blockIndex = indexReader.readBlockIndexInfo();
                String filePath = segmentDir + File.separator + blockIndex.getFile_name();
                Path path = new Path(filePath);
                long length = blockIndex.getFile_size();
                if (length != 0) {
                  BlockLocation[] blkLocations;
                  FileSystem fs = FileFactory.getFileSystem(path);
                  FileStatus file = fs.getFileStatus(path);
                  blkLocations = fs.getFileBlockLocations(path, 0, length);
                  long blockSize = file.getBlockSize();
                  long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                  long bytesRemaining = length;
                  while (((double) bytesRemaining) / splitSize > 1.1) {
                    int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(segmentId, path, length - bytesRemaining, splitSize,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
                    bytesRemaining -= splitSize;
                  }
                  if (bytesRemaining != 0) {
                    int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(segmentId, path, length - bytesRemaining, bytesRemaining,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts(), FileFormat.ROW_V1));
                  }
                } else {
                  //Create empty hosts array for zero length files
                  splits.add(makeSplit(segmentId, path, 0, length, new String[0],
                      FileFormat.ROW_V1));
                }
              }
            } finally {
              indexReader.closeThriftReader();
            }
          }
        }
      }
    }
    return splits;
  }

  private FileSplit makeSplit(String segmentId, Path file, long start, long length, String[] hosts,
      FileFormat fileFormat) {
    return new CarbonInputSplit(segmentId, file, start, length, hosts, fileFormat);
  }

  private FileSplit makeSplit(String segmentId, Path file, long start, long length, String[] hosts,
      String[] inMemoryHosts, FileFormat fileFormat) {
    return new CarbonInputSplit(segmentId, file, start, length, hosts, inMemoryHosts, fileFormat);
  }

  /**
   * Read data in one segment. For alter table partition statement
   * @param job
   * @param targetSegment
   * @param oldPartitionIdList  get old partitionId before partitionInfo was changed
   * @return
   * @throws IOException
   */
  public List<InputSplit> getSplitsOfOneSegment(JobContext job, String targetSegment,
      List<Integer> oldPartitionIdList, PartitionInfo partitionInfo) {
    List<String> segmentList = new ArrayList<>();
    segmentList.add(targetSegment);
    setSegmentsToAccess(job.getConfiguration(), segmentList);
    try {

      // process and resolve the expression
      Expression filter = getFilterPredicates(job.getConfiguration());
      CarbonTable carbonTable = getCarbonTable(job.getConfiguration());
      // this will be null in case of corrupt schema file.
      if (null == carbonTable) {
        throw new IOException("Missing/Corrupt schema file for table.");
      }

      QueryModel.processFilterExpression(carbonTable, filter, null, null);

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

      FilterResolverIntf filterInterface = carbonTable.resolveFilter(filter);
      // do block filtering and get split
      List<InputSplit> splits = getSplits(job, filterInterface, segmentList, matchedPartitions,
          partitionInfo, oldPartitionIdList);
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
  private List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver,
      List<String> validSegments, BitSet matchedPartitions, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    List<InputSplit> result = new LinkedList<InputSplit>();
    UpdateVO invalidBlockVOForSegmentId = null;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        getCarbonTable(job.getConfiguration()).getAbsoluteTableIdentifier();
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);

    Boolean isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    // for each segment fetch blocks matching filter in Driver BTree
    List<org.apache.carbondata.hadoop.CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, absoluteTableIdentifier, filterResolver, matchedPartitions,
            validSegments, partitionInfo, oldPartitionIdList);
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
          deleteDeltaFilePath =
              updateStatusManager.getDeleteDeltaFilePath(inputSplit.getPath().toString());
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
   * get data blocks of given segment
   */
  private List<org.apache.carbondata.hadoop.CarbonInputSplit> getDataBlocksOfSegment(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver,
      BitSet matchedPartitions, List<String> segmentIds, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { new Path(absoluteTableIdentifier.getTablePath()) }, job.getConfiguration());

    TableDataMap blockletMap = DataMapStoreManager.getInstance()
        .getDataMap(absoluteTableIdentifier, BlockletDataMap.NAME,
            BlockletDataMapFactory.class.getName());
    DataMapJob dataMapJob = getDataMapJob(job.getConfiguration());
    List<String> partitionsToPrune = getPartitionsToPrune(job.getConfiguration());
    List<ExtendedBlocklet> prunedBlocklets;
    if (dataMapJob != null) {
      DistributableDataMapFormat datamapDstr =
          new DistributableDataMapFormat(absoluteTableIdentifier, BlockletDataMap.NAME,
              segmentIds, partitionsToPrune,
              BlockletDataMapFactory.class.getName());
      prunedBlocklets = dataMapJob.execute(datamapDstr, resolver);
    } else {
      prunedBlocklets = blockletMap.prune(segmentIds, resolver, partitionsToPrune);
    }

    List<org.apache.carbondata.hadoop.CarbonInputSplit> resultFilterredBlocks = new ArrayList<>();
    int partitionIndex = 0;
    List<Integer> partitionIdList = new ArrayList<>();
    if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
      partitionIdList = partitionInfo.getPartitionIds();
    }
    for (ExtendedBlocklet blocklet : prunedBlocklets) {
      long partitionId = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(
          CarbonTablePath.DataFileUtil.getTaskNo(blocklet.getPath()));

      // OldPartitionIdList is only used in alter table partition command because it change
      // partition info first and then read data.
      // For other normal query should use newest partitionIdList
      if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
        if (oldPartitionIdList != null) {
          partitionIndex = oldPartitionIdList.indexOf((int)partitionId);
        } else {
          partitionIndex = partitionIdList.indexOf((int)partitionId);
        }
      }
      if (partitionIndex != -1) {
        // matchedPartitions variable will be null in two cases as follows
        // 1. the table is not a partition table
        // 2. the table is a partition table, and all partitions are matched by query
        // for partition table, the task id of carbaondata file name is the partition id.
        // if this partition is not required, here will skip it.
        if (matchedPartitions == null || matchedPartitions.get(partitionIndex)) {
          CarbonInputSplit inputSplit = convertToCarbonInputSplit(blocklet);
          if (inputSplit != null) {
            resultFilterredBlocks.add(inputSplit);
          }
        }
      }
    }
    statistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
    recorder.recordStatisticsForDriver(statistic, job.getConfiguration().get("query.id"));
    return resultFilterredBlocks;
  }

  private CarbonInputSplit convertToCarbonInputSplit(ExtendedBlocklet blocklet) throws IOException {
    org.apache.carbondata.hadoop.CarbonInputSplit split =
        org.apache.carbondata.hadoop.CarbonInputSplit.from(blocklet.getSegmentId(),
            blocklet.getBlockletId(), new FileSplit(new Path(blocklet.getPath()), 0,
                blocklet.getLength(), blocklet.getLocations()),
            ColumnarFormatVersion.valueOf((short) blocklet.getDetailInfo().getVersionNumber()));
    split.setDetailInfo(blocklet.getDetailInfo());
    return split;
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    QueryModel queryModel = createQueryModel(inputSplit, taskAttemptContext);
    CarbonReadSupport<T> readSupport = getReadSupportClass(configuration);
    return new CarbonRecordReader<T>(queryModel, readSupport);
  }

  public QueryModel createQueryModel(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    CarbonTable carbonTable = getOrCreateCarbonTable(configuration);
    TableProvider tableProvider = new SingleTableProvider(carbonTable);

    // query plan includes projection column
    String[] projectionColumnNames = getColumnProjection(configuration);
    QueryModel queryModel = carbonTable.createQueryWithProjection(projectionColumnNames);
    queryModel.setConverter(getDataTypeConverter(configuration));

    // set the filter to the query model in order to filter blocklet before scan
    Expression filter = getFilterPredicates(configuration);
    boolean[] isFilterDimensions = new boolean[carbonTable.getDimensionOrdinalMax()];
    // getAllMeasures returns list of visible and invisible columns
    boolean[] isFilterMeasures =
        new boolean[carbonTable.getAllMeasures().size()];
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable, isFilterDimensions,
        isFilterMeasures);
    queryModel.setIsFilterDimensions(isFilterDimensions);
    queryModel.setIsFilterMeasures(isFilterMeasures);
    FilterResolverIntf filterIntf = CarbonInputFormatUtil
        .resolveFilter(filter, carbonTable.getAbsoluteTableIdentifier(), tableProvider);
    queryModel.setFilterExpressionResolverTree(filterIntf);

    // update the file level index store if there are invalid segment
    if (inputSplit instanceof CarbonMultiBlockSplit) {
      CarbonMultiBlockSplit split = (CarbonMultiBlockSplit) inputSplit;
      List<String> invalidSegments = split.getAllSplits().get(0).getInvalidSegments();
      if (invalidSegments.size() > 0) {
        queryModel.setInvalidSegmentIds(invalidSegments);
      }
      List<UpdateVO> invalidTimestampRangeList =
          split.getAllSplits().get(0).getInvalidTimestampRange();
      if ((null != invalidTimestampRangeList) && (invalidTimestampRangeList.size() > 0)) {
        queryModel.setInvalidBlockForSegmentId(invalidTimestampRangeList);
      }
    }
    return queryModel;
  }

  private CarbonReadSupport<T> getReadSupportClass(Configuration configuration) {
    String readSupportClass = configuration.get(CARBON_READ_SUPPORT);
    //By default it uses dictionary decoder read class
    CarbonReadSupport<T> readSupport = null;
    if (readSupportClass != null) {
      try {
        Class<?> myClass = Class.forName(readSupportClass);
        Constructor<?> constructor = myClass.getConstructors()[0];
        Object object = constructor.newInstance();
        if (object instanceof CarbonReadSupport) {
          readSupport = (CarbonReadSupport) object;
        }
      } catch (ClassNotFoundException ex) {
        LOGGER.error(ex, "Class " + readSupportClass + "not found");
      } catch (Exception ex) {
        LOGGER.error(ex, "Error while creating " + readSupportClass);
      }
    } else {
      readSupport = new DictionaryDecodeReadSupport<>();
    }
    return readSupport;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    try {
      // Don't split the file if it is local file system
      FileSystem fileSystem = filename.getFileSystem(context.getConfiguration());
      if (fileSystem instanceof LocalFileSystem) {
        return false;
      }
    } catch (Exception e) {
      return true;
    }
    return true;
  }

  /**
   * return valid segment to access
   */
  private String[] getSegmentsToAccess(JobContext job) {
    String segmentString = job.getConfiguration().get(INPUT_SEGMENT_NUMBERS, "");
    if (segmentString.trim().isEmpty()) {
      return new String[0];
    }
    return segmentString.split(",");
  }

  /**
   * Get the row count of the Block and mapping of segment and Block count.
   *
   * @param identifier
   * @return
   * @throws IOException
   */
  public BlockMappingVO getBlockRowCount(Job job, AbsoluteTableIdentifier identifier,
      List<String> partitions) throws IOException {
    TableDataMap blockletMap = DataMapStoreManager.getInstance()
        .getDataMap(identifier, BlockletDataMap.NAME, BlockletDataMapFactory.class.getName());
    SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(identifier);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo allSegments =
        new SegmentStatusManager(identifier).getValidAndInvalidSegments();
    Map<String, Long> blockRowCountMapping = new HashMap<>();
    Map<String, Long> segmentAndBlockCountMapping = new HashMap<>();

    // TODO: currently only batch segment is supported, add support for streaming table
    List<String> filteredSegment = getFilteredSegment(job, allSegments.getValidSegments());

    List<ExtendedBlocklet> blocklets = blockletMap.prune(filteredSegment, null, partitions);
    for (ExtendedBlocklet blocklet : blocklets) {
      String blockName = blocklet.getPath();
      blockName = CarbonTablePath.getCarbonDataFileName(blockName);
      blockName = blockName + CarbonTablePath.getCarbonDataExtension();

      long rowCount = blocklet.getDetailInfo().getRowCount();

      String key = CarbonUpdateUtil.getSegmentBlockNameKey(blocklet.getSegmentId(), blockName);

      // if block is invalid then dont add the count
      SegmentUpdateDetails details = updateStatusManager.getDetailsForABlock(key);

      if (null == details || !CarbonUpdateUtil.isBlockInvalid(details.getSegmentStatus())) {
        Long blockCount = blockRowCountMapping.get(key);
        if (blockCount == null) {
          blockCount = 0L;
          Long count = segmentAndBlockCountMapping.get(blocklet.getSegmentId());
          if (count == null) {
            count = 0L;
          }
          segmentAndBlockCountMapping.put(blocklet.getSegmentId(), count + 1);
        }
        blockCount += rowCount;
        blockRowCountMapping.put(key, blockCount);
      }
    }

    return new BlockMappingVO(blockRowCountMapping, segmentAndBlockCountMapping);
  }

}
