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
package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.BlockletInfos;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.hadoop.util.BlockLevelTraverser;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.StringUtils;

/**
 * Carbon Input format class representing one carbon table
 */
public class CarbonInputFormat<T> extends FileInputFormat<Void, T> {

  //comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  private static final Log LOG = LogFactory.getLog(CarbonInputFormat.class);
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";

  /**
   * It is optional, if user does not set then it reads from store
   *
   * @param configuration
   * @param carbonTable
   * @throws IOException
   */
  public static void setCarbonTable(Configuration configuration, CarbonTable carbonTable)
      throws IOException {
    if (null != carbonTable) {
      configuration.set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
    }
  }

  public static CarbonTable getCarbonTable(Configuration configuration) throws IOException {
    String carbonTableStr = configuration.get(CARBON_TABLE);
    if (carbonTableStr == null) {
      populateCarbonTable(configuration);
      // read it from schema file in the store
      carbonTableStr = configuration.get(CARBON_TABLE);
      return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
    }
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
  }

  /**
   * this method will read the schema from the physical file and populate into CARBON_TABLE
   * @param configuration
   * @throws IOException
   */
  private static void populateCarbonTable(Configuration configuration) throws IOException {
    String dirs = configuration.get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    if (inputPaths.length == 0) {
      throw new InvalidPathException("No input paths specified in job");
    }
    AbsoluteTableIdentifier absoluteTableIdentifier =
        AbsoluteTableIdentifier.fromTablePath(inputPaths[0]);
    // read the schema file to get the absoluteTableIdentifier having the correct table id
    // persisted in the schema
    CarbonTable carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
    setCarbonTable(configuration, carbonTable);
  }

  public static void setTablePath(Configuration configuration, String tablePath)
      throws IOException {
    configuration.set(FileInputFormat.INPUT_DIR, tablePath);
  }

  /**
   * It sets unresolved filter expression.
   *
   * @param configuration
   * @param filterExpression
   */
  public static void setFilterPredicates(Configuration configuration, Expression filterExpression) {
    if (filterExpression == null) {
      return;
    }
    try {
      String filterString = ObjectSerializationUtil.convertObjectToString(filterExpression);
      configuration.set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting filter expression to Job", e);
    }
  }

  public static void setColumnProjection(Configuration configuration, CarbonProjection projection) {
    if (projection == null || projection.isEmpty()) {
      return;
    }
    String[] allColumns = projection.getAllColumns();
    StringBuilder builder = new StringBuilder();
    for (String column : allColumns) {
      builder.append(column).append(",");
    }
    String columnString = builder.toString();
    columnString = columnString.substring(0, columnString.length() - 1);
    configuration.set(COLUMN_PROJECTION, columnString);
  }

  public static String getColumnProjection(Configuration configuration) {
    return configuration.get(COLUMN_PROJECTION);
  }

  public static void setCarbonReadSupport(Configuration configuration,
      Class<? extends CarbonReadSupport> readSupportClass) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  private static CarbonTablePath getTablePath(AbsoluteTableIdentifier absIdentifier) {
    return CarbonStorePath.getCarbonTablePath(absIdentifier);
  }

  /**
   * Set list of segments to access
   */
  public static void setSegmentsToAccess(Configuration configuration, List<String> validSegments) {
    configuration
        .set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, CarbonUtil.getSegmentString(validSegments));
  }

  private static AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    return getCarbonTable(configuration).getAbsoluteTableIdentifier();
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
  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(job.getConfiguration());
    CacheClient cacheClient = new CacheClient(identifier.getStorePath());
    List<String> invalidSegments = new ArrayList<>();
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();

    // get all valid segments and set them into the configuration
    if (getSegmentsToAccess(job).length == 0) {
      SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
      SegmentStatusManager.ValidAndInvalidSegmentsInfo segments =
              segmentStatusManager.getValidAndInvalidSegments();
      SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(identifier);
      setSegmentsToAccess(job.getConfiguration(), segments.getValidSegments());
      if (segments.getValidSegments().size() == 0) {
        return new ArrayList<>(0);
      }

      // remove entry in the segment index if there are invalid segments
      invalidSegments.addAll(segments.getInvalidSegments());
      for (String invalidSegmentId : invalidSegments) {
        invalidTimestampsList.add(updateStatusManager.getInvalidTimestampRange(invalidSegmentId));
      }
      if (invalidSegments.size() > 0) {
        List<TableSegmentUniqueIdentifier> invalidSegmentsIds
            = new ArrayList<>(invalidSegments.size());
        for (String segId : invalidSegments) {
          invalidSegmentsIds.add(new TableSegmentUniqueIdentifier(identifier, segId));
        }
        cacheClient.getSegmentAccessClient().invalidateAll(invalidSegmentsIds);
      }
    }

    // process and resolve the expression
    Expression filter = getFilterPredicates(job.getConfiguration());
    CarbonTable carbonTable = getCarbonTable(job.getConfiguration());
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
    FilterResolverIntf filterInterface = CarbonInputFormatUtil.resolveFilter(filter, identifier);

    // do block filtering and get split
    List<InputSplit> splits = getSplits(job, filterInterface, cacheClient);
    cacheClient.close();
    // pass the invalid segment to task side in order to remove index entry in task side
    if (invalidSegments.size() > 0) {
      for (InputSplit split : splits) {
        ((CarbonInputSplit) split).setInvalidSegments(invalidSegments);
        ((CarbonInputSplit) split).setInvalidTimestampRange(invalidTimestampsList);
      }
    }
    return splits;
  }

  private List<InputSplit> getSplitsInternal(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    List<InputSplit> carbonSplits = new ArrayList<InputSplit>(splits.size());
    // identify table blocks
    for (InputSplit inputSplit : splits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      String segmentId = CarbonTablePath.DataPathUtil.getSegmentId(fileSplit.getPath().toString());
      if (segmentId.equals(CarbonCommonConstants.INVALID_SEGMENT_ID)) {
        continue;
      }
      carbonSplits.add(CarbonInputSplit.from(segmentId, fileSplit,
          ColumnarFormatVersion.valueOf(
              CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION)));
    }
    return carbonSplits;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver,
      CacheClient cacheClient) throws IOException {

    List<InputSplit> result = new LinkedList<InputSplit>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier absoluteTableIdentifier =
            getCarbonTable(job.getConfiguration()).getAbsoluteTableIdentifier();
    SegmentUpdateStatusManager updateStatusManager =
            new SegmentUpdateStatusManager(absoluteTableIdentifier);
    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getSegmentsToAccess(job)) {
      List<DataRefNode> dataRefNodes =
          getDataBlocksOfSegment(job, filterExpressionProcessor, absoluteTableIdentifier,
              filterResolver, segmentNo, cacheClient, updateStatusManager);
      for (DataRefNode dataRefNode : dataRefNodes) {
        BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
        TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
        if (CarbonUtil.isInvalidTableBlock(tableBlockInfo,
            updateStatusManager.getInvalidTimestampRange(tableBlockInfo.getSegmentId()),
            updateStatusManager)) {
          continue;
        }
        result.add(new CarbonInputSplit(segmentNo, new Path(tableBlockInfo.getFilePath()),
            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
            tableBlockInfo.getLocations(), tableBlockInfo.getBlockletInfos().getNoOfBlockLets(),
            tableBlockInfo.getVersion()));
      }
    }
    return result;
  }

  protected Expression getFilterPredicates(Configuration configuration) {
    try {
      String filterExprString = configuration.get(FILTER_PREDICATE);
      if (filterExprString == null) {
        return null;
      }
      Object filter = ObjectSerializationUtil.convertStringToObject(filterExprString);
      return (Expression) filter;
    } catch (IOException e) {
      throw new RuntimeException("Error while reading filter expression", e);
    }
  }

  /**
   * get data blocks of given segment
   */
  private List<DataRefNode> getDataBlocksOfSegment(JobContext job,
      FilterExpressionProcessor filterExpressionProcessor,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver,
      String segmentId, CacheClient cacheClient, SegmentUpdateStatusManager updateStatusManager)
      throws IOException {
    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap =
        getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentId, cacheClient,
            updateStatusManager);

    List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

    if (null != segmentIndexMap) {
      // build result
      for (AbstractIndex abstractIndex : segmentIndexMap.values()) {
        List<DataRefNode> filterredBlocks;
        // if no filter is given get all blocks from Btree Index
        if (null == resolver) {
          filterredBlocks = getDataBlocksOfIndex(abstractIndex);
        } else {
          // apply filter and get matching blocks
          filterredBlocks = filterExpressionProcessor.getFilterredBlocks(
                  abstractIndex.getDataRefNode(),
                  resolver,
                  abstractIndex,
                  absoluteTableIdentifier
          );
        }
        resultFilterredBlocks.addAll(filterredBlocks);
      }
    }
    statistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
    recorder.recordStatisticsForDriver(statistic, job.getConfiguration().get("query.id"));
    return resultFilterredBlocks;
  }

  /**
   * Below method will be used to get the table block info
   *
   * @param job       job context
   * @param segmentId number of segment id
   * @return list of table block
   * @throws IOException
   */
  private List<TableBlockInfo> getTableBlockInfo(JobContext job,
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier,
      Set<SegmentTaskIndexStore.TaskBucketHolder> taskKeys, UpdateVO updateDetails,
      SegmentUpdateStatusManager updateStatusManager, String segmentId)
    throws IOException {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<TableBlockInfo>();

    // get file location of all files of given segment
    JobContext newJob =
        new JobContextImpl(new Configuration(job.getConfiguration()), job.getJobID());
    newJob.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS,
        tableSegmentUniqueIdentifier.getSegmentId() + "");

    // identify table blocks
    for (InputSplit inputSplit : getSplitsInternal(newJob)) {
      CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
      // if blockname and update block name is same then cmpare  its time stamp with
      // tableSegmentUniqueIdentifiertimestamp if time stamp is greater
      // then add as TableInfo object.
      if (isValidBlockBasedOnUpdateDetails(taskKeys, carbonInputSplit, updateDetails,
          updateStatusManager, segmentId)) {
        BlockletInfos blockletInfos = new BlockletInfos(carbonInputSplit.getNumberOfBlocklets(), 0,
            carbonInputSplit.getNumberOfBlocklets());
        tableBlockInfoList.add(
            new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                tableSegmentUniqueIdentifier.getSegmentId(), carbonInputSplit.getLocations(),
                carbonInputSplit.getLength(), blockletInfos, carbonInputSplit.getVersion(),
                carbonInputSplit.getBlockStorageIdMap()));
      }
    }
    return tableBlockInfoList;
  }

  private boolean isValidBlockBasedOnUpdateDetails(
      Set<SegmentTaskIndexStore.TaskBucketHolder> taskKeys, CarbonInputSplit carbonInputSplit,
      UpdateVO updateDetails, SegmentUpdateStatusManager updateStatusManager, String segmentId) {
    String taskID = null;
    if (null != carbonInputSplit) {
      if (!updateStatusManager.isBlockValid(segmentId, carbonInputSplit.getPath().getName())) {
        return false;
      }

      if (null == taskKeys) {
        return true;
      }

      taskID = CarbonTablePath.DataFileUtil.getTaskNo(carbonInputSplit.getPath().getName());
      String bucketNo =
          CarbonTablePath.DataFileUtil.getBucketNo(carbonInputSplit.getPath().getName());

      SegmentTaskIndexStore.TaskBucketHolder taskBucketHolder =
          new SegmentTaskIndexStore.TaskBucketHolder(taskID, bucketNo);

      String blockTimestamp = carbonInputSplit.getPath().getName()
          .substring(carbonInputSplit.getPath().getName().lastIndexOf('-') + 1,
              carbonInputSplit.getPath().getName().lastIndexOf('.'));
      if (!(updateDetails.getUpdateDeltaStartTimestamp() != null
          && Long.parseLong(blockTimestamp) < updateDetails.getUpdateDeltaStartTimestamp())) {
        if (!taskKeys.contains(taskBucketHolder)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * It returns index for each task file.
   * @param job
   * @param absoluteTableIdentifier
   * @param segmentId
   * @return
   * @throws IOException
   */
  private Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> getSegmentAbstractIndexs(
      JobContext job, AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId,
      CacheClient cacheClient, SegmentUpdateStatusManager updateStatusManager) throws IOException {
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap = null;
    SegmentTaskIndexWrapper segmentTaskIndexWrapper = null;
    boolean isSegmentUpdated = false;
    Set<SegmentTaskIndexStore.TaskBucketHolder> taskKeys = null;
    TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
        new TableSegmentUniqueIdentifier(absoluteTableIdentifier, segmentId);
    segmentTaskIndexWrapper =
        cacheClient.getSegmentAccessClient().getIfPresent(tableSegmentUniqueIdentifier);
    UpdateVO updateDetails = updateStatusManager.getInvalidTimestampRange(segmentId);
    if (null != segmentTaskIndexWrapper) {
      segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
      if (isSegmentUpdate(segmentTaskIndexWrapper, updateDetails)) {
        taskKeys = segmentIndexMap.keySet();
        isSegmentUpdated = true;
      }
    }
    // if segment tree is not loaded, load the segment tree
    if (segmentIndexMap == null || isSegmentUpdated) {
      // if the segment is updated only the updated blocks TableInfo instance has to be
      // retrieved. the same will be filtered based on taskKeys , if the task is same
      // for the block then dont add it since already its btree is loaded.
      List<TableBlockInfo> tableBlockInfoList =
          getTableBlockInfo(job, tableSegmentUniqueIdentifier, taskKeys,
              updateStatusManager.getInvalidTimestampRange(segmentId), updateStatusManager,
              segmentId);
      if (!tableBlockInfoList.isEmpty()) {
        // getFileStatusOfSegments(job, new int[]{ segmentId }, fileStatusList);
        Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
        segmentToTableBlocksInfos.put(segmentId, tableBlockInfoList);
        // get Btree blocks for given segment
        tableSegmentUniqueIdentifier.setSegmentToTableBlocksInfos(segmentToTableBlocksInfos);
        tableSegmentUniqueIdentifier.setIsSegmentUpdated(isSegmentUpdated);
        segmentTaskIndexWrapper =
            cacheClient.getSegmentAccessClient().get(tableSegmentUniqueIdentifier);
        segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
      }
    }
    return segmentIndexMap;
  }

  /**
   * Get the row count of the Block and mapping of segment and Block count.
   * @param job
   * @param absoluteTableIdentifier
   * @return
   * @throws IOException
   * @throws KeyGenException
   */
  public BlockMappingVO getBlockRowCount(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException, KeyGenException {
    CacheClient cacheClient = new CacheClient(absoluteTableIdentifier.getStorePath());
    try {
      SegmentUpdateStatusManager updateStatusManager =
          new SegmentUpdateStatusManager(absoluteTableIdentifier);
      SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegments =
          new SegmentStatusManager(absoluteTableIdentifier).getValidAndInvalidSegments();
      Map<String, Long> blockRowCountMapping =
          new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      Map<String, Long> segmentAndBlockCountMapping =
          new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

      for (String eachValidSeg : validAndInvalidSegments.getValidSegments()) {
        long countOfBlocksInSeg = 0;
        Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskAbstractIndexMap =
            getSegmentAbstractIndexs(job, absoluteTableIdentifier, eachValidSeg, cacheClient,
                updateStatusManager);
        for (Map.Entry<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskMap :
            taskAbstractIndexMap
            .entrySet()) {
          AbstractIndex taskAbstractIndex = taskMap.getValue();
          countOfBlocksInSeg += new BlockLevelTraverser()
              .getBlockRowMapping(taskAbstractIndex, blockRowCountMapping, eachValidSeg,
                  updateStatusManager);
        }
        segmentAndBlockCountMapping.put(eachValidSeg, countOfBlocksInSeg);
      }
      return new BlockMappingVO(blockRowCountMapping, segmentAndBlockCountMapping);
    } finally {
      cacheClient.close();
    }
  }


  private boolean isSegmentUpdate(SegmentTaskIndexWrapper segmentTaskIndexWrapper,
      UpdateVO updateDetails) {
    if (null != updateDetails.getLatestUpdateTimestamp()
            && updateDetails.getLatestUpdateTimestamp() > segmentTaskIndexWrapper
            .getRefreshedTimeStamp()) {
      return true;
    }
    return false;
  }

  /**
   * get data blocks of given btree
   */
  private List<DataRefNode> getDataBlocksOfIndex(AbstractIndex abstractIndex) {
    List<DataRefNode> blocks = new LinkedList<DataRefNode>();
    SegmentProperties segmentProperties = abstractIndex.getSegmentProperties();

    try {
      IndexKey startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
      IndexKey endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);

      // Add all blocks of btree into result
      DataRefNodeFinder blockFinder =
          new BTreeDataRefNodeFinder(segmentProperties.getEachDimColumnValueSize());
      DataRefNode startBlock =
          blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), startIndexKey);
      DataRefNode endBlock =
          blockFinder.findLastDataBlock(abstractIndex.getDataRefNode(), endIndexKey);
      while (startBlock != endBlock) {
        blocks.add(startBlock);
        startBlock = startBlock.getNextDataRefNode();
      }
      blocks.add(endBlock);

    } catch (KeyGenException e) {
      LOG.error("Could not generate start key", e);
    }
    return blocks;
  }

  @Override public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    QueryModel queryModel = getQueryModel(inputSplit, taskAttemptContext);
    CarbonReadSupport<T> readSupport = getReadSupportClass(configuration);
    return new CarbonRecordReader<T>(queryModel, readSupport);
  }

  public QueryModel getQueryModel(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    CarbonTable carbonTable = getCarbonTable(configuration);
    // getting the table absoluteTableIdentifier from the carbonTable
    // to avoid unnecessary deserialization
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

    // query plan includes projection column
    String projection = getColumnProjection(configuration);
    CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(carbonTable, projection);
    QueryModel queryModel = QueryModel.createModel(identifier, queryPlan, carbonTable);

    // set the filter to the query model in order to filter blocklet before scan
    Expression filter = getFilterPredicates(configuration);
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
    FilterResolverIntf filterIntf =  CarbonInputFormatUtil.resolveFilter(filter, identifier);
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

  public CarbonReadSupport<T> getReadSupportClass(Configuration configuration) {
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
        LOG.error("Class " + readSupportClass + "not found", ex);
      } catch (Exception ex) {
        LOG.error("Error while creating " + readSupportClass, ex);
      }
    } else {
      readSupport = new DictionaryDecodeReadSupport<>();
    }
    return readSupport;
  }

  @Override protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    String[] segmentsToConsider = getSegmentsToAccess(job);
    if (segmentsToConsider.length == 0) {
      throw new IOException("No segments found");
    }

    getFileStatusOfSegments(job, segmentsToConsider, result);
    return result;
  }

  @Override protected boolean isSplitable(JobContext context, Path filename) {
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

  private void getFileStatusOfSegments(JobContext job, String[] segmentsToConsider,
      List<FileStatus> result) throws IOException {
    String[] partitionsToConsider = getValidPartitions(job);
    if (partitionsToConsider.length == 0) {
      throw new IOException("No partitions/data found");
    }

    PathFilter inputFilter = getDataFileFilter();
    AbsoluteTableIdentifier absIdentifier = getAbsoluteTableIdentifier(job.getConfiguration());
    CarbonTablePath tablePath = getTablePath(absIdentifier);

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { tablePath },
        job.getConfiguration());

    //get all data files of valid partitions and segments
    for (int i = 0; i < partitionsToConsider.length; ++i) {
      String partition = partitionsToConsider[i];

      for (int j = 0; j < segmentsToConsider.length; ++j) {
        String segmentId = segmentsToConsider[j];
        String dataDirectoryPath = absIdentifier
            .appendWithLocalPrefix(tablePath.getCarbonDataDirectoryPath(partition, segmentId));
        Path segmentPath = new Path(dataDirectoryPath);
        FileSystem fs = segmentPath.getFileSystem(job.getConfiguration());
        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(segmentPath);
        while (iter.hasNext()) {
          LocatedFileStatus stat = iter.next();
          if (inputFilter.accept(stat.getPath())) {
            if (stat.isDirectory()) {
              addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
              result.add(stat);
            }
          }
        }
      }
    }
  }

  /**
   * @return the PathFilter for Fact Files.
   */
  private PathFilter getDataFileFilter() {
    return new CarbonPathFilter(getUpdateExtension());
  }

  /**
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String getUpdateExtension() {
    // TODO: required to modify when supporting update, mostly will be update timestamp
    return "update";
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
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String[] getValidPartitions(JobContext job) {
    //TODO: has to Identify partitions by partition pruning
    return new String[] { "0" };
  }

}
