/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.carbon.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.BlockletInfos;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsConstants;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsRecorder;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodedReadSupportImpl;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.carbondata.lcm.status.SegmentStatusManager;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.scan.model.QueryModel;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.INVALID_SEGMENT_ID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
  private static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databasename";
  private static final String TABLE_NAME = "mapreduce.input.carboninputformat.tablename";
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  private static final String TABLE_ID = "mapreduce.input.carboninputformat.tableId";

  public static void setTableToAccess(Configuration configuration,
      CarbonTableIdentifier tableIdentifier) {
    configuration.set(CarbonInputFormat.DATABASE_NAME, tableIdentifier.getDatabaseName());
    configuration.set(CarbonInputFormat.TABLE_NAME, tableIdentifier.getTableName());
    configuration.set(CarbonInputFormat.TABLE_ID, tableIdentifier.getTableId());
  }

  /**
   * Get CarbonTableIdentifier from job configuration
   */
  public static CarbonTableIdentifier getTableToAccess(Configuration configuration) {
    String databaseName = configuration.get(CarbonInputFormat.DATABASE_NAME);
    String tableName = configuration.get(CarbonInputFormat.TABLE_NAME);
    String tableId = configuration.get(CarbonInputFormat.TABLE_ID);
    if (databaseName != null && tableName != null) {
      return new CarbonTableIdentifier(databaseName, tableName, tableId);
    }
    //TODO: better raise exception
    return null;
  }

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
      CarbonTable carbonTable = new SchemaReader()
          .readCarbonTableFromStore(getTablePath(configuration), getTableToAccess(configuration),
              getStorePathString(configuration));
      setCarbonTable(configuration, carbonTable);
      return carbonTable;
    }
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
  }

  /**
   * It sets unresolved filter expression.
   *
   * @param configuration
   * @param filterExpression
   */
  public static void setFilterPredicates(Configuration configuration, Expression filterExpression) {
    try {
      String filterString = ObjectSerializationUtil.convertObjectToString(filterExpression);
      configuration.set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting filter expression to Job", e);
    }
  }

  /**
   * It sets the resolved filter expression
   *
   * @param configuration
   * @param filterExpression
   */
  public static void setFilterPredicates(Configuration configuration,
      FilterResolverIntf filterExpression) {
    try {
      if (filterExpression == null) {
        return;
      }
      String filterString = ObjectSerializationUtil.convertObjectToString(filterExpression);
      configuration.set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting filter expression to Job", e);
    }
  }

  public static void setColumnProjection(CarbonProjection projection, Configuration configuration) {
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

  public static void setCarbonReadSupport(Class<? extends CarbonReadSupport> readSupportClass,
      Configuration configuration) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  public static CarbonTablePath getTablePath(Configuration configuration) throws IOException {

    String storePathString = getStorePathString(configuration);
    CarbonTableIdentifier tableIdentifier = CarbonInputFormat.getTableToAccess(configuration);
    if (tableIdentifier == null) {
      throw new IOException("Could not find " + DATABASE_NAME + "," + TABLE_NAME);
    }
    return CarbonStorePath.getCarbonTablePath(storePathString, tableIdentifier);
  }

  private static String getStorePathString(Configuration configuration) throws IOException {

    String dirs = configuration.get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    if (inputPaths.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    return CarbonInputFormatUtil.processPath(inputPaths[0]);
  }

  /**
   * Set List of segments to access
   */
  private void setSegmentsToAccess(Configuration configuration, List<String> segmentNosList) {

    //serialize to comma separated string
    StringBuilder stringSegmentsBuilder = new StringBuilder();
    for (int i = 0; i < segmentNosList.size(); i++) {
      String segmentNo = segmentNosList.get(i);
      stringSegmentsBuilder.append(segmentNo);
      if (i < segmentNosList.size() - 1) {
        stringSegmentsBuilder.append(",");
      }
    }
    configuration.set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, stringSegmentsBuilder.toString());
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
    try {
      CarbonTable carbonTable = getCarbonTable(job.getConfiguration());
      Object filterPredicates = getFilterPredicates(job.getConfiguration());
      if (getValidSegments(job).length == 0) {
        // Get the valid segments from the carbon store.
        SegmentStatusManager.ValidSegmentsInfo validSegments =
            new SegmentStatusManager(getAbsoluteTableIdentifier(job.getConfiguration()))
                .getValidSegments();
        if (validSegments.listOfValidSegments.isEmpty()) {
          return new ArrayList<InputSplit>();
        }
        setSegmentsToAccess(job.getConfiguration(), validSegments.listOfValidSegments);
      }

      if (filterPredicates == null) {
        return getSplitsNonFilter(job);
      } else {
        if (filterPredicates instanceof Expression) {
          //process and resolve the expression.
          CarbonInputFormatUtil.processFilterExpression((Expression) filterPredicates, carbonTable);
          return getSplits(job, CarbonInputFormatUtil.resolveFilter((Expression) filterPredicates,
              getAbsoluteTableIdentifier(job.getConfiguration())));
        } else {
          //It means user sets already resolved expression.
          return getSplits(job, (FilterResolverIntf) filterPredicates);
        }
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  /**
   * the method will return the blocks to be scanned with blocklets info
   *
   * @param job
   * @return
   * @throws IOException
   * @throws IndexBuilderException
   */
  private List<InputSplit> getSplitsNonFilter(JobContext job)
      throws IOException, IndexBuilderException {
    return getSplits(job, null);
  }

  private List<InputSplit> getSplitsInternal(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    List<InputSplit> carbonSplits = new ArrayList<InputSplit>(splits.size());
    // identify table blocks
    for (InputSplit inputSplit : splits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      String segmentId = CarbonTablePath.DataPathUtil.getSegmentId(fileSplit.getPath().toString());
      if (INVALID_SEGMENT_ID == segmentId) {
        continue;
      }
      carbonSplits.add(CarbonInputSplit.from(segmentId, fileSplit));
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
  private List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException, IndexBuilderException {

    List<InputSplit> result = new LinkedList<InputSplit>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getAbsoluteTableIdentifier(job.getConfiguration());

    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getValidSegments(job)) {
      List<DataRefNode> dataRefNodes =
          getDataBlocksOfSegment(job, filterExpressionProcessor, absoluteTableIdentifier,
              filterResolver, segmentNo);
      for (DataRefNode dataRefNode : dataRefNodes) {
        BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
        TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
        result.add(new CarbonInputSplit(segmentNo, new Path(tableBlockInfo.getFilePath()),
            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
            tableBlockInfo.getLocations(), tableBlockInfo.getBlockletInfos().getNoOfBlockLets()));
      }
    }
    return result;
  }

  /**
   * get total number of rows. Same as count(*)
   *
   * @throws IOException
   * @throws IndexBuilderException
   */
  public long getRowCount(JobContext job) throws IOException, IndexBuilderException {

    long rowCount = 0;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        getAbsoluteTableIdentifier(job.getConfiguration());
    SegmentStatusManager.ValidSegmentsInfo validSegments =
        new SegmentStatusManager(getAbsoluteTableIdentifier(job.getConfiguration()))
            .getValidSegments();
    setSegmentsToAccess(job.getConfiguration(), validSegments.listOfValidSegments);
    // no of core to load the blocks in driver
    int numberOfCores = CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT));
    } catch (NumberFormatException e) {
      numberOfCores = CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE;
    }
    // creating a thread pool
    ExecutorService threadPool = Executors.newFixedThreadPool(numberOfCores);
    List<Future<Map<String, AbstractIndex>>> loadedBlocks =
        new ArrayList<Future<Map<String, AbstractIndex>>>();
    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getValidSegments(job)) {
      // submitting the task
      loadedBlocks
          .add(threadPool.submit(new BlocksLoaderThread(job, absoluteTableIdentifier, segmentNo)));
    }
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new IndexBuilderException(e);
    }
    try {
      // adding all the rows of the blocks to get the total row
      // count
      for (Future<Map<String, AbstractIndex>> block : loadedBlocks) {
        for (AbstractIndex abstractIndex : block.get().values()) {
          rowCount += abstractIndex.getTotalNumberOfRows();
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IndexBuilderException(e);
    }
    return rowCount;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public FilterResolverIntf getResolvedFilter(Configuration configuration,
      Expression filterExpression)
      throws IOException, IndexBuilderException, QueryExecutionException {
    if (filterExpression == null) {
      return null;
    }
    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    AbsoluteTableIdentifier absoluteTableIdentifier = getAbsoluteTableIdentifier(configuration);
    //get resolved filter
    try {
      return filterExpressionProcessor.getFilterResolver(filterExpression, absoluteTableIdentifier);
    } catch (FilterUnsupportedException e) {
      throw new QueryExecutionException(e.getMessage());
    }
  }

  private AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    return new AbsoluteTableIdentifier(getStorePathString(configuration),
        getTableToAccess(configuration));
  }

  private Object getFilterPredicates(Configuration configuration) {
    try {
      String filterExprString = configuration.get(FILTER_PREDICATE);
      if (filterExprString == null) {
        return null;
      }
      Object filterExprs = ObjectSerializationUtil.convertStringToObject(filterExprString);
      return filterExprs;
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
      String segmentId) throws IndexBuilderException, IOException {

    QueryStatisticsRecorder recorder = new QueryStatisticsRecorder("");
    QueryStatistic statistic = new QueryStatistic();
    Map<String, AbstractIndex> segmentIndexMap =
        getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentId);

    List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

    // build result
    for (AbstractIndex abstractIndex : segmentIndexMap.values()) {

      List<DataRefNode> filterredBlocks = null;
      // if no filter is given get all blocks from Btree Index
      if (null == resolver) {
        filterredBlocks = getDataBlocksOfIndex(abstractIndex);
      } else {
        // apply filter and get matching blocks
        try {
          filterredBlocks = filterExpressionProcessor
              .getFilterredBlocks(abstractIndex.getDataRefNode(), resolver, abstractIndex,
                  absoluteTableIdentifier);
        } catch (QueryExecutionException e) {
          throw new IndexBuilderException(e.getMessage());
        }
      }
      resultFilterredBlocks.addAll(filterredBlocks);
    }
    statistic.addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER,
        System.currentTimeMillis());
    recorder.recordStatistics(statistic);
    recorder.logStatistics();
    return resultFilterredBlocks;
  }

  private Map<String, AbstractIndex> getSegmentAbstractIndexs(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId)
      throws IOException, IndexBuilderException {
    Map<String, AbstractIndex> segmentIndexMap = SegmentTaskIndexStore.getInstance()
        .getSegmentBTreeIfExists(absoluteTableIdentifier, segmentId);

    // if segment tree is not loaded, load the segment tree
    if (segmentIndexMap == null) {
      // List<FileStatus> fileStatusList = new LinkedList<FileStatus>();
      List<TableBlockInfo> tableBlockInfoList = new LinkedList<TableBlockInfo>();
      // getFileStatusOfSegments(job, new int[]{ segmentId }, fileStatusList);

      // get file location of all files of given segment
      JobContext newJob =
          new JobContextImpl(new Configuration(job.getConfiguration()), job.getJobID());
      newJob.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, segmentId + "");

      // identify table blocks
      for (InputSplit inputSplit : getSplitsInternal(newJob)) {
        CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
        BlockletInfos blockletInfos = new BlockletInfos(carbonInputSplit.getNumberOfBlocklets(), 0,
            carbonInputSplit.getNumberOfBlocklets());
        tableBlockInfoList.add(
            new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                segmentId, carbonInputSplit.getLocations(), carbonInputSplit.getLength(),
                blockletInfos));
      }

      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
      segmentToTableBlocksInfos.put(segmentId, tableBlockInfoList);

      // get Btree blocks for given segment
      segmentIndexMap = SegmentTaskIndexStore.getInstance()
          .loadAndGetTaskIdToSegmentsMap(segmentToTableBlocksInfos, absoluteTableIdentifier);

    }
    return segmentIndexMap;
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
    CarbonTable carbonTable = getCarbonTable(configuration);
    QueryModel queryModel;
    try {
      CarbonQueryPlan queryPlan =
          CarbonInputFormatUtil.createQueryPlan(carbonTable, configuration.get(COLUMN_PROJECTION));
      queryModel =
          QueryModel.createModel(getAbsoluteTableIdentifier(configuration), queryPlan, carbonTable);
      Object filterPredicates = getFilterPredicates(configuration);
      if (filterPredicates != null) {
        if (filterPredicates instanceof Expression) {
          CarbonInputFormatUtil.processFilterExpression((Expression) filterPredicates, carbonTable);
          queryModel.setFilterExpressionResolverTree(CarbonInputFormatUtil
              .resolveFilter((Expression) filterPredicates,
                  getAbsoluteTableIdentifier(configuration)));
        } else {
          queryModel.setFilterExpressionResolverTree((FilterResolverIntf) filterPredicates);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    CarbonReadSupport readSupport = getReadSupportClass(configuration);
    return new CarbonRecordReader<T>(queryModel, readSupport);
  }

  private CarbonReadSupport getReadSupportClass(Configuration configuration) {
    String readSupportClass = configuration.get(CARBON_READ_SUPPORT);
    //By default it uses dictionary decoder read class
    CarbonReadSupport readSupport = null;
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
      readSupport = new DictionaryDecodedReadSupportImpl();
    }
    return readSupport;
  }

  @Override protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
    return super.computeSplitSize(blockSize, minSize, maxSize);
  }

  @Override protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    return super.getBlockIndex(blkLocations, offset);
  }

  @Override protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    String[] segmentsToConsider = getValidSegments(job);
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
      if (fileSystem instanceof LocalFileSystem)
      {
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

    PathFilter inputFilter = getDataFileFilter(job);
    CarbonTablePath tablePath = getTablePath(job.getConfiguration());

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { tablePath },
        job.getConfiguration());

    //get all data files of valid partitions and segments
    for (int i = 0; i < partitionsToConsider.length; ++i) {
      String partition = partitionsToConsider[i];

      for (int j = 0; j < segmentsToConsider.length; ++j) {
        String segmentId = segmentsToConsider[j];
        Path segmentPath = new Path(tablePath.getCarbonDataDirectoryPath(partition, segmentId));
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
   * @param job
   * @return the PathFilter for Fact Files.
   */
  public PathFilter getDataFileFilter(JobContext job) {
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
   * @return updateExtension
   */
  private String[] getValidSegments(JobContext job) throws IOException {
    String segmentString = job.getConfiguration().get(INPUT_SEGMENT_NUMBERS, "");
    // if no segments
    if (segmentString.trim().isEmpty()) {
      return new String[0];
    }

    String[] segments = segmentString.split(",");
    String[] segmentIds = new String[segments.length];
    int i = 0;
    try {
      for (; i < segments.length; i++) {
        segmentIds[i] = segments[i];
      }
    } catch (NumberFormatException e) {
      throw new IOException("segment no:" + segments[i] + " should be integer");
    }
    return segmentIds;
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

  /**
   * Thread class to load the blocks
   */
  private class BlocksLoaderThread implements Callable<Map<String, AbstractIndex>> {
    // job
    private JobContext job;

    // table identifier
    private AbsoluteTableIdentifier absoluteTableIdentifier;

    // segment id
    private String segmentId;

    private BlocksLoaderThread(JobContext job, AbsoluteTableIdentifier absoluteTableIdentifier,
        String segmentId) {
      this.job = job;
      this.absoluteTableIdentifier = absoluteTableIdentifier;
      this.segmentId = segmentId;
    }

    @Override public Map<String, AbstractIndex> call() throws Exception {
      return getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentId);
    }
  }
}
