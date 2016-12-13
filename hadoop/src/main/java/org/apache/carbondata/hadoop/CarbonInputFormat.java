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

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnarFormatVersion;
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
import org.apache.carbondata.core.carbon.querystatistics.*;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodedReadSupportImpl;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.carbondata.lcm.status.SegmentStatusManager;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.scan.model.QueryModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
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
      // read it from schema file in the store
      AbsoluteTableIdentifier absIdentifier = getAbsoluteTableIdentifier(configuration);
      CarbonTable carbonTable = SchemaReader.readCarbonTableFromStore(absIdentifier);
      carbonTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier().setLastUpdatedTime(
          absIdentifier.getCarbonTableIdentifier().getLastUpdatedTime());
      setCarbonTable(configuration, carbonTable);
      return carbonTable;
    }
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
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

  public static CarbonTablePath getTablePath(Configuration configuration) throws IOException {
    AbsoluteTableIdentifier absIdentifier = getAbsoluteTableIdentifier(configuration);
    return CarbonStorePath.getCarbonTablePath(absIdentifier);
  }

  /**
   * Set list of segments to access
   */
  public static void setSegmentsToAccess(Configuration configuration, List<String> validSegments) {
    configuration
        .set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, CarbonUtil.getSegmentString(validSegments));
  }

  private static AbsoluteTableIdentifier getAbsoluteTableIdentifier(
      Configuration configuration) throws IOException {
    String dirs = configuration.get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    if (inputPaths.length == 0) {
      throw new InvalidPathException("No input paths specified in job");
    }
    AbsoluteTableIdentifier absTableIdentifier =
        AbsoluteTableIdentifier.fromTablePath(inputPaths[0]);
    long lastModifiedTime =
        SegmentStatusManager.getTableStatusLastModifiedTime(absTableIdentifier);
    absTableIdentifier.getCarbonTableIdentifier().setLastUpdatedTime(lastModifiedTime);
    return absTableIdentifier;
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
    List<String> invalidSegments = new ArrayList<>();

    // get all valid segments and set them into the configuration
    if (getSegmentsToAccess(job).length == 0) {
      SegmentStatusManager.SegmentStatus segments =
          SegmentStatusManager.getSegmentStatus(identifier);
      setSegmentsToAccess(job.getConfiguration(), segments.getValidSegments());
      if (segments.getValidSegments().size() == 0) {
        return new ArrayList<>(0);
      }

      // remove entry in the segment index if there are invalid segments
      invalidSegments.addAll(segments.getInvalidSegments());
      if (invalidSegments.size() > 0) {
        SegmentTaskIndexStore.getInstance().removeTableBlocks(invalidSegments, identifier);
      }
    }

    // process and resolve the expression
    Expression filter = getFilterPredicates(job.getConfiguration());
    CarbonTable carbonTable = getCarbonTable(job.getConfiguration());
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
    FilterResolverIntf filterInterface = CarbonInputFormatUtil.resolveFilter(filter, identifier);
    List<InputSplit> splits;
    try {
      // do block filtering and get split
      splits = getSplits(job, filterInterface);
    } catch (IndexBuilderException e) {
      throw new IOException(e);
    }
    // pass the invalid segment to task side in order to remove index entry in task side
    if (invalidSegments.size() > 0) {
      for (InputSplit split : splits) {
        ((CarbonInputSplit) split).setInvalidSegments(invalidSegments);
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
  private List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException, IndexBuilderException {

    List<InputSplit> result = new LinkedList<InputSplit>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getAbsoluteTableIdentifier(job.getConfiguration());

    //for each segment fetch blocks matching filter in Driver BTree
    for (String segmentNo : getSegmentsToAccess(job)) {
      List<DataRefNode> dataRefNodes =
          getDataBlocksOfSegment(job, filterExpressionProcessor, absoluteTableIdentifier,
              filterResolver, segmentNo);
      for (DataRefNode dataRefNode : dataRefNodes) {
        BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
        TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
        result.add(new CarbonInputSplit(segmentNo, new Path(tableBlockInfo.getFilePath()),
            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
            tableBlockInfo.getLocations(), tableBlockInfo.getBlockletInfos().getNoOfBlockLets(),
            tableBlockInfo.getVersion()));
      }
    }
    return result;
  }

  private Expression getFilterPredicates(Configuration configuration) {
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
      String segmentId) throws IndexBuilderException, IOException {
    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
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
  private List<TableBlockInfo> getTableBlockInfo(JobContext job, String segmentId)
      throws IOException {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<TableBlockInfo>();

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
              blockletInfos, carbonInputSplit.getVersion()));
    }
    return tableBlockInfoList;
  }

  private Map<String, AbstractIndex> getSegmentAbstractIndexs(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId)
      throws IOException, IndexBuilderException {
    Map<String, AbstractIndex> segmentIndexMap = SegmentTaskIndexStore.getInstance()
        .getSegmentBTreeIfExists(absoluteTableIdentifier, segmentId);

    // if segment tree is not loaded, load the segment tree
    if (segmentIndexMap == null) {
      List<TableBlockInfo> tableBlockInfoList = getTableBlockInfo(job, segmentId);

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
    AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(configuration);

    // query plan includes projection column
    String projection = getColumnProjection(configuration);
    CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(carbonTable, projection);
    QueryModel queryModel = QueryModel.createModel(identifier, queryPlan, carbonTable);

    // set the filter to the query model in order to filter blocklet before scan
    Expression filter = getFilterPredicates(configuration);
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
    FilterResolverIntf filterIntf = CarbonInputFormatUtil.resolveFilter(filter, identifier);
    queryModel.setFilterExpressionResolverTree(filterIntf);

    // update the file level index store if there are invalid segment
    if (inputSplit instanceof CarbonMultiBlockSplit) {
      CarbonMultiBlockSplit split = (CarbonMultiBlockSplit) inputSplit;
      List<String> invalidSegments = split.getAllSplits().get(0).getInvalidSegments();
      if (invalidSegments.size() > 0) {
        queryModel.setInvalidSegmentIds(invalidSegments);
      }
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
  private String[] getSegmentsToAccess(JobContext job) throws IOException {
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
