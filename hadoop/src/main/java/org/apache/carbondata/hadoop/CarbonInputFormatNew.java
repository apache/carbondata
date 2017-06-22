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
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.indexstore.AbstractTableDataMap;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.DataMapStoreManager;
import org.apache.carbondata.core.indexstore.DataMapType;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;
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
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.StringUtils;

/**
 * Carbon Input format class representing one carbon table
 */
public class CarbonInputFormatNew<T> extends FileInputFormat<Void, T> {

  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  // comma separated list of input files
  public static final String INPUT_FILES = "mapreduce.input.carboninputformat.files";
  private static final Log LOG = LogFactory.getLog(CarbonInputFormatNew.class);
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
   *
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
    configuration.set(CarbonInputFormatNew.INPUT_SEGMENT_NUMBERS,
        CarbonUtil.getSegmentString(validSegments));
  }

  /**
   * Set list of files to access
   */
  public static void setFilesToAccess(Configuration configuration, List<String> validFiles) {
    configuration.set(CarbonInputFormatNew.INPUT_FILES, CarbonUtil.getSegmentString(validFiles));
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
    AbstractTableDataMap blockletMap =
        DataMapStoreManager.getInstance().getDataMap(identifier, "blocklet", DataMapType.BLOCKLET);
    try {
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
          List<TableSegmentUniqueIdentifier> invalidSegmentsIds =
              new ArrayList<>(invalidSegments.size());
          for (String segId : invalidSegments) {
            invalidSegmentsIds.add(new TableSegmentUniqueIdentifier(identifier, segId));
          }
          cacheClient.getSegmentAccessClient().invalidateAll(invalidSegmentsIds);
          blockletMap.clear(invalidSegments);
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

      // prune partitions for filter query on partition table
      BitSet matchedPartitions = null;
      if (null != filter) {
        PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName());
        if (null != partitionInfo) {
          Partitioner partitioner = PartitionUtil.getPartitioner(partitionInfo);
          matchedPartitions = new FilterExpressionProcessor()
              .getFilteredPartitions(filter, partitionInfo, partitioner);
          if (matchedPartitions.cardinality() == 0) {
            // no partition is required
            return new ArrayList<InputSplit>();
          }
          if (matchedPartitions.cardinality() == partitioner.numPartitions()) {
            // all partitions are required, no need to prune partitions
            matchedPartitions = null;
          }
        }
      }

      FilterResolverIntf filterInterface = CarbonInputFormatUtil.resolveFilter(filter, identifier);

      // do block filtering and get split
      List<InputSplit> splits = getSplits(job, filterInterface, matchedPartitions, cacheClient);
      // pass the invalid segment to task side in order to remove index entry in task side
      if (invalidSegments.size() > 0) {
        for (InputSplit split : splits) {
          ((CarbonInputSplit) split).setInvalidSegments(invalidSegments);
          ((CarbonInputSplit) split).setInvalidTimestampRange(invalidTimestampsList);
        }
      }
      return splits;
    } finally {
      // close the cache cache client to clear LRU cache memory
      cacheClient.close();
    }
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
      BitSet matchedPartitions, CacheClient cacheClient) throws IOException {

    List<InputSplit> result = new LinkedList<InputSplit>();
    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    UpdateVO invalidBlockVOForSegmentId = null;
    Boolean isIUDTable = false;

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getCarbonTable(job.getConfiguration()).getAbsoluteTableIdentifier();
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);

    isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    //for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, absoluteTableIdentifier, filterResolver, matchedPartitions,
            Arrays.asList(getSegmentsToAccess(job)));
    for (CarbonInputSplit inputSplit : dataBlocksOfSegment) {

      // Get the UpdateVO for those tables on which IUD operations being performed.
      if (isIUDTable) {
        invalidBlockVOForSegmentId =
            updateStatusManager.getInvalidTimestampRange(inputSplit.getSegmentId());
      }
      if (isIUDTable) {
        // In case IUD is not performed in this table avoid searching for
        // invalidated blocks.
        if (CarbonUtil
            .isInvalidTableBlock(inputSplit.getSegmentId(), inputSplit.getPath().toString(),
                invalidBlockVOForSegmentId, updateStatusManager)) {
          continue;
        }
      }
      String[] deleteDeltaFilePath = null;
      try {
        deleteDeltaFilePath =
            updateStatusManager.getDeleteDeltaFilePath(inputSplit.getPath().toString());
      } catch (Exception e) {
        throw new IOException(e);
      }
      inputSplit.setDeleteDeltaFiles(deleteDeltaFilePath);
      result.add(inputSplit);
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
  private List<CarbonInputSplit> getDataBlocksOfSegment(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver,
      BitSet matchedPartitions, List<String> segmentIds) throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { new Path(absoluteTableIdentifier.getTablePath()) }, job.getConfiguration());

    AbstractTableDataMap blockletMap = DataMapStoreManager.getInstance()
        .getDataMap(absoluteTableIdentifier, "blocklet", DataMapType.BLOCKLET);
    List<Blocklet> prunedBlocklets = blockletMap.prune(segmentIds, resolver);

    List<CarbonInputSplit> resultFilterredBlocks = new ArrayList<>();
    for (Blocklet blocklet : prunedBlocklets) {
      int taskId = CarbonTablePath.DataFileUtil
          .getTaskIdFromTaskNo(CarbonTablePath.DataFileUtil.getTaskNo(blocklet.getPath().toString()));

      // matchedPartitions variable will be null in two cases as follows
      // 1. the table is not a partition table
      // 2. the table is a partition table, and all partitions are matched by query
      // for partition table, the task id of carbaondata file name is the partition id.
      // if this partition is not required, here will skip it.
      if (matchedPartitions == null || matchedPartitions.get(taskId)) {
        resultFilterredBlocks.add(convertToCarbonInputSplit(blocklet));
      }
    }
    statistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
    recorder.recordStatisticsForDriver(statistic, job.getConfiguration().get("query.id"));
    return resultFilterredBlocks;
  }

  private CarbonInputSplit convertToCarbonInputSplit(Blocklet blocklet) throws IOException {
    blocklet.updateLocations();
    CarbonInputSplit split = CarbonInputSplit.from(blocklet.getSegmentId(),
        new FileSplit(blocklet.getPath(), 0, blocklet.getLength(), blocklet.getLocations()),
        ColumnarFormatVersion.valueOf((short) blocklet.getDetailInfo().getVersionNumber()));
    split.setDetailInfo(blocklet.getDetailInfo());
    return split;
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
    FilterResolverIntf filterIntf = CarbonInputFormatUtil.resolveFilter(filter, identifier);
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
   * return valid file to access
   */
  private String[] getFilesToAccess(JobContext job) {
    String fileString = job.getConfiguration().get(INPUT_FILES, "");
    if (fileString.trim().isEmpty()) {
      return new String[0];
    }
    return fileString.split(",");
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
