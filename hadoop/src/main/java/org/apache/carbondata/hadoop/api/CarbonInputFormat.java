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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonCommonConstantsInternal;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.datamap.DataMapJob;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.DataMapUtil;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapWrapperSimpleInfo;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.log4j.Logger;

/**
 * Base class for carbondata input format, there are two input format implementations:
 * 1. CarbonFileInputFormat: for reading carbondata files without table level metadata support.
 *
 * 2. CarbonTableInputFormat: for reading carbondata files with table level metadata support,
 * such as segment and explicit schema metadata.
 *
 * @param <T>
 */
public abstract class CarbonInputFormat<T> extends FileInputFormat<Void, T> {
  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  private static final String VALIDATE_INPUT_SEGMENT_IDs =
      "mapreduce.input.carboninputformat.validsegments";
  // comma separated list of input files
  private static final String ALTER_PARTITION_ID = "mapreduce.input.carboninputformat.partitionid";
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonInputFormat.class.getName());
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String CARBON_TRANSACTIONAL_TABLE =
      "mapreduce.input.carboninputformat.transactional";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  private static final String CARBON_CONVERTER = "mapreduce.input.carboninputformat.converter";
  public static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databaseName";
  public static final String TABLE_NAME = "mapreduce.input.carboninputformat.tableName";
  private static final String PARTITIONS_TO_PRUNE =
      "mapreduce.input.carboninputformat.partitions.to.prune";
  private static final String FGDATAMAP_PRUNING = "mapreduce.input.carboninputformat.fgdatamap";
  private static final String READ_COMMITTED_SCOPE =
      "mapreduce.input.carboninputformat.read.committed.scope";

  // record segment number and hit blocks
  protected int numSegments = 0;
  protected int numStreamSegments = 0;
  protected int numStreamFiles = 0;
  protected int hitedStreamFiles = 0;
  protected int numBlocks = 0;
  protected List fileLists = null;

  private CarbonTable carbonTable;


  public int getNumSegments() {
    return numSegments;
  }

  public int getNumStreamSegments() {
    return numStreamSegments;
  }

  public int getNumStreamFiles() {
    return numStreamFiles;
  }

  public int getHitedStreamFiles() {
    return hitedStreamFiles;
  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public void setFileLists(List fileLists) {
    this.fileLists = fileLists;
  }

  /**
   * Set the `tableInfo` in `configuration`
   */
  public static void setTableInfo(Configuration configuration, TableInfo tableInfo)
      throws IOException {
    if (null != tableInfo) {
      configuration.set(TABLE_INFO, CarbonUtil.encodeToString(tableInfo.serialize()));
    }
  }

  /**
   * Get TableInfo object from `configuration`
   */
  protected static TableInfo getTableInfo(Configuration configuration) throws IOException {
    String tableInfoStr = configuration.get(TABLE_INFO);
    if (tableInfoStr == null) {
      return null;
    } else {
      TableInfo output = new TableInfo();
      output.readFields(new DataInputStream(
          new ByteArrayInputStream(CarbonUtil.decodeStringToBytes(tableInfoStr))));
      return output;
    }
  }

  /**
   * Get the cached CarbonTable or create it by TableInfo in `configuration`
   */
  public CarbonTable getOrCreateCarbonTable(Configuration configuration)
      throws IOException {
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

  public static void setTablePath(Configuration configuration, String tablePath) {
    configuration.set(FileInputFormat.INPUT_DIR, tablePath);
  }

  public static void setTransactionalTable(Configuration configuration,
      boolean isTransactionalTable) {
    configuration.set(CARBON_TRANSACTIONAL_TABLE, String.valueOf(isTransactionalTable));
  }

  public static void setPartitionIdList(Configuration configuration, List<String> partitionIds) {
    configuration.set(ALTER_PARTITION_ID, partitionIds.toString());
  }

  /**
   * It sets unresolved filter expression.
   *
   * @param configuration
   * @para    DataMapJob dataMapJob = getDataMapJob(job.getConfiguration());
m filterExpression
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

  /**
   * Set the column projection column names
   *
   * @param configuration     Configuration info
   * @param projectionColumns projection columns name
   */
  public static void setColumnProjection(Configuration configuration, String[] projectionColumns) {
    Objects.requireNonNull(projectionColumns);
    if (projectionColumns.length < 1) {
      throw new RuntimeException("Projection can't be empty");
    }
    StringBuilder builder = new StringBuilder();
    for (String column : projectionColumns) {
      builder.append(column).append(",");
    }
    String columnString = builder.toString();
    columnString = columnString.substring(0, columnString.length() - 1);
    configuration.set(COLUMN_PROJECTION, columnString);
  }

  /**
   * Set the column projection column names from CarbonProjection
   *
   * @param configuration Configuration info
   * @param projection    CarbonProjection object that includes unique projection column name
   */
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

  public static void setFgDataMapPruning(Configuration configuration, boolean enable) {
    configuration.set(FGDATAMAP_PRUNING, String.valueOf(enable));
  }

  public static boolean isFgDataMapPruningEnable(Configuration configuration) {
    String enable = configuration.get(FGDATAMAP_PRUNING);

    // if FDDATAMAP_PRUNING is not set, by default we will use FGDataMap
    return (enable == null) || enable.equalsIgnoreCase("true");
  }

  /**
   * Set list of segments to access
   */
  public static void setSegmentsToAccess(Configuration configuration, List<Segment> validSegments) {
    configuration.set(INPUT_SEGMENT_NUMBERS, CarbonUtil.convertToString(validSegments));
  }

  /**
   * Set `CARBON_INPUT_SEGMENTS` from property to configuration
   */
  public static void setQuerySegment(Configuration conf, AbsoluteTableIdentifier identifier) {
    String dbName = identifier.getCarbonTableIdentifier().getDatabaseName().toLowerCase();
    String tbName = identifier.getCarbonTableIdentifier().getTableName().toLowerCase();
    getQuerySegmentToAccess(conf, dbName, tbName);
  }

  /**
   * Set `CARBON_INPUT_SEGMENTS` from property to configuration
   */
  public static void setQuerySegment(Configuration conf, String segmentList) {
    if (!segmentList.trim().equals("*")) {
      CarbonInputFormat
          .setSegmentsToAccess(conf, Segment.toSegmentList(segmentList.split(","), null));
    }
  }

  /**
   * set list of segment to access
   */
  public static void setValidateSegmentsToAccess(Configuration configuration, Boolean validate) {
    configuration.set(CarbonInputFormat.VALIDATE_INPUT_SEGMENT_IDs, validate.toString());
  }

  /**
   * get list of segment to access
   */
  public static boolean getValidateSegmentsToAccess(Configuration configuration) {
    return configuration.get(CarbonInputFormat.VALIDATE_INPUT_SEGMENT_IDs, "true")
        .equalsIgnoreCase("true");
  }

  /**
   * set list of partitions to prune
   */
  public static void setPartitionsToPrune(Configuration configuration,
      List<PartitionSpec> partitions) {
    if (partitions == null) {
      return;
    }
    try {
      String partitionString =
          ObjectSerializationUtil.convertObjectToString(new ArrayList<>(partitions));
      configuration.set(PARTITIONS_TO_PRUNE, partitionString);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error while setting partition information to Job" + partitions, e);
    }
  }

  /**
   * get list of partitions to prune
   */
  public static List<PartitionSpec> getPartitionsToPrune(Configuration configuration)
      throws IOException {
    String partitionString = configuration.get(PARTITIONS_TO_PRUNE);
    if (partitionString != null) {
      return (List<PartitionSpec>) ObjectSerializationUtil.convertStringToObject(partitionString);
    }
    return null;
  }

  public AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    String tablePath = configuration.get(INPUT_DIR, "");
    try {
      return AbsoluteTableIdentifier
          .from(tablePath, getDatabaseName(configuration), getTableName(configuration));
    } catch (InvalidConfigurationException e) {
      throw new IOException(e);
    }
  }

  public static void setReadCommittedScope(Configuration configuration,
      ReadCommittedScope committedScope) {
    if (committedScope == null) {
      return;
    }
    try {
      String subFoldersString = ObjectSerializationUtil.convertObjectToString(committedScope);
      configuration.set(READ_COMMITTED_SCOPE, subFoldersString);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error while setting committedScope information to Job" + committedScope, e);
    }
  }

  public static ReadCommittedScope getReadCommittedScope(Configuration configuration)
      throws IOException {
    String subFoldersString = configuration.get(READ_COMMITTED_SCOPE);
    if (subFoldersString != null) {
      return (ReadCommittedScope) ObjectSerializationUtil.convertStringToObject(subFoldersString);
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
  @Override public abstract List<InputSplit> getSplits(JobContext job) throws IOException;

  List<ExtendedBlocklet> getDistributedSplit(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<PartitionSpec> partitionNames,
      List<Segment> validSegments, List<Segment> invalidSegments,
      List<String> segmentsToBeRefreshed) throws IOException {
    try {
      DataMapJob dataMapJob =
          (DataMapJob) DataMapUtil.createDataMapJob(DataMapUtil.DISTRIBUTED_JOB_NAME);
      if (dataMapJob == null) {
        throw new ExceptionInInitializerError("Unable to create DistributedDataMapJob");
      }
      return DataMapUtil
          .executeDataMapJob(table, filterResolverIntf, dataMapJob, partitionNames, validSegments,
              invalidSegments, null, segmentsToBeRefreshed);
    } catch (Exception e) {
      // Check if fallback is disabled for testing purposes then directly throw exception.
      if (CarbonProperties.getInstance().isFallBackDisabled()) {
        throw e;
      }
      LOG.error("Exception occurred while getting splits using index server. Initiating Fall "
          + "back to embedded mode", e);
      return DataMapUtil
          .executeDataMapJob(table, filterResolverIntf,
              DataMapUtil.getEmbeddedJob(), partitionNames, validSegments, invalidSegments, null,
              true, segmentsToBeRefreshed);
    }
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
  protected List<CarbonInputSplit> getDataBlocksOfSegment(JobContext job, CarbonTable carbonTable,
      Expression expression, BitSet matchedPartitions, List<Segment> segmentIds,
      PartitionInfo partitionInfo, List<Integer> oldPartitionIdList,
      List<Segment> invalidSegments, List<String> segmentsToBeRefreshed)
      throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { new Path(carbonTable.getTablePath()) }, job.getConfiguration());
    List<ExtendedBlocklet> prunedBlocklets =
        getPrunedBlocklets(job, carbonTable, expression, segmentIds, invalidSegments,
            segmentsToBeRefreshed);
    List<CarbonInputSplit> resultFilteredBlocks = new ArrayList<>();
    int partitionIndex = 0;
    List<Integer> partitionIdList = new ArrayList<>();
    if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
      partitionIdList = partitionInfo.getPartitionIds();
    }
    for (ExtendedBlocklet blocklet : prunedBlocklets) {

      // OldPartitionIdList is only used in alter table partition command because it change
      // partition info first and then read data.
      // For other normal query should use newest partitionIdList
      if (partitionInfo != null && partitionInfo.getPartitionType() != PartitionType.NATIVE_HIVE) {
        long partitionId = CarbonTablePath.DataFileUtil
            .getTaskIdFromTaskNo(CarbonTablePath.DataFileUtil.getTaskNo(blocklet.getPath()));
        if (oldPartitionIdList != null) {
          partitionIndex = oldPartitionIdList.indexOf((int) partitionId);
        } else {
          partitionIndex = partitionIdList.indexOf((int) partitionId);
        }
      }
      if (partitionIndex != -1) {
        // matchedPartitions variable will be null in two cases as follows
        // 1. the table is not a partition table
        // 2. the table is a partition table, and all partitions are matched by query
        // for partition table, the task id of carbaondata file name is the partition id.
        // if this partition is not required, here will skip it.
        if (matchedPartitions == null || matchedPartitions.get(partitionIndex)) {
          resultFilteredBlocks.add(blocklet.getInputSplit());
        }
      }
    }
    statistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
    recorder.recordStatisticsForDriver(statistic, job.getConfiguration().get("query.id"));
    return resultFilteredBlocks;
  }

  /**
   * for explain command
   * get number of block by counting distinct file path of blocklets
   */
  private int getBlockCount(List<ExtendedBlocklet> blocklets) {
    Set<String> filepaths = new HashSet<>();
    for (ExtendedBlocklet blocklet: blocklets) {
      filepaths.add(blocklet.getPath());
    }
    return filepaths.size();
  }

  /**
   * Prune the blocklets using the filter expression with available datamaps.
   * First pruned with default blocklet datamap, then pruned with CG and FG datamaps
   */
  private List<ExtendedBlocklet> getPrunedBlocklets(JobContext job, CarbonTable carbonTable,
      Expression expression, List<Segment> segmentIds, List<Segment> invalidSegments,
      List<String> segmentsToBeRefreshed) throws IOException {
    ExplainCollector.addPruningInfo(carbonTable.getTableName());
    final DataMapFilter filter = new DataMapFilter(carbonTable, expression);
    ExplainCollector.setFilterStatement(expression == null ? "none" : expression.getStatement());
    boolean distributedCG = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
            CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT));
    DataMapJob dataMapJob = DataMapUtil.getDataMapJob(job.getConfiguration());
    List<PartitionSpec> partitionsToPrune = getPartitionsToPrune(job.getConfiguration());
    // First prune using default datamap on driver side.
    TableDataMap defaultDataMap = DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable);
    List<ExtendedBlocklet> prunedBlocklets = null;
    // This is to log the event, so user will know what is happening by seeing logs.
    LOG.info("Started block pruning ...");
    boolean isDistributedPruningEnabled = CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName());
    if (isDistributedPruningEnabled) {
      try {
        prunedBlocklets =
            getDistributedSplit(carbonTable, filter.getResolver(), partitionsToPrune, segmentIds,
                invalidSegments, segmentsToBeRefreshed);
      } catch (Exception e) {
        // Check if fallback is disabled then directly throw exception otherwise try driver
        // pruning.
        if (CarbonProperties.getInstance().isFallBackDisabled()) {
          throw e;
        }
        prunedBlocklets = defaultDataMap.prune(segmentIds, filter, partitionsToPrune);
      }
    } else {
      prunedBlocklets = defaultDataMap.prune(segmentIds, filter, partitionsToPrune);

      if (ExplainCollector.enabled()) {
        ExplainCollector.setDefaultDataMapPruningBlockHit(getBlockCount(prunedBlocklets));
      }

      if (prunedBlocklets.size() == 0) {
        return prunedBlocklets;
      }

      DataMapChooser chooser = new DataMapChooser(getOrCreateCarbonTable(job.getConfiguration()));

      // Get the available CG datamaps and prune further.
      DataMapExprWrapper cgDataMapExprWrapper = chooser.chooseCGDataMap(filter.getResolver());

      if (cgDataMapExprWrapper != null) {
        // Prune segments from already pruned blocklets
        DataMapUtil.pruneSegments(segmentIds, prunedBlocklets);
        List<ExtendedBlocklet> cgPrunedBlocklets;
        // Again prune with CG datamap.
        if (distributedCG && dataMapJob != null) {
          cgPrunedBlocklets = DataMapUtil
              .executeDataMapJob(carbonTable, filter.getResolver(), dataMapJob, partitionsToPrune,
                  segmentIds, invalidSegments, DataMapLevel.CG, true, new ArrayList<String>());
        } else {
          cgPrunedBlocklets = cgDataMapExprWrapper.prune(segmentIds, partitionsToPrune);
        }
        // since index datamap prune in segment scope,
        // the result need to intersect with previous pruned result
        prunedBlocklets =
            intersectFilteredBlocklets(carbonTable, prunedBlocklets, cgPrunedBlocklets);
        if (ExplainCollector.enabled()) {
          ExplainCollector.recordCGDataMapPruning(
              DataMapWrapperSimpleInfo.fromDataMapWrapper(cgDataMapExprWrapper),
              prunedBlocklets.size(), getBlockCount(prunedBlocklets));
        }
      }

      if (prunedBlocklets.size() == 0) {
        return prunedBlocklets;
      }
      // Now try to prune with FG DataMap.
      if (isFgDataMapPruningEnable(job.getConfiguration()) && dataMapJob != null) {
        DataMapExprWrapper fgDataMapExprWrapper = chooser.chooseFGDataMap(filter.getResolver());
        List<ExtendedBlocklet> fgPrunedBlocklets;
        if (fgDataMapExprWrapper != null) {
          // Prune segments from already pruned blocklets
          DataMapUtil.pruneSegments(segmentIds, prunedBlocklets);
          // Prune segments from already pruned blocklets
          fgPrunedBlocklets = DataMapUtil
              .executeDataMapJob(carbonTable, filter.getResolver(), dataMapJob, partitionsToPrune,
                  segmentIds, invalidSegments, fgDataMapExprWrapper.getDataMapLevel(), true,
                  new ArrayList<String>());
          // note that the 'fgPrunedBlocklets' has extra datamap related info compared with
          // 'prunedBlocklets', so the intersection should keep the elements in 'fgPrunedBlocklets'
          prunedBlocklets =
              intersectFilteredBlocklets(carbonTable, prunedBlocklets, fgPrunedBlocklets);
          ExplainCollector.recordFGDataMapPruning(
              DataMapWrapperSimpleInfo.fromDataMapWrapper(fgDataMapExprWrapper),
              prunedBlocklets.size(), getBlockCount(prunedBlocklets));
        }
      }
    }
    LOG.info("Finished block pruning ...");
    return prunedBlocklets;
  }

  private List<ExtendedBlocklet> intersectFilteredBlocklets(CarbonTable carbonTable,
      List<ExtendedBlocklet> previousDataMapPrunedBlocklets,
      List<ExtendedBlocklet> otherDataMapPrunedBlocklets) {
    List<ExtendedBlocklet> prunedBlocklets = null;
    if (BlockletDataMapUtil.isCacheLevelBlock(carbonTable)) {
      prunedBlocklets = new ArrayList<>();
      for (ExtendedBlocklet otherBlocklet : otherDataMapPrunedBlocklets) {
        if (previousDataMapPrunedBlocklets.contains(otherBlocklet)) {
          prunedBlocklets.add(otherBlocklet);
        }
      }
    } else {
      prunedBlocklets = (List) CollectionUtils
          .intersection(otherDataMapPrunedBlocklets, previousDataMapPrunedBlocklets);
    }
    return prunedBlocklets;
  }


  static List<InputSplit> convertToCarbonInputSplit(List<ExtendedBlocklet> extendedBlocklets) {
    List<InputSplit> resultFilteredBlocks = new ArrayList<>();
    for (ExtendedBlocklet blocklet : extendedBlocklets) {
      if (blocklet != null) {
        resultFilteredBlocks.add(blocklet.getInputSplit());
      }
    }
    return resultFilteredBlocks;
  }

  @Override public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    QueryModel queryModel = createQueryModel(inputSplit, taskAttemptContext,
        getFilterPredicates(taskAttemptContext.getConfiguration()));
    CarbonReadSupport<T> readSupport = getReadSupportClass(configuration);
    return new CarbonRecordReader<T>(queryModel, readSupport,
        taskAttemptContext.getConfiguration());
  }

  public QueryModel createQueryModel(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException {
    return createQueryModel(inputSplit, taskAttemptContext,
        getFilterPredicates(taskAttemptContext.getConfiguration()));
  }

  public QueryModel createQueryModel(InputSplit inputSplit, TaskAttemptContext taskAttemptContext,
      Expression filterExpression) throws IOException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    CarbonTable carbonTable = getOrCreateCarbonTable(configuration);

    // set projection column in the query model
    String projectionString = getColumnProjection(configuration);
    String[] projectColumns;
    if (projectionString != null) {
      projectColumns = projectionString.split(",");
    } else {
      projectColumns = new String[]{};
    }
    checkAndAddImplicitExpression(filterExpression, inputSplit);
    QueryModel queryModel = new QueryModelBuilder(carbonTable)
        .projectColumns(projectColumns)
        .filterExpression(filterExpression)
        .dataConverter(getDataTypeConverter(configuration))
        .build();
    return queryModel;
  }

  /**
   * This method will create an Implict Expression and set it as right child in the given
   * expression
   *
   * @param expression
   * @param inputSplit
   */
  private void checkAndAddImplicitExpression(Expression expression, InputSplit inputSplit) {
    if (inputSplit instanceof CarbonMultiBlockSplit) {
      CarbonMultiBlockSplit split = (CarbonMultiBlockSplit) inputSplit;
      List<CarbonInputSplit> splits = split.getAllSplits();
      // iterate over all the splits and create block to bblocklet mapping
      Map<String, Set<Integer>> blockIdToBlockletIdMapping = new HashMap<>();
      for (CarbonInputSplit carbonInputSplit : splits) {
        Set<Integer> validBlockletIds = carbonInputSplit.getValidBlockletIds();
        if (null != validBlockletIds && !validBlockletIds.isEmpty()) {
          String uniqueBlockPath = carbonInputSplit.getFilePath();
          String shortBlockPath = CarbonTablePath
              .getShortBlockId(uniqueBlockPath.substring(uniqueBlockPath.lastIndexOf("/Part") + 1));
          blockIdToBlockletIdMapping.put(shortBlockPath, validBlockletIds);
        }
      }
      if (!blockIdToBlockletIdMapping.isEmpty()) {
        // create implicit expression and set as right child
        FilterUtil
            .createImplicitExpressionAndSetAsRightChild(expression, blockIdToBlockletIdMapping);
      }
    }
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

  public static void setCarbonReadSupport(Configuration configuration,
      Class<? extends CarbonReadSupport> readSupportClass) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  /**
   * It is optional, if user does not set then it reads from store
   *
   * @param configuration
   * @param converterClass is the Data type converter for different computing engine
   */
  public static void setDataTypeConverter(
      Configuration configuration, Class<? extends DataTypeConverter> converterClass) {
    if (null != converterClass) {
      configuration.set(CARBON_CONVERTER, converterClass.getCanonicalName());
    }
  }

  public static DataTypeConverter getDataTypeConverter(Configuration configuration)
      throws IOException {
    String converterClass = configuration.get(CARBON_CONVERTER);
    if (converterClass == null) {
      return new DataTypeConverterImpl();
    }

    try {
      return (DataTypeConverter) Class.forName(converterClass).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void setDatabaseName(Configuration configuration, String databaseName) {
    if (null != databaseName) {
      configuration.set(DATABASE_NAME, databaseName);
    }
  }

  public static String getDatabaseName(Configuration configuration)
      throws InvalidConfigurationException {
    String databseName = configuration.get(DATABASE_NAME);
    if (null == databseName) {
      throw new InvalidConfigurationException("Database name is not set.");
    }
    return databseName;
  }

  public static void setTableName(Configuration configuration, String tableName) {
    if (null != tableName) {
      configuration.set(TABLE_NAME, tableName);
    }
  }

  public static String getTableName(Configuration configuration)
      throws InvalidConfigurationException {
    String tableName = configuration.get(TABLE_NAME);
    if (tableName == null) {
      throw new InvalidConfigurationException("Table name is not set");
    }
    return tableName;
  }

  public static void setAccessStreamingSegments(Configuration configuration, Boolean validate)
      throws InvalidConfigurationException {
    configuration.set(
        CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING + "." + getDatabaseName(
            configuration) + "." + getTableName(configuration), validate.toString());
  }

  public static boolean getAccessStreamingSegments(Configuration configuration) {
    try {
      return configuration.get(
          CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING + "." + getDatabaseName(
              configuration) + "." + getTableName(
                  configuration), "false").equalsIgnoreCase("true");

    } catch (InvalidConfigurationException e) {
      return false;
    }
  }

  /**
   * Project all Columns for carbon reader
   *
   * @return String araay of columnNames
   * @param carbonTable
   */
  public String[] projectAllColumns(CarbonTable carbonTable) {
    List<ColumnSchema> colList = carbonTable.getTableInfo().getFactTable().getListOfColumns();
    List<String> projectColumns = new ArrayList<>();
    // complex type and add just the parent column name while skipping the child columns.
    for (ColumnSchema col : colList) {
      if (!col.getColumnName().contains(".")) {
        projectColumns.add(col.getColumnName());
      }
    }
    return projectColumns.toArray(new String[projectColumns.size()]);
  }

  private static void getQuerySegmentToAccess(Configuration conf, String dbName, String tableName) {
    String segmentNumbersFromProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tableName, "*");
    if (!segmentNumbersFromProperty.trim().equals("*")) {
      CarbonInputFormat.setSegmentsToAccess(conf,
          Segment.toSegmentList(segmentNumbersFromProperty.split(","), null));
    }
  }

  /**
   * Set `CARBON_INPUT_SEGMENTS` from property to configuration
   */
  public static void setQuerySegment(Configuration conf, CarbonTable carbonTable) {
    String tableName = carbonTable.getTableName();
    getQuerySegmentToAccess(conf, carbonTable.getDatabaseName(), tableName);
  }

}
