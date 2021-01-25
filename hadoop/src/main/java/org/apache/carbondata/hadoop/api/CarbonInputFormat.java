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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.index.IndexChooser;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.IndexInputFormat;
import org.apache.carbondata.core.index.IndexJob;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.index.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.index.dev.expr.IndexWrapperSimpleInfo;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.profiler.ExplainCollector;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.BlockletIndexUtil;
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
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonInputFormat.class.getName());

  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  private static final String VALIDATE_INPUT_SEGMENT_IDs =
      "mapreduce.input.carboninputformat.validsegments";
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
  private static final String FG_INDEX_PRUNING = "mapreduce.input.carboninputformat.fgindex";
  private static final String READ_COMMITTED_SCOPE =
      "mapreduce.input.carboninputformat.read.committed.scope";
  private static final String READ_ONLY_DELTA = "readDeltaOnly";

  // record segment number and hit blocks
  protected int numSegments = 0;
  protected int numStreamSegments = 0;
  protected int numStreamFiles = 0;
  protected int hitStreamFiles = 0;
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

  public int getHitStreamFiles() {
    return hitStreamFiles;
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

  public static void setIndexTablesScanTree(Configuration configuration,
      String indexTablesScanTree) {
    configuration.set(IndexUtil.CARBON_INDEX_TABLES_SCAN_TREE, indexTablesScanTree);
  }

  /**
   * It sets unresolved filter expression.
   */
  public static void setFilterPredicates(Configuration configuration,
      IndexFilter filterExpression) {
    if (filterExpression == null || filterExpression.getExpression() == null) {
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
    setColumnProjection(configuration, projection.getAllColumns());
  }

  public static String getColumnProjection(Configuration configuration) {
    return configuration.get(COLUMN_PROJECTION);
  }

  public static boolean isFgIndexPruningEnable(Configuration configuration) {
    String enable = configuration.get(FG_INDEX_PRUNING);

    // if FD_INDEX_PRUNING is not set, by default we will use FG Index
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
    setQuerySegmentToAccess(conf, dbName, tbName);
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
   * get list of block/blocklet and make them to CarbonInputSplit
   * @param job JobContext with Configuration
   * @return list of CarbonInputSplit
   * @throws IOException
   */
  @Override
  public abstract List<InputSplit> getSplits(JobContext job) throws IOException;

  /**
   * This method will execute a distributed job to get the count for the
   * table. If the job fails for some reason then an embedded job is fired to
   * get the count.
   */
  Long getDistributedCount(CarbonTable table, List<PartitionSpec> partitionNames,
      List<Segment> validSegments, Configuration configuration) {
    IndexInputFormat indexInputFormat =
        new IndexInputFormat(table, null, validSegments, new ArrayList<>(),
            partitionNames, false, null, false, false);
    indexInputFormat.setIsWriteToFile(false);
    try {
      IndexJob indexJob = (IndexJob) IndexUtil.createIndexJob(IndexUtil.DISTRIBUTED_JOB_NAME);
      if (indexJob == null) {
        throw new ExceptionInInitializerError("Unable to create index job");
      }
      return indexJob.executeCountJob(indexInputFormat, configuration);
    } catch (Exception e) {
      if (CarbonProperties.getInstance().isFallBackDisabled()) {
        LOG.error("Fallback is disabled");
        throw e;
      }
      LOG.error("Failed to get count from index server. Initializing fallback", e);
      IndexJob indexJob = IndexUtil.getEmbeddedJob();
      return indexJob.executeCountJob(indexInputFormat, configuration);
    }
  }

  List<ExtendedBlocklet> getDistributedBlockRowCount(CarbonTable table,
      List<PartitionSpec> partitionNames, List<Segment> validSegments,
      List<Segment> invalidSegments, List<String> segmentsToBeRefreshed,
      Configuration configuration) {
    return getDistributedSplit(table, null, partitionNames, validSegments, invalidSegments,
        segmentsToBeRefreshed, true, configuration);
  }

  private List<ExtendedBlocklet> getDistributedSplit(CarbonTable table,
      FilterResolverIntf filterResolverIntf, List<PartitionSpec> partitionNames,
      List<Segment> validSegments, List<Segment> invalidSegments,
      List<String> segmentsToBeRefreshed, boolean isCountJob, Configuration configuration) {
    try {
      IndexJob indexJob = (IndexJob) IndexUtil.createIndexJob(IndexUtil.DISTRIBUTED_JOB_NAME);
      if (indexJob == null) {
        throw new ExceptionInInitializerError("Unable to create index job");
      }
      return IndexUtil
          .executeIndexJob(table, filterResolverIntf, indexJob, partitionNames, validSegments,
              invalidSegments, null, false, segmentsToBeRefreshed, isCountJob, configuration);
    } catch (Exception e) {
      // Check if fallback is disabled for testing purposes then directly throw exception.
      if (CarbonProperties.getInstance().isFallBackDisabled()) {
        throw e;
      }
      LOG.error("Exception occurred while getting splits using index server. Initiating Fall "
          + "back to embedded mode", e);
      return IndexUtil
          .executeIndexJob(table, filterResolverIntf, IndexUtil.getEmbeddedJob(), partitionNames,
              validSegments, invalidSegments, null, true, segmentsToBeRefreshed, isCountJob,
              configuration);
    }
  }

  public IndexFilter getFilterPredicates(Configuration configuration) {
    try {
      String filterExprString = configuration.get(FILTER_PREDICATE);
      if (filterExprString == null) {
        return null;
      }
      IndexFilter filter =
          (IndexFilter) ObjectSerializationUtil.convertStringToObject(filterExprString);
      if (filter != null) {
        CarbonTable carbonTable = getOrCreateCarbonTable(configuration);
        filter.setTable(carbonTable);
      }
      return filter;
    } catch (IOException e) {
      throw new RuntimeException("Error while reading filter expression", e);
    }
  }

  /**
   * get data blocks of given segment
   */
  protected List<CarbonInputSplit> getDataBlocksOfSegment(JobContext job, CarbonTable carbonTable,
      IndexFilter expression, List<Segment> validSegments,
      List<Segment> invalidSegments, List<String> segmentsToBeRefreshed)
      throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();

    List<ExtendedBlocklet> prunedBlocklets =
        getPrunedBlocklets(job, carbonTable, expression, validSegments, invalidSegments,
            segmentsToBeRefreshed);
    List<CarbonInputSplit> resultFilteredBlocks = new ArrayList<>();
    for (ExtendedBlocklet blocklet : prunedBlocklets) {
      // matchedPartitions variable will be null in two cases as follows
      // 1. the table is not a partition table
      // 2. the table is a partition table, and all partitions are matched by query
      // for partition table, the task id of carbondata file name is the partition id.
      // if this partition is not required, here will skip it.
      resultFilteredBlocks.add(blocklet.getInputSplit());
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
    Set<String> filePaths = new HashSet<>();
    for (ExtendedBlocklet blocklet: blocklets) {
      filePaths.add(blocklet.getPath());
    }
    return filePaths.size();
  }

  /**
   * Prune the blocklets using the filter expression with available index.
   * First pruned with default blocklet index, then pruned with CG and FG index
   */
  private List<ExtendedBlocklet> getPrunedBlocklets(JobContext job, CarbonTable carbonTable,
      IndexFilter filter, List<Segment> validSegments, List<Segment> invalidSegments,
      List<String> segmentsToBeRefreshed) throws IOException {
    ExplainCollector.addPruningInfo(carbonTable.getTableName());
    filter = filter == null ? new IndexFilter(carbonTable, null) : filter;
    ExplainCollector.setFilterStatement(
        filter.getExpression() == null ? "none" : filter.getExpression().getStatement());
    boolean distributedCG = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.USE_DISTRIBUTED_INDEX,
            CarbonCommonConstants.USE_DISTRIBUTED_INDEX_DEFAULT));
    IndexJob indexJob = IndexUtil.getIndexJob(job.getConfiguration());
    List<PartitionSpec> partitionsToPrune = getPartitionsToPrune(job.getConfiguration());
    // First prune using default index on driver side.
    TableIndex defaultIndex = IndexStoreManager.getInstance().getDefaultIndex(carbonTable);
    List<ExtendedBlocklet> prunedBlocklets;
    // This is to log the event, so user will know what is happening by seeing logs.
    LOG.info("Started block pruning ...");
    boolean isDistributedPruningEnabled = CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName());
    if (isDistributedPruningEnabled) {
      try {
        prunedBlocklets =
            getDistributedSplit(carbonTable, filter.getResolver(), partitionsToPrune, validSegments,
                invalidSegments, segmentsToBeRefreshed, false, job.getConfiguration());
      } catch (Exception e) {
        // Check if fallback is disabled then directly throw exception otherwise try driver
        // pruning.
        if (CarbonProperties.getInstance().isFallBackDisabled()) {
          throw e;
        }
        prunedBlocklets = defaultIndex.prune(validSegments, filter, partitionsToPrune);
      }
    } else {
      if (carbonTable.isTransactionalTable()) {
        IndexExprWrapper indexExprWrapper =
            IndexChooser.getDefaultIndex(getOrCreateCarbonTable(job.getConfiguration()), null);
        IndexUtil.loadIndexes(carbonTable, indexExprWrapper, validSegments);
      }
      prunedBlocklets = defaultIndex.prune(validSegments, filter, partitionsToPrune);

      if (ExplainCollector.enabled()) {
        ExplainCollector.setDefaultIndexPruningBlockHit(getBlockCount(prunedBlocklets));
      }

      if (prunedBlocklets.size() == 0) {
        return prunedBlocklets;
      }

      IndexChooser chooser = new IndexChooser(getOrCreateCarbonTable(job.getConfiguration()));

      // Get the available CG indexes and prune further.
      IndexExprWrapper cgIndexExprWrapper = chooser.chooseCGIndex(filter.getResolver());

      if (cgIndexExprWrapper != null) {
        // Prune segments from already pruned blocklets
        IndexUtil.pruneSegments(validSegments, prunedBlocklets);
        List<ExtendedBlocklet> cgPrunedBlocklets = new ArrayList<>();
        boolean isCGPruneFallback = false;
        // Again prune with CG index.
        try {
          if (distributedCG && indexJob != null) {
            cgPrunedBlocklets = IndexUtil
                .executeIndexJob(carbonTable, filter.getResolver(), indexJob, partitionsToPrune,
                    validSegments, invalidSegments, IndexLevel.CG, new ArrayList<>(),
                    job.getConfiguration());
          } else {
            cgPrunedBlocklets = cgIndexExprWrapper.prune(validSegments, partitionsToPrune);
          }
        } catch (Exception e) {
          isCGPruneFallback = true;
          LOG.error("CG index pruning failed.", e);
        }
        // If isCGPruneFallback = true, it means that CG index pruning failed,
        // hence no need to do intersect and simply pass the prunedBlocklets from default index
        if (!isCGPruneFallback) {
          // since index index prune in segment scope,
          // the result need to intersect with previous pruned result
          prunedBlocklets =
              intersectFilteredBlocklets(carbonTable, prunedBlocklets, cgPrunedBlocklets);
        }
        if (ExplainCollector.enabled()) {
          ExplainCollector.recordCGIndexPruning(
              IndexWrapperSimpleInfo.fromIndexWrapper(cgIndexExprWrapper),
              prunedBlocklets.size(), getBlockCount(prunedBlocklets));
        }
      }

      if (prunedBlocklets.size() == 0) {
        return prunedBlocklets;
      }
      // Now try to prune with FG Index.
      if (isFgIndexPruningEnable(job.getConfiguration()) && indexJob != null) {
        IndexExprWrapper fgIndexExprWrapper = chooser.chooseFGIndex(filter.getResolver());
        List<ExtendedBlocklet> fgPrunedBlocklets;
        if (fgIndexExprWrapper != null) {
          // Prune segments from already pruned blocklets
          IndexUtil.pruneSegments(validSegments, prunedBlocklets);
          // Prune segments from already pruned blocklets
          fgPrunedBlocklets = IndexUtil
              .executeIndexJob(carbonTable, filter.getResolver(), indexJob, partitionsToPrune,
                  validSegments, invalidSegments, fgIndexExprWrapper.getIndexLevel(),
                  new ArrayList<>(), job.getConfiguration());
          // note that the 'fgPrunedBlocklets' has extra index related info compared with
          // 'prunedBlocklets', so the intersection should keep the elements in 'fgPrunedBlocklets'
          prunedBlocklets =
              intersectFilteredBlocklets(carbonTable, prunedBlocklets, fgPrunedBlocklets);
          ExplainCollector.recordFGIndexPruning(
              IndexWrapperSimpleInfo.fromIndexWrapper(fgIndexExprWrapper),
              prunedBlocklets.size(), getBlockCount(prunedBlocklets));
        }
      }
    }
    LOG.info("Finished block pruning ...");
    return prunedBlocklets;
  }

  private List<ExtendedBlocklet> intersectFilteredBlocklets(CarbonTable carbonTable,
      List<ExtendedBlocklet> previousIndexPrunedBlocklets,
      List<ExtendedBlocklet> otherIndexPrunedBlocklets) {
    List<ExtendedBlocklet> prunedBlocklets;
    if (BlockletIndexUtil.isCacheLevelBlock(carbonTable)) {
      prunedBlocklets = new ArrayList<>();
      for (ExtendedBlocklet otherBlocklet : otherIndexPrunedBlocklets) {
        if (previousIndexPrunedBlocklets.contains(otherBlocklet)) {
          prunedBlocklets.add(otherBlocklet);
        }
      }
    } else {
      prunedBlocklets = (List) CollectionUtils
          .intersection(otherIndexPrunedBlocklets, previousIndexPrunedBlocklets);
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

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException {
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
      IndexFilter indexFilter) throws IOException {
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
    if (indexFilter != null) {
      boolean hasColumnDrift = carbonTable.isTransactionalTable() && carbonTable.hasColumnDrift();
      checkAndAddImplicitExpression(indexFilter, inputSplit, hasColumnDrift);
    }
    QueryModel queryModel = new QueryModelBuilder(carbonTable)
        .projectColumns(projectColumns)
        .filterExpression(indexFilter)
        .dataConverter(getDataTypeConverter(configuration))
        .build();
    String readDeltaOnly = configuration.get(READ_ONLY_DELTA);
    if (Boolean.parseBoolean(readDeltaOnly)) {
      queryModel.setReadOnlyDelta(true);
    }
    return queryModel;
  }

  /**
   * This method will create an Implicit Expression and set it as right child in the given
   * expression
   */
  private void checkAndAddImplicitExpression(IndexFilter indexFilter, InputSplit inputSplit,
      boolean hasColumnDrift) {
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
        FilterUtil.createImplicitExpressionAndSetAsRightChild(indexFilter.getExpression(),
            blockIdToBlockletIdMapping);
        // process filter expression after adding implicit expression. If hasColumnDrift is false,
        // then the filter expression will be processed during QueryModelBuilder.build()
        if (hasColumnDrift) {
          indexFilter.processFilterExpression();
        }
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
      readSupport = new CarbonReadSupport<T>() {
        @Override
        public T readRow(Object[] data) {
          return (T) data;
        }
      };
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

  public static void setCarbonReadSupport(Configuration configuration,
      Class<? extends CarbonReadSupport> readSupportClass) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  /**
   * It is optional, if user does not set then it reads from store
   *
   * @param configuration hadoop configuration
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
    String databaseName = configuration.get(DATABASE_NAME);
    if (null == databaseName) {
      throw new InvalidConfigurationException("Database name is not set.");
    }
    return databaseName;
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

  /**
   * Project all Columns for carbon reader
   *
   * @return String array of columnNames
   */
  public String[] projectAllColumns(CarbonTable carbonTable) {
    List<ColumnSchema> colList = carbonTable.getTableInfo().getFactTable().getListOfColumns();
    List<String> projectColumns = new ArrayList<>();
    // complex type and add just the parent column name while skipping the child columns.
    for (ColumnSchema col : colList) {
      if (!col.isComplexColumn()) {
        projectColumns.add(col.getColumnName());
      }
    }
    return projectColumns.toArray(new String[projectColumns.size()]);
  }

  private static void setQuerySegmentToAccess(Configuration conf, String dbName, String tableName) {
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
    // The below change is for Secondary Index table. If CARBON_INPUT_SEGMENTS is set to main table,
    // then the same has to be reflected for index tables.
    String parentTableName = carbonTable.getParentTableName();
    if (!parentTableName.isEmpty()) {
      tableName = parentTableName;
    }
    setQuerySegmentToAccess(conf, carbonTable.getDatabaseName(), tableName);
  }

}
