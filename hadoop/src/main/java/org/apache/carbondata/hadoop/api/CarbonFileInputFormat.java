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
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
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
public class CarbonFileInputFormat<T> extends FileInputFormat<Void, T> implements Serializable {

  public static final String READ_SUPPORT_CLASS = "carbon.read.support.class";
  // comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";
  private static final String VALIDATE_INPUT_SEGMENT_IDs =
      "mapreduce.input.carboninputformat.validsegments";
  // comma separated list of input files
  public static final String INPUT_FILES = "mapreduce.input.carboninputformat.files";
  private static final String ALTER_PARTITION_ID = "mapreduce.input.carboninputformat.partitionid";
  private static final Log LOG = LogFactory.getLog(CarbonFileInputFormat.class);
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  private static final String CARBON_CONVERTER = "mapreduce.input.carboninputformat.converter";
  private static final String DATA_MAP_DSTR = "mapreduce.input.carboninputformat.datamapdstr";
  public static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databaseName";
  public static final String TABLE_NAME = "mapreduce.input.carboninputformat.tableName";
  private static final String PARTITIONS_TO_PRUNE =
      "mapreduce.input.carboninputformat.partitions.to.prune";
  public static final String UPADTE_T =
      "mapreduce.input.carboninputformat.partitions.to.prune";

  // a cache for carbon table, it will be used in task side
  private CarbonTable carbonTable;

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
  private static TableInfo getTableInfo(Configuration configuration) throws IOException {
    String tableInfoStr = configuration.get(TABLE_INFO);
    if (tableInfoStr == null) {
      return null;
    } else {
      TableInfo output = new TableInfo();
      output.readFields(
          new DataInputStream(
              new ByteArrayInputStream(CarbonUtil.decodeStringToBytes(tableInfoStr))));
      return output;
    }
  }


  public static void setTablePath(Configuration configuration, String tablePath) {
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

  public static DataMapJob getDataMapJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(DATA_MAP_DSTR);
    if (jobString != null) {
      return (DataMapJob) ObjectSerializationUtil.convertStringToObject(jobString);
    }
    return null;
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
    String segmentNumbersFromProperty = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tbName, "*");
    if (!segmentNumbersFromProperty.trim().equals("*")) {
      CarbonFileInputFormat
          .setSegmentsToAccess(conf, Segment.toSegmentList(segmentNumbersFromProperty.split(",")));
    }
  }

  /**
   * set list of segment to access
   */
  public static void setValidateSegmentsToAccess(Configuration configuration, Boolean validate) {
    configuration.set(CarbonFileInputFormat.VALIDATE_INPUT_SEGMENT_IDs, validate.toString());
  }

  /**
   * get list of segment to access
   */
  public static boolean getValidateSegmentsToAccess(Configuration configuration) {
    return configuration.get(CarbonFileInputFormat.VALIDATE_INPUT_SEGMENT_IDs, "true")
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
      throw new RuntimeException("Error while setting patition information to Job", e);
    }
  }

  /**
   * get list of partitions to prune
   */
  private static List<PartitionSpec> getPartitionsToPrune(Configuration configuration)
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
    //    TableDataMap blockletMap = DataMapStoreManager.getInstance()
    //        .getDataMap(identifier, BlockletDataMap.NAME, BlockletDataMapFactory.class.getName());

    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      // get all valid segments and set them into the configuration
      // check for externalTable segment (Segment_null)
      // process and resolve the expression
      Expression filter = getFilterPredicates(job.getConfiguration());
      TableProvider tableProvider = new SingleTableProvider(carbonTable);
      // this will be null in case of corrupt schema file.
      PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());
      CarbonInputFormatUtil.processFilterExpression(filter, carbonTable, null, null);

      FilterResolverIntf filterInterface = CarbonInputFormatUtil
          .resolveFilter(filter, carbonTable.getAbsoluteTableIdentifier(), tableProvider);

      String segmentDir = CarbonTablePath.getSegmentPath(identifier.getTablePath(), "null");
      FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
      if (FileFactory.isFileExist(segmentDir, fileType)) {
        // if external table Segments are found, add it to the List
        List<Segment> externalTableSegments = new ArrayList<Segment>();
        Segment seg = new Segment("null", null);
        externalTableSegments.add(seg);

        Map<String, String> indexFiles =
            new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir);

        if (indexFiles.size() == 0) {
          throw new RuntimeException("Index file not present to read the carbondata file");
        }
        // do block filtering and get split
        List<InputSplit> splits =
            getSplits(job, filterInterface, externalTableSegments, null, partitionInfo, null);

        return splits;
      }
    }
    return null;
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
      List<Segment> validSegments, BitSet matchedPartitions, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    List<InputSplit> result = new LinkedList<InputSplit>();
    UpdateVO invalidBlockVOForSegmentId = null;
    Boolean isIUDTable = false;

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getOrCreateCarbonTable(job.getConfiguration()).getAbsoluteTableIdentifier();
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);

    isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    // for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, absoluteTableIdentifier, filterResolver, matchedPartitions,
            validSegments, partitionInfo, oldPartitionIdList);
    for (CarbonInputSplit inputSplit : dataBlocksOfSegment) {

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
      BitSet matchedPartitions, List<Segment> segmentIds, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { new Path(absoluteTableIdentifier.getTablePath()) }, job.getConfiguration());
    boolean distributedCG = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
            CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT));
    DataMapExprWrapper dataMapExprWrapper =
        DataMapChooser.get().choose(getOrCreateCarbonTable(job.getConfiguration()), resolver);
    DataMapJob dataMapJob = getDataMapJob(job.getConfiguration());
    List<PartitionSpec> partitionsToPrune = getPartitionsToPrune(job.getConfiguration());
    List<ExtendedBlocklet> prunedBlocklets;
    if (distributedCG || dataMapExprWrapper.getDataMapType() == DataMapLevel.FG) {
      DistributableDataMapFormat datamapDstr =
          new DistributableDataMapFormat(absoluteTableIdentifier, dataMapExprWrapper,
              segmentIds, partitionsToPrune,
              BlockletDataMapFactory.class.getName());
      prunedBlocklets = dataMapJob.execute(datamapDstr, resolver);
      // Apply expression on the blocklets.
      prunedBlocklets = dataMapExprWrapper.pruneBlocklets(prunedBlocklets);
    } else {
      prunedBlocklets = dataMapExprWrapper.prune(segmentIds, partitionsToPrune);
    }

    List<CarbonInputSplit> resultFilterredBlocks = new ArrayList<>();
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
    CarbonInputSplit split =
        CarbonInputSplit.from(blocklet.getSegmentId(),
            blocklet.getBlockletId(), new FileSplit(new Path(blocklet.getPath()), 0,
                blocklet.getLength(), blocklet.getLocations()),
            ColumnarFormatVersion.valueOf((short) blocklet.getDetailInfo().getVersionNumber()),
            blocklet.getDataMapWriterPath());
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
    String projectionString = getColumnProjection(configuration);
    String[] projectionColumnNames = null;
    if (projectionString != null) {
      projectionColumnNames = projectionString.split(",");
    }
    QueryModel queryModel = carbonTable.createQueryWithProjection(
        projectionColumnNames, getDataTypeConverter(configuration));

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

  private CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
    CarbonTable carbonTableTemp;
    if (carbonTable == null) {
      // carbon table should be created either from deserialized table info (schema saved in
      // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
      TableInfo tableInfo = getTableInfo(configuration);
      CarbonTable localCarbonTable;
      if (tableInfo != null) {
        localCarbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      } else {
        String schemaPath = CarbonTablePath
            .getSchemaFilePath(getAbsoluteTableIdentifier(configuration).getTablePath());
        if (!FileFactory.isFileExist(schemaPath, FileFactory.getFileType(schemaPath))) {
          TableInfo tableInfoInfer =
              SchemaReader.inferSchemaForExternalTable(getAbsoluteTableIdentifier(configuration));
          localCarbonTable = CarbonTable.buildFromTableInfo(tableInfoInfer);
        } else {
          localCarbonTable =
              SchemaReader.readCarbonTableFromStore(getAbsoluteTableIdentifier(configuration));
        }
      }
      this.carbonTable = localCarbonTable;
      return localCarbonTable;
    } else {
      carbonTableTemp = this.carbonTable;
      return carbonTableTemp;
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

  public static DataTypeConverter getDataTypeConverter(Configuration configuration)
      throws IOException {
    String converter = configuration.get(CARBON_CONVERTER);
    if (converter == null) {
      return new DataTypeConverterImpl();
    }
    return (DataTypeConverter) ObjectSerializationUtil.convertStringToObject(converter);
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

  public org.apache.hadoop.mapred.RecordReader<Void, T> getRecordReader(
      org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return null;
  }
}
