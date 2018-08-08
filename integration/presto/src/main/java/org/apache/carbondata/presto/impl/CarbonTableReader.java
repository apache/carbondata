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

package org.apache.carbondata.presto.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.presto.PrestoFilterUtil;

import com.facebook.presto.hadoop.$internal.com.google.gson.Gson;
import com.facebook.presto.hadoop.$internal.io.netty.util.internal.ConcurrentSet;
import com.facebook.presto.hadoop.$internal.org.apache.commons.collections.CollectionUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * CarbonTableReader will be a facade of these utils
 * 1:CarbonMetadata,(logic table)
 * 2:FileFactory, (physic table file)
 * 3:CarbonCommonFactory, (offer some )
 * 4:DictionaryFactory, (parse dictionary util)
 * Currently, it is mainly used to parse metadata of tables under
 * the configured carbondata-store path and filter the relevant
 * input splits with given query predicates.
 */
public class CarbonTableReader {

  // default PathFilter, accepts files in carbondata format (with .carbondata extension).
  private static final PathFilter DefaultFilter = new PathFilter() {
    @Override public boolean accept(Path path) {
      return CarbonTablePath.isCarbonDataFile(path.getName());
    }
  };
  public CarbonTableConfig config;
  /**
   * The names of the tables under the schema (this.carbonFileList).
   */
  private ConcurrentSet<SchemaTableName> tableList;
  /**
   * carbonFileList represents the store path of the schema, which is configured as carbondata-store
   * in the CarbonData catalog file ($PRESTO_HOME$/etc/catalog/carbondata.properties).
   */
  private CarbonFile carbonFileList;
  private FileFactory.FileType fileType;
  /**
   * A cache for Carbon reader, with this cache,
   * metadata of a table is only read from file system once.
   */
  private AtomicReference<HashMap<SchemaTableName, CarbonTableCacheModel>> carbonCache;

  private LoadMetadataDetails[] loadMetadataDetails;

  private String queryId;

  /**
   * Logger instance
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonTableReader.class.getName());

  /**
   * List Of Schemas
   */
  private List<String> schemaNames = new ArrayList<>();

  @Inject public CarbonTableReader(CarbonTableConfig config) {
    this.config = requireNonNull(config, "CarbonTableConfig is null");
    this.carbonCache = new AtomicReference(new HashMap());
    tableList = new ConcurrentSet<>();
    setS3Properties();
    populateCarbonProperties();
  }

  /**
   * For presto worker node to initialize the metadata cache of a table.
   *
   * @param table the name of the table and schema.
   * @return
   */
  public CarbonTableCacheModel getCarbonCache(SchemaTableName table) {

    if (!carbonCache.get().containsKey(table) || carbonCache.get().get(table) == null) {
      // if this table is not cached, try to read the metadata of the table and cache it.
      try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
          FileFactory.class.getClassLoader())) {
        if (carbonFileList == null) {
          fileType = FileFactory.getFileType(config.getStorePath());
          try {
            carbonFileList = FileFactory.getCarbonFile(config.getStorePath(), fileType);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      updateSchemaTables(table);
      parseCarbonMetadata(table);
    }
    if (carbonCache.get().containsKey(table)) {
      return carbonCache.get().get(table);
    } else {
      return null;
    }
  }

  private void removeTableFromCache(SchemaTableName table) {
    DataMapStoreManager.getInstance()
        .clearDataMaps(carbonCache.get().get(table).carbonTable.getAbsoluteTableIdentifier());
    carbonCache.get().remove(table);
    tableList.remove(table);

  }

  /**
   * Return the schema names under a schema store path (this.carbonFileList).
   *
   * @return
   */
  public List<String> getSchemaNames() {
    return updateSchemaList();
  }

  /**
   * Get the CarbonFile instance which represents the store path in the configuration,
   * and assign it to this.carbonFileList.
   *
   * @return
   */
  private boolean updateCarbonFile() {
    if (carbonFileList == null) {
      fileType = FileFactory.getFileType(config.getStorePath());
      try {
        carbonFileList = FileFactory.getCarbonFile(config.getStorePath(), fileType);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return true;
  }

  /**
   * Return the schema names under a schema store path (this.carbonFileList).
   *
   * @return
   */
  private List<String> updateSchemaList() {
    updateCarbonFile();
    if (carbonFileList != null) {
      Stream.of(carbonFileList.listFiles()).forEach(this::getName);
      return schemaNames;
    } else return ImmutableList.of();
  }

  private void getName(CarbonFile carbonFile) {
    if (!carbonFile.getName().equalsIgnoreCase("_system") && !carbonFile.getName()
        .equalsIgnoreCase(".ds_store")) {
      schemaNames.add(carbonFile.getName());
    }
  }

  /**
   * Get the names of the tables in the given schema.
   *
   * @param schema name of the schema
   * @return
   */
  public Set<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    return updateTableList(schema);
  }

  /**
   * Get the names of the tables in the given schema.
   *
   * @param schemaName name of the schema
   * @return
   */
  private Set<String> updateTableList(String schemaName) {
    updateCarbonFile();
    List<CarbonFile> schema =
        Stream.of(carbonFileList.listFiles()).filter(a -> schemaName.equals(a.getName()))
            .collect(Collectors.toList());
    if (schema.size() > 0) {
      return Stream.of((schema.get(0)).listFiles()).map(CarbonFile::getName)
          .collect(Collectors.toSet());
    } else return ImmutableSet.of();
  }

  /**
   * Get the CarbonTable instance of the given table.
   *
   * @param schemaTableName name of the given table.
   * @return
   */
  public CarbonTable getTable(SchemaTableName schemaTableName) {
    try {
      updateSchemaTables(schemaTableName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    requireNonNull(schemaTableName, "schemaTableName is null");
    return loadTableMetadata(schemaTableName);
  }

  /**
   * Find all the tables under the schema store path (this.carbonFileList)
   * and cache all the table names in this.tableList. Notice that whenever this method
   * is called, it clears this.tableList and populate the list by reading the files.
   */
  private void updateSchemaTables(SchemaTableName schemaTableName) {
    // update logic determine later
    boolean isKeyExists = carbonCache.get().containsKey(schemaTableName);

    if (carbonFileList == null) {
      updateSchemaList();
    }
    try {
      if (isKeyExists
          && !FileFactory.isFileExist(
          CarbonTablePath.getSchemaFilePath(
              carbonCache.get().get(schemaTableName).carbonTable.getTablePath()), fileType)) {
        removeTableFromCache(schemaTableName);
        throw new TableNotFoundException(schemaTableName);
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }

    if (isKeyExists) {
      CarbonTableCacheModel carbonTableCacheModel = carbonCache.get().get(schemaTableName);
      if (carbonTableCacheModel != null
          && carbonTableCacheModel.carbonTable.getTableInfo() != null) {
        Long latestTime = FileFactory.getCarbonFile(CarbonTablePath
            .getSchemaFilePath(carbonCache.get().get(schemaTableName).carbonTable.getTablePath()))
            .getLastModifiedTime();
        Long oldTime = carbonTableCacheModel.carbonTable.getTableInfo().getLastUpdatedTime();
        if (DateUtils.truncate(new Date(latestTime), Calendar.MINUTE)
            .after(DateUtils.truncate(new Date(oldTime), Calendar.MINUTE))) {
          removeTableFromCache(schemaTableName);
        }
      }
    }
    if (!tableList.contains(schemaTableName)) {
      for (CarbonFile cf : carbonFileList.listFiles()) {
        if (!cf.getName().endsWith(".mdt")) {
          for (CarbonFile table : cf.listFiles()) {
            tableList.add(new SchemaTableName(cf.getName(), table.getName()));
          }
        }
      }
    }
  }

  /**
   * Find the table with the given name and build a CarbonTable instance for it.
   * This method should be called after this.updateSchemaTables().
   *
   * @param schemaTableName name of the given table.
   * @return
   */
  private CarbonTable loadTableMetadata(SchemaTableName schemaTableName) {
    for (SchemaTableName table : tableList) {
      if (!table.equals(schemaTableName)) continue;

      return parseCarbonMetadata(table);
    }
    throw new TableNotFoundException(schemaTableName);
  }

  /**
   * Read the metadata of the given table
   * and cache it in this.carbonCache (CarbonTableReader cache).
   *
   * @param table name of the given table.
   * @return the CarbonTable instance which contains all the needed metadata for a table.
   */
  private CarbonTable parseCarbonMetadata(SchemaTableName table) {
    CarbonTable result = null;
    try {
      CarbonTableCacheModel cache = carbonCache.get().get(table);
      if (cache == null) {
        cache = new CarbonTableCacheModel();
      }
      if (cache.isValid()) return cache.carbonTable;

      // If table is not previously cached, then:

      // Step 1: get store path of the table and cache it.
      // create table identifier. the table id is randomly generated.
      CarbonTableIdentifier carbonTableIdentifier =
          new CarbonTableIdentifier(table.getSchemaName(), table.getTableName(),
              UUID.randomUUID().toString());
      String storePath = config.getStorePath();
      String tablePath = storePath + "/" + carbonTableIdentifier.getDatabaseName() + "/"
          + carbonTableIdentifier.getTableName();

      //Step 2: read the metadata (tableInfo) of the table.
      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        // TBase is used to read and write thrift objects.
        // TableInfo is a kind of TBase used to read and write table information.
        // TableInfo is generated by thrift,
        // see schema.thrift under format/src/main/thrift for details.
        public TBase create() {
          return new org.apache.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
          new ThriftReader(CarbonTablePath.getSchemaFilePath(tablePath), createTBase);
      thriftReader.open();
      org.apache.carbondata.format.TableInfo tableInfo =
          (org.apache.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();

      // Step 3: convert format level TableInfo to code level TableInfo
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      // wrapperTableInfo is the code level information of a table in carbondata core,
      // different from the Thrift TableInfo.
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
              tablePath);

      // Step 4: Load metadata info into CarbonMetadata
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);

      cache.carbonTable = CarbonMetadata.getInstance().getCarbonTable(
          table.getSchemaName(), table.getTableName());

      // cache the table
      carbonCache.get().put(table, cache);

      result = cache.carbonTable;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return result;
  }

  public List<CarbonLocalMultiBlockSplit> getInputSplits2(CarbonTableCacheModel tableCacheModel,
      Expression filters, TupleDomain<ColumnHandle> constraints) throws IOException {
    List<CarbonLocalInputSplit> result = new ArrayList<>();
    List<CarbonLocalMultiBlockSplit> multiBlockSplitList = new ArrayList<>();
    CarbonTable carbonTable = tableCacheModel.carbonTable;
    TableInfo tableInfo = tableCacheModel.carbonTable.getTableInfo();
    Configuration config = FileFactory.getConfiguration();
    config.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
    String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
    config.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
    config.set(CarbonTableInputFormat.DATABASE_NAME, carbonTable.getDatabaseName());
    config.set(CarbonTableInputFormat.TABLE_NAME, carbonTable.getTableName());
    config.set("query.id", queryId);

    JobConf jobConf = new JobConf(config);
    List<PartitionSpec> filteredPartitions = new ArrayList();

    PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());

    if (partitionInfo != null && partitionInfo.getPartitionType() == PartitionType.NATIVE_HIVE) {
      try {
        loadMetadataDetails = SegmentStatusManager.readTableStatusFile(
            CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()));
      } catch (IOException exception) {
        LOGGER.error(exception.getMessage());
        throw exception;
      }
      filteredPartitions = findRequiredPartitions(constraints, carbonTable, loadMetadataDetails);
    }
    try {
      CarbonTableInputFormat.setTableInfo(config, tableInfo);
      CarbonTableInputFormat carbonTableInputFormat =
          createInputFormat(jobConf, carbonTable.getAbsoluteTableIdentifier(), filters,
              filteredPartitions);
      Job job = Job.getInstance(jobConf);
      List<InputSplit> splits = carbonTableInputFormat.getSplits(job);
      Gson gson = new Gson();
      if (splits != null && splits.size() > 0) {
        for (InputSplit inputSplit : splits) {
          CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
          result.add(new CarbonLocalInputSplit(carbonInputSplit.getSegmentId(),
              carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
              carbonInputSplit.getLength(), Arrays.asList(carbonInputSplit.getLocations()),
              carbonInputSplit.getNumberOfBlocklets(), carbonInputSplit.getVersion().number(),
              carbonInputSplit.getDeleteDeltaFiles(), carbonInputSplit.getBlockletId(),
              gson.toJson(carbonInputSplit.getDetailInfo())));
        }

        // Use block distribution
        List<List<CarbonLocalInputSplit>> inputSplits = new ArrayList(
            result.stream().map(x -> (CarbonLocalInputSplit) x).collect(Collectors.groupingBy(
                carbonInput -> carbonInput.getSegmentId().concat(carbonInput.getPath()))).values());
        if (inputSplits != null) {
          for (int j = 0; j < inputSplits.size(); j++) {
            multiBlockSplitList.add(new CarbonLocalMultiBlockSplit(inputSplits.get(j),
                inputSplits.get(j).stream().flatMap(f -> Arrays.stream(getLocations(f))).distinct()
                    .toArray(String[]::new)));
          }
        }
        LOGGER.error("Size fo MultiblockList   " + multiBlockSplitList.size());

      }

    } catch (IOException e) {
      throw new RuntimeException("Error creating Splits from CarbonTableInputFormat", e);
    }

    return multiBlockSplitList;
  }

  /**
   * Returns list of partition specs to query based on the domain constraints
   *
   * @param constraints
   * @param carbonTable
   * @throws IOException
   */
  private List<PartitionSpec> findRequiredPartitions(TupleDomain<ColumnHandle> constraints,
      CarbonTable carbonTable, LoadMetadataDetails[] loadMetadataDetails) throws IOException {
    Set<PartitionSpec> partitionSpecs = new HashSet<>();
    List<PartitionSpec> prunePartitions = new ArrayList();

    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      SegmentFileStore segmentFileStore = null;
      try {
        segmentFileStore =
            new SegmentFileStore(carbonTable.getTablePath(), loadMetadataDetail.getSegmentFile());
        partitionSpecs.addAll(segmentFileStore.getPartitionSpecs());

      } catch (IOException exception) {
        LOGGER.error(exception.getMessage());
        throw exception;
      }
    }
    List<String> partitionValuesFromExpression =
        PrestoFilterUtil.getPartitionFilters(carbonTable, constraints);

    List<PartitionSpec> partitionSpecList = partitionSpecs.stream().filter(
        partitionSpec -> CollectionUtils
            .isSubCollection(partitionValuesFromExpression, partitionSpec.getPartitions()))
        .collect(Collectors.toList());

    prunePartitions.addAll(partitionSpecList);

    return prunePartitions;
  }

  private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
      AbsoluteTableIdentifier identifier, Expression filterExpression,
      List<PartitionSpec> filteredPartitions) throws IOException {
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    CarbonTableInputFormat
        .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression);
    if (filteredPartitions.size() != 0) {
      CarbonTableInputFormat.setPartitionsToPrune(conf, filteredPartitions);
    }
    return format;
  }

  private void populateCarbonProperties() {
    addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, config.getUnsafeMemoryInMb());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION,
        config.getEnableUnsafeInQueryExecution());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        config.getEnableUnsafeColumnPage());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, config.getEnableUnsafeSort());
    addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, config.getEnableQueryStatistics());
  }

  private void setS3Properties() {
    FileFactory.getConfiguration().set(ACCESS_KEY, Objects.toString(config.getS3A_AcesssKey(), ""));
    FileFactory.getConfiguration().set(SECRET_KEY, Objects.toString(config.getS3A_SecretKey()));
    FileFactory.getConfiguration()
        .set(CarbonCommonConstants.S3_ACCESS_KEY, Objects.toString(config.getS3_AcesssKey(), ""));
    FileFactory.getConfiguration()
        .set(CarbonCommonConstants.S3_SECRET_KEY, Objects.toString(config.getS3_SecretKey()));
    FileFactory.getConfiguration()
        .set(CarbonCommonConstants.S3N_ACCESS_KEY, Objects.toString(config.getS3N_AcesssKey(), ""));
    FileFactory.getConfiguration()
        .set(CarbonCommonConstants.S3N_SECRET_KEY, Objects.toString(config.getS3N_SecretKey(), ""));
    FileFactory.getConfiguration().set(ENDPOINT, Objects.toString(config.getS3EndPoint(), ""));
  }

  private void addProperty(String propertyName, String propertyValue) {
    if (propertyValue != null) {
      CarbonProperties.getInstance().addProperty(propertyName, propertyValue);
    }
  }

  /**
   * @param cis
   * @return
   */
  private String[] getLocations(CarbonLocalInputSplit cis) {
    return cis.getLocations().toArray(new String[cis.getLocations().size()]);
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }
}
