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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.service.impl.PathFactory;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

import com.facebook.presto.hadoop.$internal.com.google.gson.Gson;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;

import static java.util.Objects.requireNonNull;

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
  private CarbonTableConfig config;
  /**
   * The names of the tables under the schema (this.carbonFileList).
   */
  private List<SchemaTableName> tableList;
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
  private ConcurrentHashMap<SchemaTableName, CarbonTableCacheModel> cc;

  @Inject public CarbonTableReader(CarbonTableConfig config) {
    this.config = requireNonNull(config, "CarbonTableConfig is null");
    this.cc = new ConcurrentHashMap<>();
    tableList = new LinkedList<>();
  }

  /**
   * For presto worker node to initialize the metadata cache of a table.
   *
   * @param table the name of the table and schema.
   * @return
   */
  public CarbonTableCacheModel getCarbonCache(SchemaTableName table) {

    if (!cc.containsKey(table) || cc.get(table) == null) {
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
    if (cc.containsKey(table)) {
      return cc.get(table);
    } else {
      return null;
    }
  }

  private void removeTableFromCache(SchemaTableName table) {
    DataMapStoreManager.getInstance().clearDataMaps(cc.get(table).carbonTable.getAbsoluteTableIdentifier());
    cc.remove(table);
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
   * Get the CarbonFile instance which represents the store path in the configuration, and assign it to
   * this.carbonFileList.
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
      return Stream.of(carbonFileList.listFiles()).map(CarbonFile::getName).collect(Collectors.toList());
    } else return ImmutableList.of();
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
    CarbonTable table = loadTableMetadata(schemaTableName);

    return table;
  }

  /**
   * Find all the tables under the schema store path (this.carbonFileList)
   * and cache all the table names in this.tableList. Notice that whenever this method
   * is called, it clears this.tableList and populate the list by reading the files.
   */
  private void updateSchemaTables(SchemaTableName schemaTableName) {
// update logic determine later
    boolean isKeyExists = cc.containsKey(schemaTableName);

    if (carbonFileList == null) {
      updateSchemaList();
    }
    try {
      if(isKeyExists && !FileFactory.isFileExist(cc.get(schemaTableName).carbonTablePath.getSchemaFilePath(),fileType)){
        removeTableFromCache(schemaTableName);
        throw new TableNotFoundException(schemaTableName);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
    if(isKeyExists && FileFactory.getCarbonFile(cc.get(schemaTableName).carbonTablePath.getPath()).getLastModifiedTime() > cc.get(schemaTableName).tableInfo.getLastUpdatedTime()){
      removeTableFromCache(schemaTableName);
    }
    if(!tableList.contains(schemaTableName)) {
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
   * Read the metadata of the given table and cache it in this.cc (CarbonTableReader cache).
   *
   * @param table name of the given table.
   * @return the CarbonTable instance which contains all the needed metadata for a table.
   */
  private CarbonTable parseCarbonMetadata(SchemaTableName table) {
    CarbonTable result = null;
    try {
      CarbonTableCacheModel cache = cc.getOrDefault(table, new CarbonTableCacheModel());
      if (cache.isValid()) return cache.carbonTable;

      // If table is not previously cached, then:

      // Step 1: get store path of the table and cache it.
      // create table identifier. the table id is randomly generated.
      cache.carbonTableIdentifier =
              new CarbonTableIdentifier(table.getSchemaName(), table.getTableName(),
                      UUID.randomUUID().toString());
      String storePath = config.getStorePath();
      String tablePath = storePath + "/" + cache.carbonTableIdentifier.getDatabaseName() + "/"
          + cache.carbonTableIdentifier.getTableName();

      // get the store path of the table.

      AbsoluteTableIdentifier absoluteTableIdentifier =
          new AbsoluteTableIdentifier(tablePath, cache.carbonTableIdentifier);
      cache.carbonTablePath =
          PathFactory.getInstance().getCarbonTablePath(absoluteTableIdentifier, null);
      // cache the table
      cc.put(table, cache);

      //Step 2: read the metadata (tableInfo) of the table.
      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        // TBase is used to read and write thrift objects.
        // TableInfo is a kind of TBase used to read and write table information.
        // TableInfo is generated by thrift, see schema.thrift under format/src/main/thrift for details.
        public TBase create() {
          return new org.apache.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
              new ThriftReader(cache.carbonTablePath.getSchemaFilePath(), createTBase);
      thriftReader.open();
      org.apache.carbondata.format.TableInfo tableInfo =
              (org.apache.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();


      // Step 3: convert format level TableInfo to code level TableInfo
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      // wrapperTableInfo is the code level information of a table in carbondata core, different from the Thrift TableInfo.
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
              tablePath);
      wrapperTableInfo.setMetaDataFilepath(
              CarbonTablePath.getFolderContainingFile(cache.carbonTablePath.getSchemaFilePath()));

      // Step 4: Load metadata info into CarbonMetadata
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);

      cache.tableInfo = wrapperTableInfo;
      cache.carbonTable = CarbonMetadata.getInstance()
              .getCarbonTable(cache.carbonTableIdentifier.getTableUniqueName());
      result = cache.carbonTable;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return result;
  }


  public List<CarbonLocalInputSplit> getInputSplits2(CarbonTableCacheModel tableCacheModel,
                                                     Expression filters)  {
    List<CarbonLocalInputSplit> result = new ArrayList<>();

    CarbonTable carbonTable = tableCacheModel.carbonTable;
    TableInfo tableInfo = tableCacheModel.tableInfo;
    Configuration config = new Configuration();
    config.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
    String carbonTablePath = PathFactory.getInstance()
        .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier(), null).getPath();
    config.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
    config.set(CarbonTableInputFormat.DATABASE_NAME, carbonTable.getDatabaseName());
    config.set(CarbonTableInputFormat.TABLE_NAME, carbonTable.getTableName());

    try {
      CarbonTableInputFormat.setTableInfo(config, tableInfo);
      CarbonTableInputFormat carbonTableInputFormat =
              createInputFormat(config, carbonTable.getAbsoluteTableIdentifier(), filters);
      JobConf jobConf = new JobConf(config);
      Job job = Job.getInstance(jobConf);
      List<InputSplit> splits = carbonTableInputFormat.getSplits(job);
      CarbonInputSplit carbonInputSplit = null;
      Gson gson = new Gson();
      if (splits != null && splits.size() > 0) {
        for (InputSplit inputSplit : splits) {
          carbonInputSplit = (CarbonInputSplit) inputSplit;
          result.add(new CarbonLocalInputSplit(carbonInputSplit.getSegmentId(),
                  carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                  carbonInputSplit.getLength(), Arrays.asList(carbonInputSplit.getLocations()),
                  carbonInputSplit.getNumberOfBlocklets(), carbonInputSplit.getVersion().number(),
                  carbonInputSplit.getDeleteDeltaFiles(),
                  gson.toJson(carbonInputSplit.getDetailInfo())));
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Error creating Splits from CarbonTableInputFormat", e);
    }

    return result;
  }

  private CarbonTableInputFormat<Object>  createInputFormat( Configuration conf, AbsoluteTableIdentifier identifier, Expression filterExpression)
          throws IOException {
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    CarbonTableInputFormat.setTablePath(conf,
            identifier.appendWithLocalPrefix(identifier.getTablePath()));
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression);

    return format;
  }


}