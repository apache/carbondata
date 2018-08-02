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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.service.model.ScanRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

/**
 * Provides table management.
 */
@InterfaceAudience.Internal
public class TableManager {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(TableManager.class.getCanonicalName());

  private StoreConf storeConf;

  // mapping of table path to CarbonTable object
  private Map<String, CarbonTable> cache = new HashMap<>();

  public TableManager(StoreConf storeConf) {
    this.storeConf = storeConf;
  }

  public void createTable(TableDescriptor descriptor) throws CarbonException {
    TableIdentifier table = descriptor.getTable();
    Field[] fields = descriptor.getSchema().getFields();
    // sort_columns
    List<String> sortColumnsList = null;
    try {
      sortColumnsList = descriptor.getSchema().prepareSortColumns(descriptor.getProperties());
    } catch (MalformedCarbonCommandException e) {
      throw new CarbonException(e.getMessage());
    }
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];

    TableSchemaBuilder builder = TableSchema.builder();
    CarbonWriterBuilder.buildTableSchema(fields, builder, sortColumnsList, sortColumnsSchemaList);

    TableSchema schema = builder.tableName(table.getTableName())
        .properties(descriptor.getProperties())
        .setSortColumns(Arrays.asList(sortColumnsSchemaList))
        .build();

    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    schema.getSchemaEvolution().getSchemaEvolutionEntryList().add(schemaEvolutionEntry);
    schema.setTableName(table.getTableName());

    String tablePath = descriptor.getTablePath();
    if (tablePath == null) {
      tablePath = getTablePath(table.getTableName(), table.getDatabaseName());
    }

    TableInfo tableInfo = CarbonTable.builder()
        .databaseName(table.getDatabaseName())
        .tableName(table.getTableName())
        .tablePath(tablePath)
        .tableSchema(schema)
        .isTransactionalTable(true)
        .buildTableInfo();

    try {
      createTable(tableInfo, descriptor.isIfNotExists());
    } catch (IOException e) {
      LOGGER.error(e, "create tableDescriptor failed");
      throw new CarbonException(e.getMessage());
    }
  }

  private void createTable(TableInfo tableInfo, boolean ifNotExists) throws IOException {
    AbsoluteTableIdentifier identifier = tableInfo.getOrCreateAbsoluteTableIdentifier();
    boolean tableExists = FileFactory.isFileExist(identifier.getTablePath());
    if (tableExists) {
      if (ifNotExists) {
        return;
      } else {
        throw new IOException(
            "car't create table " + tableInfo.getDatabaseName() + "." + tableInfo.getFactTable()
                .getTableName() + ", because it already exists");
      }
    }

    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    String databaseName = tableInfo.getDatabaseName();
    String tableName = tableInfo.getFactTable().getTableName();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(tableInfo, databaseName, tableName);

    String schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    try {
      if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
        boolean isDirCreated = FileFactory.mkdirs(schemaMetadataPath, fileType);
        if (!isDirCreated) {
          throw new IOException("Failed to create the metadata directory " + schemaMetadataPath);
        }
      }
      ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
      thriftWriter.open(FileWriteOperation.OVERWRITE);
      thriftWriter.write(thriftTableInfo);
      thriftWriter.close();
    } catch (IOException e) {
      LOGGER.error(e, "Failed to handle create table");
      throw e;
    }
  }

  public void dropTable(TableIdentifier table) throws CarbonException {
    String tablePath = getTablePath(table.getTableName(), table.getDatabaseName());
    cache.remove(tablePath);
    try {
      FileFactory.deleteFile(tablePath);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  public CarbonTable getTable(TableIdentifier table) throws CarbonException {
    String tablePath = getTablePath(table.getTableName(), table.getDatabaseName());
    if (cache.containsKey(tablePath)) {
      return cache.get(tablePath);
    } else {
      org.apache.carbondata.format.TableInfo formatTableInfo = null;
      try {
        formatTableInfo = CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(tablePath));
      } catch (IOException e) {
        throw new CarbonException(e);
      }
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo tableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          formatTableInfo, table.getDatabaseName(), table.getTableName(), tablePath);
      tableInfo.setTablePath(tablePath);
      CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      cache.put(tablePath, carbonTable);
      return carbonTable;
    }
  }

  public List<TableDescriptor> listTable() throws CarbonException {
    throw new UnsupportedOperationException();
  }

  public TableDescriptor getDescriptor(TableIdentifier table) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  public void alterTable(TableIdentifier table, TableDescriptor newTable) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  public String getTablePath(String tableName, String databaseName) {
    Objects.requireNonNull(tableName);
    Objects.requireNonNull(databaseName);
    return String.format("%s/%s", storeConf.storeLocation(), tableName);
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of host address to list of block.
   * This should be invoked in driver side.
   */
  static List<Distributable> pruneBlock(CarbonTable table, Expression filter) throws IOException {
    Objects.requireNonNull(table);
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat format;
    try {
      // We just want to do pruning, so passing empty projection columns
      format = CarbonInputFormatUtil.createCarbonTableInputFormat(
          job, table, new String[0], filter, null, null, true);
    } catch (InvalidConfigurationException e) {
      throw new IOException(e.getMessage());
    }

    // We will do FG pruning in reader side, so don't do it here
    CarbonInputFormat.setFgDataMapPruning(job.getConfiguration(), false);
    List<InputSplit> splits = format.getSplits(job);
    List<Distributable> blockInfos = new ArrayList<>(splits.size());
    for (InputSplit split : splits) {
      blockInfos.add((Distributable) split);
    }
    return blockInfos;
  }

  /**
   * Scan data and return matched rows. This should be invoked in worker side.
   * @param table carbon table
   * @param scan scan parameter
   * @return matched rows
   * @throws IOException if IO error occurs
   */
  public static List<CarbonRow> scan(CarbonTable table, ScanRequest scan) throws IOException {
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(System.nanoTime());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);

    CarbonMultiBlockSplit mbSplit = scan.getSplit();
    long limit = scan.getLimit();
    QueryModel queryModel = createQueryModel(table, scan);

    LOGGER.info(String.format("[QueryId:%d] %s, number of block: %d", scan.getRequestId(),
        queryModel.toString(), mbSplit.getAllSplits().size()));

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try (CarbonRecordReader<CarbonRow> reader = new IndexedRecordReader(scan.getRequestId(),
        table, queryModel)) {
      reader.initialize(mbSplit, null);

      // loop to read required number of rows.
      // By default, if user does not specify the limit value, limit is Long.MaxValue
      long rowCount = 0;
      while (reader.nextKeyValue() && rowCount < limit) {
        rows.add(reader.getCurrentValue());
        rowCount++;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOGGER.info(String.format("[QueryId:%d] scan completed, return %d rows",
        scan.getRequestId(), rows.size()));
    return rows;
  }

  private static QueryModel createQueryModel(CarbonTable table, ScanRequest scan) {
    String[] projectColumns = scan.getProjectColumns();
    Expression filter = null;
    if (scan.getFilterExpression() != null) {
      filter = scan.getFilterExpression();
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }
}
