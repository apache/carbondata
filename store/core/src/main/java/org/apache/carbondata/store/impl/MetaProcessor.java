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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.store.api.descriptor.TableDescriptor;
import org.apache.carbondata.store.api.descriptor.TableIdentifier;
import org.apache.carbondata.store.api.exception.StoreException;

class MetaProcessor {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(MetaProcessor.class.getCanonicalName());

  private CarbonStoreBase store;

  MetaProcessor(CarbonStoreBase store) {
    this.store = store;
  }

  // mapping of table path to CarbonTable object
  private Map<String, CarbonTable> cache = new HashMap<>();

  public void createTable(TableDescriptor descriptor) throws StoreException {
    Field[] fields = descriptor.getSchema().getFields();
    // sort_columns
    List<String> sortColumnsList = null;
    try {
      sortColumnsList = descriptor.getSchema().prepareSortColumns(descriptor.getProperties());
    } catch (MalformedCarbonCommandException e) {
      throw new StoreException(e.getMessage());
    }
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];

    TableSchemaBuilder builder = TableSchema.builder();
    CarbonWriterBuilder.buildTableSchema(fields, builder, sortColumnsList, sortColumnsSchemaList);

    TableSchema schema = builder.tableName(descriptor.getTable().getTableName())
        .properties(descriptor.getProperties())
        .setSortColumns(Arrays.asList(sortColumnsSchemaList))
        .build();

    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    schema.getSchemaEvolution().getSchemaEvolutionEntryList().add(schemaEvolutionEntry);
    schema.setTableName(descriptor.getTable().getTableName());

    String tablePath = descriptor.getTablePath();
    if (tablePath == null) {
      tablePath = store.getTablePath(
          descriptor.getTable().getTableName(), descriptor.getTable().getDatabaseName());
    }

    TableInfo tableInfo = CarbonTable.builder()
        .databaseName(descriptor.getTable().getDatabaseName())
        .tableName(descriptor.getTable().getTableName())
        .tablePath(tablePath)
        .tableSchema(schema)
        .isTransactionalTable(true)
        .buildTableInfo();

    try {
      createTable(tableInfo, descriptor.isIfNotExists());
    } catch (IOException e) {
      LOGGER.error(e, "create tableDescriptor failed");
      throw new StoreException(e.getMessage());
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

  public void dropTable(TableIdentifier table) throws IOException {
    String tablePath = store.getTablePath(table.getTableName(), table.getDatabaseName());
    cache.remove(tablePath);
    FileFactory.deleteFile(tablePath);
  }

  public CarbonTable getTable(TableIdentifier table) throws IOException {
    String tablePath = store.getTablePath(table.getTableName(), table.getDatabaseName());
    if (cache.containsKey(tablePath)) {
      return cache.get(tablePath);
    } else {
      org.apache.carbondata.format.TableInfo formatTableInfo =
          CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(tablePath));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo tableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          formatTableInfo, "", "", "");
      tableInfo.setTablePath(tablePath);
      CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      cache.put(tablePath, carbonTable);
      return carbonTable;
    }
  }
}