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

package org.apache.carbondata.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.format.SchemaEvolutionEntry;
import org.apache.carbondata.store.api.Table;

public class TableBuilder {

  private String databaseName;
  private String tableName;
  private String tablePath;
  private TableSchema tableSchema;

  private TableBuilder() { }

  public static TableBuilder newInstance() {
    return new TableBuilder();
  }

  public Table create() throws IOException {
    if (tableName == null || tablePath == null || tableSchema == null) {
      throw new IllegalArgumentException("must provide table name and table path");
    }

    if (databaseName == null) {
      databaseName = "default";
    }

    TableInfo tableInfo = new TableInfo();
    tableInfo.setDatabaseName(databaseName);
    tableInfo.setTableUniqueName(databaseName + "_" + tableName);
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTablePath(tablePath);
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setDataMapSchemaList(new ArrayList<DataMapSchema>(0));
    AbsoluteTableIdentifier identifier = tableInfo.getOrCreateAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(
        identifier.getTablePath(),
        identifier.getCarbonTableIdentifier());
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(
            tableInfo,
            tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new SchemaEvolutionEntry(
            tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }
    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    CarbonTable table = CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
    return new TableImpl(table);
  }

  public TableBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TableBuilder tableSchema(TableSchema schema) {
    if (tableName == null) {
      throw new IllegalArgumentException("set table name first");
    }
    validateSchema(schema.getListOfColumns());
    schema.setTableName(tableName);
    this.tableSchema = schema;
    return this;
  }

  // check whether there are duplicated column
  private void validateSchema(List<ColumnSchema> schema) {
    Set<String> fieldNames = new HashSet<>();
    for (ColumnSchema field : schema) {
      if (fieldNames.contains(field.getColumnName())) {
        throw new IllegalArgumentException(
            "Duplicated column found, column name " + field.getColumnName());
      }
      fieldNames.add(field.getColumnName());
    }
  }

  public TableBuilder tablePath(String tablePath) {
    this.tablePath = tablePath;
    return this;
  }

}
