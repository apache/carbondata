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

package org.apache.carbondata.hive;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.hive.util.DataTypeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;

public class CarbonStorageHandler implements HiveStorageHandler {

  private final Logger LOGGER =
      LogServiceFactory.getLogService(this.getClass().getName());

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return MapredCarbonInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return MapredCarbonOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return CarbonHiveSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new HiveMetaHook() {

      @Override
      public void preCreateTable(Table table) throws MetaException {

      }

      @Override
      public void rollbackCreateTable(Table table) throws MetaException {
        commitDropTable(table, false);
      }

      @Override
      public void commitCreateTable(Table table) throws MetaException {
        try {
          writeSchemaFile(getTableInfo(table));
        } catch (IOException | SQLException e) {
          LOGGER.error(e);
          throw new MetaException("Problem while writing schema file: " + e.getMessage());
        }
      }

      private TableInfo getTableInfo(Table table) throws SQLException {
        TableInfo tableInfo = new TableInfo();
        TableSchemaBuilder builder = new TableSchemaBuilder();
        builder.tableName(table.getTableName());
        List<FieldSchema> columns = table.getSd().getCols();
        String sortColumnsString = table.getParameters().get("sort_columns");
        List<String> sortColumns = new ArrayList<>();
        if (sortColumnsString != null) {
          sortColumns =
              Arrays.asList(table.getParameters().get("sort_columns").toLowerCase().split("\\,"));
        }
        List<String> partitionColumns = new ArrayList<>();
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
          partitionColumns.add(fieldSchema.getName());
        }
        PartitionInfo partitionInfo = null;
        AtomicInteger integer = new AtomicInteger();
        for (FieldSchema fieldSchema : columns) {
          ColumnSchema col = builder.addColumn(new StructField(fieldSchema.getName().toLowerCase(),
                  DataTypeUtil.convertHiveTypeToCarbon(fieldSchema.getType())),
              integer, sortColumns.contains(fieldSchema.getName()), false);
          if (partitionColumns.contains(col.getColumnName())) {
            if (partitionInfo == null) {
              partitionInfo = new PartitionInfo();
            }
            partitionInfo.addColumnSchema(col);
          }
        }
        TableSchema tableSchema = builder.build();
        SchemaEvolution schemaEvol = new SchemaEvolution();
        List<SchemaEvolutionEntry> schemaEvolutionEntry = new ArrayList<>();
        schemaEvolutionEntry.add(new SchemaEvolutionEntry());
        schemaEvol.setSchemaEvolutionEntryList(schemaEvolutionEntry);
        tableSchema.setSchemaEvolution(schemaEvol);
        for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
          tableSchema.getTableProperties()
              .put(entry.getKey().toLowerCase(), entry.getValue().toLowerCase());
        }
        tableSchema.setPartitionInfo(partitionInfo);
        tableInfo.setDatabaseName(table.getDbName());
        tableInfo.setTablePath(table.getSd().getLocation());
        tableInfo.setTransactionalTable(true);
        tableInfo.setFactTable(tableSchema);
        return tableInfo;
      }

      private void writeSchemaFile(TableInfo tableInfo) throws IOException {
        ThriftWrapperSchemaConverterImpl schemaConverter = new ThriftWrapperSchemaConverterImpl();
        CarbonFile schemaFile =
            FileFactory.getCarbonFile(CarbonTablePath.getSchemaFilePath(tableInfo.getTablePath()));
        if (!schemaFile.exists()) {
          if (!schemaFile.getParentFile().mkdirs()) {
            throw new IOException(
                "Unable to create directory: " + schemaFile.getParentFile().getAbsolutePath());
          }
        }
        ThriftWriter thriftWriter = new ThriftWriter(schemaFile.getAbsolutePath(), false);
        thriftWriter.open(FileWriteOperation.OVERWRITE);
        thriftWriter.write(schemaConverter
            .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
                tableInfo.getFactTable().getTableName()));
        thriftWriter.close();
        schemaFile.setLastModifiedTime(System.currentTimeMillis());
      }

      @Override
      public void preDropTable(Table table) throws MetaException {

      }

      @Override
      public void rollbackDropTable(Table table) throws MetaException {

      }

      @Override
      public void commitDropTable(Table table, boolean b) throws MetaException {
        FileFactory.deleteAllFilesOfDir(new File(table.getSd().getLocation()));
      }
    };
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

  }

  @Override
  public void setConf(Configuration configuration) {

  }

  @Override
  public Configuration getConf() {
    return FileFactory.getConfiguration();
  }
}
