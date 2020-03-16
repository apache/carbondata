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

package org.apache.carbondata.hive.util;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataLoadMetrics;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.log4j.Logger;

public class HiveCarbonUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(HiveCarbonUtil.class.getName());

  public static CarbonLoadModel getCarbonLoadModel(Configuration tableProperties) {
    String[] tableUniqueName = tableProperties.get("name").split("\\.");
    String databaseName = tableUniqueName[0];
    String tableName = tableUniqueName[1];
    String tablePath = tableProperties.get(hive_metastoreConstants.META_TABLE_LOCATION);
    String columns = tableProperties.get(hive_metastoreConstants.META_TABLE_COLUMNS);
    String sortColumns = tableProperties.get("sort_columns");
    String columnTypes = tableProperties.get(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);
    String partitionColumns =
        tableProperties.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String partitionColumnTypes =
        tableProperties.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
    if (partitionColumns != null) {
      columns = columns + "," + partitionColumns;
      columnTypes = columnTypes + ":" + partitionColumnTypes;
    }
    String[] columnTypeArray = splitSchemaStringToArray(columnTypes);
    String complexDelim = tableProperties.get("complex_delimiter", "");
    CarbonLoadModel carbonLoadModel =
        getCarbonLoadModel(tableName, databaseName, tablePath, sortColumns, columns.split(","),
            columnTypeArray, tableProperties);
    carbonLoadModel.setCarbonTransactionalTable(true);
    carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().setTransactionalTable(true);
    for (String delim : complexDelim.split(",")) {
      carbonLoadModel.setComplexDelimiter(delim);
    }
    return carbonLoadModel;
  }

  public static CarbonLoadModel getCarbonLoadModel(Properties tableProperties,
      Configuration configuration) {
    String[] tableUniqueName = tableProperties.getProperty("name").split("\\.");
    String databaseName = tableUniqueName[0];
    String tableName = tableUniqueName[1];
    String tablePath = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
    String columns = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_COLUMNS);
    String sortColumns = tableProperties.getProperty("sort_columns");
    String[] columnTypes = splitSchemaStringToArray(tableProperties.getProperty("columns.types"));
    String complexDelim = tableProperties.getProperty("complex_delimiter", "");
    CarbonLoadModel carbonLoadModel =
        getCarbonLoadModel(tableName, databaseName, tablePath, sortColumns, columns.split(","),
            columnTypes, configuration);
    for (String delim : complexDelim.split(",")) {
      carbonLoadModel.setComplexDelimiter(delim);
    }
    return carbonLoadModel;
  }

  public static CarbonLoadModel getCarbonLoadModel(String tableName, String databaseName,
      String location, String sortColumnsString, String[] columns, String[] columnTypes,
      Configuration configuration) {
    CarbonLoadModel loadModel;
    CarbonTable carbonTable;
    try {
      String schemaFilePath = CarbonTablePath.getSchemaFilePath(location, configuration);
      AbsoluteTableIdentifier absoluteTableIdentifier =
          AbsoluteTableIdentifier.from(location, databaseName, tableName, "");
      if (FileFactory.getCarbonFile(schemaFilePath).exists()) {
        carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
        carbonTable.setTransactionalTable(true);
      } else {
        String carbonDataFile = CarbonUtil.getFilePathExternalFilePath(location, configuration);
        if (carbonDataFile == null) {
          carbonTable = CarbonTable.buildFromTableInfo(
              getTableInfo(tableName, databaseName, location, sortColumnsString, columns,
                  columnTypes, new ArrayList<>()));
        } else {
          carbonTable = CarbonTable.buildFromTableInfo(
              SchemaReader.inferSchema(absoluteTableIdentifier, false, configuration));
        }
        carbonTable.setTransactionalTable(false);
      }
    } catch (SQLException | IOException e) {
      throw new RuntimeException("Unable to fetch schema for the table: " + tableName, e);
    }
    CarbonLoadModelBuilder carbonLoadModelBuilder = new CarbonLoadModelBuilder(carbonTable);
    Map<String, String> options = new HashMap<>();
    options.put("fileheader", Strings.mkString(columns, ","));
    try {
      loadModel = carbonLoadModelBuilder.build(options, System.currentTimeMillis(), "");
    } catch (InvalidLoadOptionException | IOException e) {
      throw new RuntimeException(e);
    }
    loadModel.setSkipParsers();
    loadModel.setMetrics(new DataLoadMetrics());
    return loadModel;
  }

  public static CarbonTable getCarbonTable(Configuration tableProperties) throws SQLException {
    String[] tableUniqueName = tableProperties.get("name").split("\\.");
    String databaseName = tableUniqueName[0];
    String tableName = tableUniqueName[1];
    String tablePath = tableProperties.get(hive_metastoreConstants.META_TABLE_LOCATION);
    String columns = tableProperties.get(hive_metastoreConstants.META_TABLE_COLUMNS);
    String sortColumns = tableProperties.get("sort_columns");
    String columnTypes = tableProperties.get(hive_metastoreConstants.META_TABLE_COLUMN_TYPES);
    String partitionColumns =
        tableProperties.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    String partitionColumnTypes =
        tableProperties.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
    if (partitionColumns != null) {
      columns = columns + "," + partitionColumns;
      columnTypes = columnTypes + ":" + partitionColumnTypes;
    }
    String[][] validatedColumnsAndTypes = validateColumnsAndTypes(columns, columnTypes);
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(
        HiveCarbonUtil.getTableInfo(tableName, databaseName, tablePath,
            sortColumns, validatedColumnsAndTypes[0],
            validatedColumnsAndTypes[1], new ArrayList<>()));
    carbonTable.setTransactionalTable(false);
    return carbonTable;
  }

  // In case of empty table some extra field is getting added in the columns and columntypes
  // which should be removed after validation.
  private static String[][] validateColumnsAndTypes(String columns, String columnTypes) {
    String[] columnTypeArray = HiveCarbonUtil.splitSchemaStringToArray(columnTypes);
    String[] columnArray = columns.split(",");
    String[] validatedColumnArray;
    String[] validatedColumnTypeArray;
    int length = columnArray.length;
    if (columnArray[length - 3].equalsIgnoreCase("BLOCK__OFFSET__INSIDE__FILE")) {
      validatedColumnArray = new String[length - 3];
      validatedColumnTypeArray = new String[length - 3];
      System.arraycopy(columnArray, 0, validatedColumnArray, 0, length - 3);
      System.arraycopy(columnTypeArray, 0, validatedColumnTypeArray, 0, length - 3);
    } else {
      validatedColumnArray = columnArray;
      validatedColumnTypeArray = columnTypeArray;
    }
    return new String[][]{validatedColumnArray, validatedColumnTypeArray};
  }

  private static TableInfo getTableInfo(String tableName, String databaseName, String location,
      String sortColumnsString, String[] columns, String[] columnTypes,
      List<String> partitionColumns) throws SQLException {
    TableInfo tableInfo = new TableInfo();
    TableSchemaBuilder builder = new TableSchemaBuilder();
    builder.tableName(tableName);
    List<String> sortColumns = new ArrayList<>();
    if (sortColumnsString != null) {
      sortColumns = Arrays.asList(sortColumnsString.toLowerCase().split("\\,"));
    }
    PartitionInfo partitionInfo = null;
    AtomicInteger integer = new AtomicInteger();
    List<StructField> partitionStructFields = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      DataType dataType = DataTypeUtil.convertHiveTypeToCarbon(columnTypes[i]);
      Field field = new Field(columns[i].toLowerCase(), dataType);
      if (partitionColumns.contains(columns[i])) {
        partitionStructFields
            .add(new StructField(columns[i].toLowerCase(), dataType, field.getChildren()));
      } else {
        builder.addColumn(new StructField(columns[i].toLowerCase(), dataType, field.getChildren()),
            integer, sortColumns.contains(columns[i]), false);
      }
    }
    if (!partitionStructFields.isEmpty()) {
      List<ColumnSchema> partitionColumnSchemas = new ArrayList<>();
      for (StructField partitionStructField : partitionStructFields) {
        partitionColumnSchemas.add(builder.addColumn(partitionStructField, integer,
            sortColumns.contains(partitionStructField.getFieldName()), false));
      }
      partitionInfo = new PartitionInfo(partitionColumnSchemas, PartitionType.NATIVE_HIVE);
    }
    TableSchema tableSchema = builder.build();
    SchemaEvolution schemaEvol = new SchemaEvolution();
    List<SchemaEvolutionEntry> schemaEvolutionEntry = new ArrayList<>();
    schemaEvolutionEntry.add(new SchemaEvolutionEntry());
    schemaEvol.setSchemaEvolutionEntryList(schemaEvolutionEntry);
    tableSchema.setSchemaEvolution(schemaEvol);
    tableSchema.setPartitionInfo(partitionInfo);
    tableInfo.setDatabaseName(databaseName);
    tableInfo.setTablePath(location);
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTableUniqueName(databaseName + "_" + tableName);
    return tableInfo;
  }

  private static void writeSchemaFile(TableInfo tableInfo) throws IOException {
    ThriftWrapperSchemaConverterImpl schemaConverter = new ThriftWrapperSchemaConverterImpl();
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(tableInfo.getTablePath());
    String metadataPath = CarbonTablePath.getMetadataPath(tableInfo.getTablePath());
    FileFactory.mkdirs(metadataPath);
    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open(FileWriteOperation.OVERWRITE);
    thriftWriter.write(schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName()));
    thriftWriter.close();
  }

  public static HiveMetaHook getMetaHook() {
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
          List<FieldSchema> fieldSchemas = table.getSd().getCols();
          String[] columns = new String[fieldSchemas.size() + table.getPartitionKeys().size()];
          String[] columnTypes = new String[fieldSchemas.size() + table.getPartitionKeys().size()];
          int i = 0;
          for (FieldSchema fieldSchema : table.getSd().getCols()) {
            columns[i] = fieldSchema.getName();
            columnTypes[i++] = fieldSchema.getType();
          }
          List<String> partitionColumns = new ArrayList<>();
          for (FieldSchema partitionCol : table.getPartitionKeys()) {
            columns[i] = partitionCol.getName().toLowerCase();
            columnTypes[i++] = partitionCol.getType();
            partitionColumns.add(partitionCol.getName().toLowerCase());
          }
          TableInfo tableInfo =
              getTableInfo(table.getTableName(), table.getDbName(), table.getSd().getLocation(),
                  table.getParameters().getOrDefault("sort_columns", ""), columns, columnTypes,
                  partitionColumns);
          tableInfo.getFactTable().getTableProperties().putAll(table.getParameters());
          writeSchemaFile(tableInfo);
        } catch (IOException | SQLException e) {
          LOGGER.error(e);
          throw new MetaException("Problem while writing schema file: " + e.getMessage());
        }
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

  public static String[] splitSchemaStringToArray(String schema) {
    List<String> tokens = new ArrayList();
    StringBuilder stack = new StringBuilder();
    int openingCount = 0;
    int length = schema.length();
    for (int i = 0; i < length; i++) {
      if (schema.charAt(i) == '(') {
        stack.append(schema.charAt(i));
        while (++i < length && schema.charAt(i) != ')') {
          stack.append(schema.charAt(i));
        }
        stack.append(schema.charAt(i));
      } else if (schema.charAt(i) == '<') {
        openingCount++;
        stack.append(schema.charAt(i));
      } else if (schema.charAt(i) == '>') {
        --openingCount;
        if (i == schema.length() - 1) {
          stack.append(schema.charAt(i));
          tokens.add(stack.toString());
          stack = new StringBuilder();
          openingCount = 0;
        } else {
          stack.append(schema.charAt(i));
        }
      } else if ((schema.charAt(i) == ':' || schema.charAt(i) == ',') && openingCount > 0) {
        stack.append(schema.charAt(i));
      } else if ((schema.charAt(i) == ':' || schema.charAt(i) == ',') && openingCount == 0) {
        tokens.add(stack.toString());
        stack = new StringBuilder();
        openingCount = 0;
      } else if (i == schema.length() - 1) {
        stack.append(schema.charAt(i));
        tokens.add(stack.toString());
        stack = new StringBuilder();
        openingCount = 0;
      } else {
        stack.append(schema.charAt(i));
      }
    }
    return tokens.toArray(new String[tokens.size()]);
  }

}
