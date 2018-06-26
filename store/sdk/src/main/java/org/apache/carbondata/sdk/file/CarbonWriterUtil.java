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
package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;

public class CarbonWriterUtil {

  private static CarbonWriterUtil INSTANCE;

  private CarbonWriterUtil() {

  }

  public static synchronized CarbonWriterUtil getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new CarbonWriterUtil();
    }
    return INSTANCE;
  }

  private void setCsvHeader(CarbonLoadModel model, Schema schema) {
    Field[] fields = schema.getFields();
    StringBuilder builder = new StringBuilder();
    String[] columns = new String[fields.length];
    int i = 0;
    for (Field field : fields) {
      if (null != field) {
        builder.append(field.getFieldName());
        builder.append(",");
        columns[i++] = field.getFieldName();
      }
    }
    String header = builder.toString();
    model.setCsvHeader(header.substring(0, header.length() - 1));
    model.setCsvHeaderColumns(columns);
  }

  public CarbonLoadModel createLoadModel(String path, boolean persistSchemaFile, long UUID,
      String taskNo, Map<String, String> options, Schema schema, int blockSize, int blockletSize,
      String[] sortColumns, boolean isTransactionalTable)
      throws IOException, InvalidLoadOptionException {
    // build CarbonTable using schema
    CarbonTable table =
        buildCarbonTable(blockSize, blockletSize, sortColumns, schema, isTransactionalTable, UUID,
            path);
    if (persistSchemaFile) {
      // we are still using the traditional carbon table folder structure
      persistSchemaFile(table, CarbonTablePath.getSchemaFilePath(path));
    }
    // build LoadModel
    return buildLoadModel(table, UUID, taskNo, options, schema);
  }

  public CarbonLoadModel createLoadModel(String path, boolean persistSchemaFile, long UUID,
      String taskNo, Map<String, String> options, Schema schema)
      throws IOException, InvalidLoadOptionException {
    int blockSize;
    int blockletSize;
    if (options.containsKey(CarbonCommonConstants.TABLE_BLOCKSIZE)) {
      blockSize = Integer.parseInt(options.get(CarbonCommonConstants.TABLE_BLOCKSIZE));
    } else {
      blockSize = Integer.parseInt(CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL);
    }
    if (options.containsKey(CarbonCommonConstants.BLOCKLET_SIZE)) {
      blockletSize = Integer.parseInt(options.get(CarbonCommonConstants.BLOCKLET_SIZE));
    } else {
      blockletSize = Integer.parseInt(CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    }
    return createLoadModel(path, persistSchemaFile, UUID, taskNo, options, schema, blockSize,
        blockletSize, new String[] {}, false);
  }

  /**
   * Build a {@link CarbonTable}
   */
  private CarbonTable buildCarbonTable(int blockSize, int blockletSize, String[] sortColumns,
      Schema schema, boolean isTransactionalTable, long UUID, String path) {
    TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
    if (blockSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockSize(blockSize);
    }

    if (blockletSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockletSize(blockletSize);
    }

    List<String> sortColumnsList = new ArrayList<>();
    if (sortColumns == null) {
      // If sort columns are not specified, default set all dimensions to sort column.
      // When dimensions are default set to sort column,
      // Inverted index will be supported by default for sort columns.
      //Null check for field to handle hole in field[] ex.
      //  user passed size 4 but supplied only 2 fileds
      for (Field field : schema.getFields()) {
        if (null != field) {
          if (field.getDataType() == DataTypes.STRING || field.getDataType() == DataTypes.DATE
              || field.getDataType() == DataTypes.TIMESTAMP) {
            sortColumnsList.add(field.getFieldName());
          }
        }
      }
      sortColumns = new String[sortColumnsList.size()];
      sortColumnsList.toArray(sortColumns);
    } else {
      sortColumnsList = Arrays.asList(sortColumns);
    }
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];
    Field[] fields = schema.getFields();
    buildTableSchema(fields, tableSchemaBuilder, sortColumnsList, sortColumnsSchemaList);

    tableSchemaBuilder.setSortColumns(Arrays.asList(sortColumnsSchemaList));
    String tableName;
    String dbName;
    if (isTransactionalTable) {
      tableName = "_tempTable";
      dbName = "_tempDB";
    } else {
      dbName = "";
      tableName = "_tempTable_" + String.valueOf(UUID);
    }
    TableSchema tableSchema = tableSchemaBuilder.build();
    tableSchema.setTableName(tableName);
    return CarbonTable.builder().tableName(tableSchema.getTableName()).databaseName(dbName)
        .tablePath(path).tableSchema(tableSchema).isTransactionalTable(isTransactionalTable)
        .build();
  }

  private void buildTableSchema(Field[] fields, TableSchemaBuilder tableSchemaBuilder,
      List<String> sortColumnsList, ColumnSchema[] sortColumnsSchemaList) {
    Set<String> uniqueFields = new HashSet<>();
    // a counter which will be used in case of complex array type. This valIndex will be assigned
    // to child of complex array type in the order val1, val2 so that each array type child is
    // differentiated to any level
    AtomicInteger valIndex = new AtomicInteger(0);
    // Check if any of the columns specified in sort columns are missing from schema.
    for (String sortColumn : sortColumnsList) {
      boolean exists = false;
      for (Field field : fields) {
        if (field.getFieldName().equalsIgnoreCase(sortColumn)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        throw new RuntimeException(
            "column: " + sortColumn + " specified in sort columns does not exist in schema");
      }
    }
    int i = 0;
    for (Field field : fields) {
      if (null != field) {
        if (!uniqueFields.add(field.getFieldName())) {
          throw new RuntimeException(
              "Duplicate column " + field.getFieldName() + " found in table schema");
        }
        int isSortColumn = sortColumnsList.indexOf(field.getFieldName());
        if (isSortColumn > -1) {
          // unsupported types for ("array", "struct", "double", "float", "decimal")
          if (field.getDataType() == DataTypes.DOUBLE || field.getDataType() == DataTypes.FLOAT
              || DataTypes.isDecimal(field.getDataType()) || field.getDataType().isComplexType()) {
            throw new RuntimeException(
                " sort columns not supported for " + "array, struct, double, float, decimal ");
          }
        }
        if (field.getChildren() != null && field.getChildren().size() > 0) {
          if (field.getDataType().getName().equalsIgnoreCase("ARRAY")) {
            // Loop through the inner columns and for a StructData
            DataType complexType =
                DataTypes.createArrayType(field.getChildren().get(0).getDataType());
            tableSchemaBuilder
                .addColumn(new StructField(field.getFieldName(), complexType), valIndex, false);
          } else if (field.getDataType().getName().equalsIgnoreCase("STRUCT")) {
            // Loop through the inner columns and for a StructData
            List<StructField> structFieldsArray =
                new ArrayList<StructField>(field.getChildren().size());
            for (StructField childFld : field.getChildren()) {
              structFieldsArray
                  .add(new StructField(childFld.getFieldName(), childFld.getDataType()));
            }
            DataType complexType = DataTypes.createStructType(structFieldsArray);
            tableSchemaBuilder
                .addColumn(new StructField(field.getFieldName(), complexType), valIndex, false);
          }
        } else {
          ColumnSchema columnSchema = tableSchemaBuilder
              .addColumn(new StructField(field.getFieldName(), field.getDataType()), valIndex,
                  isSortColumn > -1);
          if (isSortColumn > -1) {
            columnSchema.setSortColumn(true);
            sortColumnsSchemaList[isSortColumn] = columnSchema;
          }
        }
      }
    }
  }

  /**
   * Save the schema of the {@param table} to {@param persistFilePath}
   *
   * @param table           table object containing schema
   * @param persistFilePath absolute file path with file name
   */
  private void persistSchemaFile(CarbonTable table, String persistFilePath) throws IOException {
    TableInfo tableInfo = table.getTableInfo();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(persistFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }
    ThriftWriter thriftWriter = new ThriftWriter(persistFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
  }

  /**
   * Build a {@link CarbonLoadModel}
   */
  private CarbonLoadModel buildLoadModel(CarbonTable table, long UUID, String taskNo,
      Map<String, String> options, Schema schema) throws InvalidLoadOptionException, IOException {
    if (options == null) {
      options = new HashMap<>();
    }
    CarbonLoadModelBuilder builder = new CarbonLoadModelBuilder(table);
    CarbonLoadModel build = builder.build(options, UUID, taskNo);
    setCsvHeader(build, schema);
    return build;
  }
}
