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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;

/**
 * Biulder for {@link CarbonWriter}
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class CarbonWriterBuilder {
  private Schema schema;
  private String path;
  private String[] sortColumns;
  private boolean persistSchemaFile;
  private int blockletSize;
  private int blockSize;
  private boolean isTransactionalTable;
  private long UUID;
  private Map<String, String> options;
  private String taskNo;

  /**
   * prepares the builder with the schema provided
   * @param schema is instance of Schema
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withSchema(Schema schema) {
    Objects.requireNonNull(schema, "schema should not be null");
    this.schema = schema;
    return this;
  }

  /**
   * Sets the output path of the writer builder
   * @param path is the absolute path where output files are written
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder outputPath(String path) {
    Objects.requireNonNull(path, "path should not be null");
    this.path = path;
    return this;
  }

  /**
   * sets the list of columns that needs to be in sorted order
   * @param sortColumns is a string array of columns that needs to be sorted.
   *                    If it is null, all dimensions are selected for sorting
   *                    If it is empty array, no columns are sorted
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder sortBy(String[] sortColumns) {
    this.sortColumns = sortColumns;
    return this;
  }

  /**
   * sets the taskNo for the writer. SDKs concurrently running
   * will set taskNo in order to avoid conflits in file write.
   * @param taskNo is the TaskNo user wants to specify. Mostly it system time.
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder taskNo(String taskNo) {
    this.taskNo = taskNo;
    return this;
  }



  /**
   * If set, create a schema file in metadata folder.
   * @param persist is a boolean value, If set, create a schema file in metadata folder
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder persistSchemaFile(boolean persist) {
    this.persistSchemaFile = persist;
    return this;
  }

  /**
   * If set false, writes the carbondata and carbonindex files in a flat folder structure
   * @param isTransactionalTable is a boolelan value if set to false then writes
   *                     the carbondata and carbonindex files in a flat folder structure
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder isTransactionalTable(boolean isTransactionalTable) {
    Objects.requireNonNull(isTransactionalTable, "Transactional Table should not be null");
    this.isTransactionalTable = isTransactionalTable;
    return this;
  }

  /**
   * to set the timestamp in the carbondata and carbonindex index files
   * @param UUID is a timestamp to be used in the carbondata and carbonindex index files
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder uniqueIdentifier(long UUID) {
    Objects.requireNonNull(UUID, "Unique Identifier should not be null");
    this.UUID = UUID;
    return this;
  }

  /**
   * To support the load options for sdk writer
   * @param options key,value pair of load options.
   *                supported keys values are
   *                a. bad_records_logger_enable -- true (write into separate logs), false
   *                b. bad_records_action -- FAIL, FORCE, IGNORE, REDIRECT
   *                c. bad_record_path -- path
   *                d. dateformat -- same as JAVA SimpleDateFormat
   *                e. timestampformat -- same as JAVA SimpleDateFormat
   *                f. complex_delimiter_level_1 -- value to Split the complexTypeData
   *                g. complex_delimiter_level_2 -- value to Split the nested complexTypeData
   *                h. quotechar
   *                i. escapechar
   *
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withLoadOptions(Map<String, String> options) {
    Objects.requireNonNull(options, "Load options should not be null");
    //validate the options.
    if (options.size() > 9) {
      throw new IllegalArgumentException("Supports only nine options now. "
          + "Refer method header or documentation");
    }

    for (String option: options.keySet()) {
      if (!option.equalsIgnoreCase("bad_records_logger_enable") &&
          !option.equalsIgnoreCase("bad_records_action") &&
          !option.equalsIgnoreCase("bad_record_path") &&
          !option.equalsIgnoreCase("dateformat") &&
          !option.equalsIgnoreCase("timestampformat") &&
          !option.equalsIgnoreCase("complex_delimiter_level_1") &&
          !option.equalsIgnoreCase("complex_delimiter_level_2") &&
          !option.equalsIgnoreCase("quotechar") &&
          !option.equalsIgnoreCase("escapechar")) {
        throw new IllegalArgumentException("Unsupported options. "
            + "Refer method header or documentation");
      }
    }

    // convert it to treeMap as keys need to be case insensitive
    Map<String, String> optionsTreeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    optionsTreeMap.putAll(options);
    this.options = optionsTreeMap;
    return this;
  }

  /**
   * To set the carbondata file size in MB between 1MB-2048MB
   * @param blockSize is size in MB between 1MB to 2048 MB
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withBlockSize(int blockSize) {
    if (blockSize <= 0 || blockSize > 2048) {
      throw new IllegalArgumentException("blockSize should be between 1 MB to 2048 MB");
    }
    this.blockSize = blockSize;
    return this;
  }

  /**
   * To set the blocklet size of carbondata file
   * @param blockletSize is blocklet size in MB
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withBlockletSize(int blockletSize) {
    if (blockletSize <= 0) {
      throw new IllegalArgumentException("blockletSize should be greater than zero");
    }
    this.blockletSize = blockletSize;
    return this;
  }

  /**
   * Build a {@link CarbonWriter}, which accepts row in CSV format
   */
  public CarbonWriter buildWriterForCSVInput() throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    CarbonLoadModel loadModel = createLoadModel();
    return new CSVCarbonWriter(loadModel);
  }

  /**
   * Build a {@link CarbonWriter}, which accepts Avro object
   * @return
   * @throws IOException
   */
  public CarbonWriter buildWriterForAvroInput() throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    CarbonLoadModel loadModel = createLoadModel();
    return new AvroCarbonWriter(loadModel);
  }

  private CarbonLoadModel createLoadModel() throws IOException, InvalidLoadOptionException {
    // build CarbonTable using schema
    CarbonTable table = buildCarbonTable();
    if (persistSchemaFile) {
      // we are still using the traditional carbon table folder structure
      persistSchemaFile(table, CarbonTablePath.getSchemaFilePath(path));
    }

    // build LoadModel
    return buildLoadModel(table, UUID, taskNo, options);
  }

  /**
   * Build a {@link CarbonTable}
   */
  private CarbonTable buildCarbonTable() {
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
      for (Field field : schema.getFields()) {
        if (field.getDataType() == DataTypes.STRING ||
            field.getDataType() == DataTypes.DATE ||
            field.getDataType() == DataTypes.TIMESTAMP) {
          sortColumnsList.add(field.getFieldName());
        }
      }
      sortColumns = new String[sortColumnsList.size()];
      sortColumns = sortColumnsList.toArray(sortColumns);
    } else {
      sortColumnsList = Arrays.asList(sortColumns);
    }
    for (Field field : schema.getFields()) {
      tableSchemaBuilder.addColumn(
          new StructField(field.getFieldName(), field.getDataType()),
          sortColumnsList.contains(field.getFieldName()));
    }
    String tableName;
    String dbName;
    if (isTransactionalTable) {
      tableName = "_tempTable";
      dbName = "_tempDB";
    } else {
      dbName = null;
      tableName = null;
    }
    TableSchema schema = tableSchemaBuilder.build();
    schema.setTableName(tableName);
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName(dbName)
        .tablePath(path)
        .tableSchema(schema)
        .isTransactionalTable(isTransactionalTable)
        .build();
    return table;
  }

  /**
   * Save the schema of the {@param table} to {@param persistFilePath}
   * @param table table object containing schema
   * @param persistFilePath absolute file path with file name
   */
  private void persistSchemaFile(CarbonTable table, String persistFilePath) throws IOException {
    TableInfo tableInfo = table.getTableInfo();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(persistFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(
            tableInfo,
            tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(
            tableInfo.getLastUpdatedTime());
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
      Map<String, String> options) throws InvalidLoadOptionException, IOException {
    if (options == null) {
      options = new HashMap<>();
    }
    CarbonLoadModelBuilder builder = new CarbonLoadModelBuilder(table);
    return builder.build(options, UUID, taskNo);
  }
}