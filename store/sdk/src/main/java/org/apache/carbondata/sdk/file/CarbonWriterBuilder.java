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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
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

import org.apache.hadoop.fs.s3a.Constants;

/**
 * Builder for {@link CarbonWriter}
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
  private int localDictionaryThreshold;
  private boolean isLocalDictionaryEnabled;

  /**
   * Sets the output path of the writer builder
   * @param path is the absolute path where output files are written
   * This method must be called when building CarbonWriterBuilder
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
   * If it is null or by default all dimensions are selected for sorting
   * If it is empty array, no columns are sorted
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder sortBy(String[] sortColumns) {
    this.sortColumns = sortColumns;
    return this;
  }

  /**
   * sets the taskNo for the writer. SDKs concurrently running
   * will set taskNo in order to avoid conflicts in file's name during write.
   * @param taskNo is the TaskNo user wants to specify.
   * by default it is system time in nano seconds.
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder taskNo(long taskNo) {
    this.taskNo = String.valueOf(taskNo);
    return this;
  }



  /**
   * If set, create a schema file in metadata folder.
   * @param persist is a boolean value, If set to true, creates a schema file in metadata folder.
   * By default set to false. will not create metadata folder
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder persistSchemaFile(boolean persist) {
    this.persistSchemaFile = persist;
    return this;
  }

  /**
   * If set false, writes the carbondata and carbonindex files in a flat folder structure
   * @param isTransactionalTable is a boolelan value
   * If set to false, then writes the carbondata and carbonindex files
   * in a flat folder structure.
   * If set to true, then writes the carbondata and carbonindex files
   * in segment folder structure.
   * By default set to false.
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder isTransactionalTable(boolean isTransactionalTable) {
    Objects.requireNonNull(isTransactionalTable, "Transactional Table should not be null");
    this.isTransactionalTable = isTransactionalTable;
    return this;
  }

  /**
   * Set the access key for S3
   *
   * @param key   the string of access key for different S3 type,like: fs.s3a.access.key
   * @param value the value of access key
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setAccessKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the access key for S3.
   *
   * @param value the value of access key
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setAccessKey(String value) {
    return setAccessKey(Constants.ACCESS_KEY, value);
  }

  /**
   * Set the secret key for S3
   *
   * @param key   the string of secret key for different S3 type,like: fs.s3a.secret.key
   * @param value the value of secret key
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setSecretKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the secret key for S3
   *
   * @param value the value of secret key
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setSecretKey(String value) {
    return setSecretKey(Constants.SECRET_KEY, value);
  }

  /**
   * Set the endpoint for S3
   *
   * @param key   the string of endpoint for different S3 type,like: fs.s3a.endpoint
   * @param value the value of endpoint
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setEndPoint(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the endpoint for S3
   *
   * @param value the value of endpoint
   * @return CarbonWriterBuilder
   */
  public CarbonWriterBuilder setEndPoint(String value) {
    FileFactory.getConfiguration().set(Constants.ENDPOINT, value);
    return this;
  }

  /**
   * to set the timestamp in the carbondata and carbonindex index files
   * @param UUID is a timestamp to be used in the carbondata and carbonindex index files.
   * By default set to zero.
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
   * supported keys values are
   * a. bad_records_logger_enable -- true (write into separate logs), false
   * b. bad_records_action -- FAIL, FORCE, IGNORE, REDIRECT
   * c. bad_record_path -- path
   * d. dateformat -- same as JAVA SimpleDateFormat
   * e. timestampformat -- same as JAVA SimpleDateFormat
   * f. complex_delimiter_level_1 -- value to Split the complexTypeData
   * g. complex_delimiter_level_2 -- value to Split the nested complexTypeData
   * h. quotechar
   * i. escapechar
   *
   * Default values are as follows.
   *
   * a. bad_records_logger_enable -- "false"
   * b. bad_records_action -- "FAIL"
   * c. bad_record_path -- ""
   * d. dateformat -- "" , uses from carbon.properties file
   * e. timestampformat -- "", uses from carbon.properties file
   * f. complex_delimiter_level_1 -- "$"
   * g. complex_delimiter_level_2 -- ":"
   * h. quotechar -- "\""
   * i. escapechar -- "\\"
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
   * default value is 1024 MB
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
   * @param localDictionaryThreshold is localDictionaryThreshold,default is 1000
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder localDictionaryThreshold(int localDictionaryThreshold) {
    if (localDictionaryThreshold <= 0) {
      throw new IllegalArgumentException(
          "Local Dictionary Threshold should be between greater than 0");
    }
    this.localDictionaryThreshold = localDictionaryThreshold;
    return this;
  }

  /**
   * @param enableLocalDictionary enable local dictionary  , default is false
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder enableLocalDictionary(boolean enableLocalDictionary) {
    this.isLocalDictionaryEnabled = enableLocalDictionary;
    return this;
  }


  /**
   * To set the blocklet size of CarbonData file
   * @param blockletSize is blocklet size in MB
   * default value is 64 MB
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
   * @param schema carbon Schema object {org.apache.carbondata.sdk.file.Schema}
   * @return CSVCarbonWriter
   * @throws IOException
   * @throws InvalidLoadOptionException
   */
  public CarbonWriter buildWriterForCSVInput(Schema schema)
      throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    this.schema = schema;
    CarbonLoadModel loadModel = createLoadModel();
    return new CSVCarbonWriter(loadModel);
  }

  /**
   * Build a {@link CarbonWriter}, which accepts Avro object
   * @param avroSchema avro Schema object {org.apache.avro.Schema}
   * @return AvroCarbonWriter
   * @throws IOException
   * @throws InvalidLoadOptionException
   */
  public CarbonWriter buildWriterForAvroInput(org.apache.avro.Schema avroSchema)
      throws IOException, InvalidLoadOptionException {
    this.schema = AvroCarbonWriter.getCarbonSchemaFromAvroSchema(avroSchema);
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    CarbonLoadModel loadModel = createLoadModel();
    // AVRO records are pushed to Carbon as Object not as Strings. This was done in order to
    // handle multi level complex type support. As there are no conversion converter step is
    // removed from the load. LoadWithoutConverter flag is going to point to the Loader Builder
    // which will skip Conversion Step.
    loadModel.setLoadWithoutConverterStep(true);
    return new AvroCarbonWriter(loadModel);
  }

  /**
   * Build a {@link CarbonWriter}, which accepts Json object
   * @param carbonSchema carbon Schema object
   * @return JsonCarbonWriter
   * @throws IOException
   * @throws InvalidLoadOptionException
   */
  public JsonCarbonWriter buildWriterForJsonInput(Schema carbonSchema)
      throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(carbonSchema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    this.schema = carbonSchema;
    CarbonLoadModel loadModel = createLoadModel();
    loadModel.setJsonFileLoad(true);
    return new JsonCarbonWriter(loadModel);
  }

  private void setCsvHeader(CarbonLoadModel model) {
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
    tableSchemaBuilder.enableLocalDictionary(isLocalDictionaryEnabled);
    tableSchemaBuilder.localDictionaryThreshold(localDictionaryThreshold);
    List<String> sortColumnsList = new ArrayList<>();
    if (sortColumns == null) {
      // If sort columns are not specified, default set all dimensions to sort column.
      // When dimensions are default set to sort column,
      // Inverted index will be supported by default for sort columns.
      //Null check for field to handle hole in field[] ex.
      //  user passed size 4 but supplied only 2 fileds
      for (Field field : schema.getFields()) {
        if (null != field) {
          if (field.getDataType() == DataTypes.STRING ||
              field.getDataType() == DataTypes.DATE  ||
              field.getDataType() == DataTypes.TIMESTAMP) {
            sortColumnsList.add(field.getFieldName());
          }
        }
      }
      sortColumns = new String[sortColumnsList.size()];
      sortColumns = sortColumnsList.toArray(sortColumns);
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
    TableSchema schema = tableSchemaBuilder.build();
    schema.setTableName(tableName);
    CarbonTable table =
        CarbonTable.builder().tableName(schema.getTableName()).databaseName(dbName).tablePath(path)
            .tableSchema(schema).isTransactionalTable(isTransactionalTable).build();
    return table;
  }

  private void buildTableSchema(Field[] fields, TableSchemaBuilder tableSchemaBuilder,
      List<String> sortColumnsList, ColumnSchema[] sortColumnsSchemaList) {
    Set<String> uniqueFields = new HashSet<>();
    // a counter which will be used in case of complex array type. This valIndex will be assigned
    // to child of complex array type in the order val1, val2 so that each array type child is
    // differentiated to any level
    AtomicInteger valIndex = new AtomicInteger(0);
    // Check if any of the columns specified in sort columns are missing from schema.
    for (String sortColumn: sortColumnsList) {
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
              .addColumn(new StructField(field.getFieldName(), field.getDataType()),
                  valIndex, isSortColumn > -1);
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
    CarbonLoadModel build = builder.build(options, UUID, taskNo);
    setCsvHeader(build);
    return build;
  }
}
