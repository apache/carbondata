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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.hive.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.TableOptionConstant;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

public class MapredCarbonOutputFormat<T> extends CarbonTableOutputFormat
    implements HiveOutputFormat<Void, T>, OutputFormat<Void, T> {

  @Override
  public RecordWriter<Void, T> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
      Progressable progressable) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
  }

  private TableInfo getTableInfo(Properties tableProperties) throws SQLException {
    TableInfo tableInfo = new TableInfo();
    TableSchemaBuilder builder = new TableSchemaBuilder();
    String[] tableIdentifier = tableProperties.getProperty("name").split("\\.");
    builder.tableName(tableIdentifier[1]);
    tableInfo.setDatabaseName(tableIdentifier[0]);
    String[] columns = tableProperties.getProperty("columns").split(",");
    String[] column_types = splitsSchemaStringToArray(tableProperties.getProperty("columns.types"));
    String sortColumnsString = tableProperties.getProperty("sort_columns");
    List<String> sortColumns = new ArrayList<>();
    if (sortColumnsString != null) {
      sortColumns = Arrays.asList(sortColumnsString.toLowerCase().split("\\,"));
    }
    List<String> partitionColumns = new ArrayList<>();
    //    for (FieldSchema fieldSchema : table.getPartitionKeys()) {
    //      partitionColumns.add(fieldSchema.getName());
    //    }
    PartitionInfo partitionInfo = null;
    AtomicInteger integer = new AtomicInteger();
    for (int i = 0; i < columns.length; i++) {
      DataType dataType = DataTypeUtil.convertHiveTypeToCarbon(column_types[i]);
      Field field = new Field(columns[i].toLowerCase(), dataType);
      ColumnSchema col = builder
          .addColumn(new StructField(columns[i].toLowerCase(), dataType, field.getChildren()),
              integer, sortColumns.contains(columns[i]), false);
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
    for (Map.Entry<Object, Object> entry : tableProperties.entrySet()) {
      tableSchema.getTableProperties()
          .put(entry.getKey().toString().toLowerCase(), entry.getValue().toString().toLowerCase());
    }
    tableSchema.setPartitionInfo(partitionInfo);
    tableInfo.setTablePath(tableProperties.getProperty("location"));
    tableInfo.setTransactionalTable(false);
    tableInfo.setFactTable(tableSchema);
    return tableInfo;
  }

  private String[] splitsSchemaStringToArray(String schema) {
    List<String> tokens = new ArrayList();
    StringBuilder stack = new StringBuilder();
    int indx = 0;
    int openingCount = 0;
    for (int i = 0; i < schema.length(); i++) {
      if (schema.charAt(i) == '<') {
        openingCount++;
        stack.append(schema.charAt(i));
      } else if (schema.charAt(i) == '>') {
        --openingCount;
        if (i == schema.length() - 1) {
          stack.append(schema.charAt(i));
          tokens.add(stack.toString());
          stack = new StringBuilder();
          openingCount = 0;
          indx = 0;
        } else {
          stack.append(schema.charAt(i));
        }
      } else if (schema.charAt(i) == ':' && openingCount > 0) {
        stack.append(schema.charAt(i));
      } else if (schema.charAt(i) == ':' && openingCount == 0) {
        tokens.add(stack.toString());
        stack = new StringBuilder();
        indx = 0;
        openingCount = 0;
      } else if (i == schema.length() - 1) {
        stack.append(schema.charAt(i));
        tokens.add(stack.toString());
        stack = new StringBuilder();
        indx = 0;
        openingCount = 0;
      } else {
        stack.append(schema.charAt(i));
      }
    }
    return tokens.toArray(new String[tokens.size()]);
  }

  private CarbonLoadModel createCarbonLoadModel(Properties tableProperties) throws IOException {
    CarbonLoadModel loadModel = new CarbonLoadModel();
    String[] tableUniqueName = tableProperties.get("name").toString().split("\\.");
    String databaseName = tableUniqueName[0];
    String tableName = tableUniqueName[1];
    String tablePath = tableProperties.get("location").toString();
    loadModel.setTablePath(tablePath);
    loadModel.setDatabaseName(databaseName);
    loadModel.setTableName(tableName);
    loadModel.setSerializationNullFormat(",\\N");
    loadModel.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + ",true");
    loadModel.setBadRecordsAction(TableOptionConstant.BAD_RECORDS_ACTION.getName() + ",fail");
    loadModel
        .setIsEmptyDataBadRecord(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + ",false");
    loadModel.setCsvHeader(tableProperties.get("columns").toString());
    loadModel.setCsvHeaderColumns(tableProperties.get("columns").toString().split(","));
    String complexDelim = tableProperties.getProperty("complex_delimiter", "");
    for (String delim : complexDelim.split(",")) {
      loadModel.setComplexDelimiter(delim);
    }
    CarbonTable carbonTable;
    try {
      carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(tableProperties));
    } catch (SQLException e) {
      throw new RuntimeException("Unable to fetch schema for the table: " + tableName, e);
    }
    String globalSortPartitions = carbonTable.getTableInfo().getFactTable().getTableProperties()
        .get("global_sort_partitions");
    if (globalSortPartitions != null) {
      loadModel.setGlobalSortPartitions(globalSortPartitions);
    }
    String columnCompressor = carbonTable.getTableInfo().getFactTable().getTableProperties()
        .get(CarbonCommonConstants.COMPRESSOR);
    if (null == columnCompressor) {
      columnCompressor = CompressorFactory.getInstance().getCompressor().getName();
    }
    loadModel.setColumnCompressor(columnCompressor);
    loadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable));
    loadModel.setCarbonTransactionalTable(false);
    return loadModel;
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    CarbonLoadModel carbonLoadModel = createCarbonLoadModel(tableProperties);
    carbonLoadModel.setSkipParsers();
    CarbonTableOutputFormat.setLoadModel(jc, carbonLoadModel);
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(jc, taskAttemptID);
    org.apache.hadoop.mapreduce.RecordWriter<NullWritable, ObjectArrayWritable> re =
        super.getRecordWriter(context);
    return new FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable writable) throws IOException {
        try {
          ObjectArrayWritable objectArrayWritable = new ObjectArrayWritable();
          objectArrayWritable.set(((CarbonHiveRow) writable).getData());
          re.write(NullWritable.get(), objectArrayWritable);
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        }
      }

      @Override
      public void close(boolean b) throws IOException {
        try {
          re.close(context);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
  }

}
