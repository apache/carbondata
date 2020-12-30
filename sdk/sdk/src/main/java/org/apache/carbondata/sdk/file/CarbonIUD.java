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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;

public class CarbonIUD {

  private final Configuration configuration;
  private final Map<String, Map<String, Set<String>>> filterColumnToValueMappingForDelete;
  private final Map<String, Map<String, Set<String>>> filterColumnToValueMappingForUpdate;
  private final Map<String, Map<String, String>> updateColumnToValueMapping;

  private CarbonIUD(Configuration conf) {
    configuration = conf;
    filterColumnToValueMappingForDelete = new HashMap<>();
    filterColumnToValueMappingForUpdate = new HashMap<>();
    updateColumnToValueMapping = new HashMap<>();
  }

  /**
   * @return CarbonIUD object
   */
  public static CarbonIUD getInstance(Configuration conf) {
    return new CarbonIUD(conf);
  }

  public static CarbonIUD getInstance() {
    return new CarbonIUD(null);
  }

  /**
   * @param path   is the table path on which delete is performed
   * @param column is the columnName on which records have to be deleted
   * @param value  of column on which the records have to be deleted
   * @return CarbonIUD object
   */
  public CarbonIUD delete(String path, String column, String value) {
    prepareDelete(path, column, value, filterColumnToValueMappingForDelete);
    return this;
  }

  /**
   * This method deletes the rows at given path by applying the filterExpression
   *
   * @param path             is the table path on which delete is performed
   * @param filterExpression is the expression to delete the records
   * @throws IOException
   * @throws InterruptedException
   */
  public void delete(String path, Expression filterExpression)
      throws IOException, InterruptedException {
    CarbonReader reader = CarbonReader.builder(path)
        .projection(new String[] { CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID })
        .withHadoopConf(configuration)
        .filter(filterExpression).build();

    RecordWriter<NullWritable, ObjectArrayWritable> deleteDeltaWriter =
        CarbonTableOutputFormat.getDeleteDeltaRecordWriter(path);
    ObjectArrayWritable writable = new ObjectArrayWritable();
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      writable.set(row);
      deleteDeltaWriter.write(NullWritable.get(), writable);
    }
    deleteDeltaWriter.close(null);
    reader.close();
  }

  /**
   * Calling this method will start the execution of delete process
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void closeDelete() throws IOException, InterruptedException {
    for (Map.Entry<String, Map<String, Set<String>>> path : this.filterColumnToValueMappingForDelete
        .entrySet()) {
      deleteExecution(path.getKey());
      createEmptyMetadataFile(path.getKey());
    }
  }

  private void createEmptyMetadataFile(String path) throws IOException {
    if (!StringUtils.isEmpty(path)) {
      path = path + CarbonCommonConstants.FILE_SEPARATOR +
          CarbonCommonConstants.CARBON_SDK_EMPTY_METADATA_PATH;
      CarbonFile emptySDKDirectory = FileFactory.getCarbonFile(path);
      if (!emptySDKDirectory.exists()) {
        emptySDKDirectory.mkdirs();
      }
    }
  }

  /**
   * @param path      is the table path on which update is performed
   * @param column    is the columnName on which records have to be updated
   * @param value     of column on which the records have to be updated
   * @param updColumn is the name of updatedColumn
   * @param updValue  is the value of updatedColumn
   * @return CarbonUID
   */
  public CarbonIUD update(String path, String column, String value, String updColumn,
      String updValue) {
    prepareUpdate(path, column, value, updColumn, updValue);
    return this;
  }

  /**
   * This method updates the rows at given path by applying the filterExpression
   *
   * @param path                        is the table path on which update is performed.
   * @param filterExpression            is the expression object to update the records
   * @param updatedColumnToValueMapping contains the mapping of updatedColumns to updatedValues
   * @throws IOException
   * @throws InterruptedException
   * @throws InvalidLoadOptionException
   */
  public void update(String path, Expression filterExpression,
      Map<String, String> updatedColumnToValueMapping)
      throws IOException, InterruptedException, InvalidLoadOptionException {
    List<String> indexFiles = getCarbonIndexFile(path);
    Schema schema = CarbonSchemaReader.readSchema(indexFiles.get(0)).asOriginOrder();
    Field[] fields = schema.getFields();
    String[] projectionColumns = new String[fields.length + 1];
    for (int i = 0; i < fields.length; i++) {
      projectionColumns[i] = (fields[i].getFieldName());
    }
    projectionColumns[projectionColumns.length - 1] =
        CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID;
    CarbonWriter writer =
        CarbonWriter.builder().outputPath(path)
            .withHadoopConf(configuration)
            .withCsvInput(schema)
            .writtenBy("CarbonIUD")
            .build();
    CarbonReader reader =
        CarbonReader.builder(path).projection(projectionColumns)
            .withHadoopConf(configuration)
            .filter(filterExpression).build();
    RecordWriter<NullWritable, ObjectArrayWritable> deleteDeltaWriter =
        CarbonTableOutputFormat.getDeleteDeltaRecordWriter(path);
    ObjectArrayWritable writable = new ObjectArrayWritable();
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      writable.set(Arrays.copyOfRange(row, row.length - 1, row.length));
      for (Map.Entry<String, String> column : updatedColumnToValueMapping.entrySet()) {
        row[getColumnIndex(fields, column.getKey())] = column.getValue();
      }
      writer.write(Arrays.copyOfRange(row, 0, row.length - 1));
      deleteDeltaWriter.write(NullWritable.get(), writable);
    }
    deleteDeltaWriter.close(null);
    writer.close();
    reader.close();
  }

  /**
   * Calling this method will start the execution of update process
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws InvalidLoadOptionException
   */
  private void closeUpdate() throws IOException, InterruptedException, InvalidLoadOptionException {
    for (Map.Entry<String, Map<String, Set<String>>> path : this.filterColumnToValueMappingForUpdate
        .entrySet()) {
      if (this.updateColumnToValueMapping.containsKey(path.getKey())) {
        updateExecution(path.getKey());
        createEmptyMetadataFile(path.getKey());
      }
    }
  }

  /**
   * Calling this method will execute delete and update operation in one statement.
   * This method will first perform delete operation and then update operation
   * (update operation is only performed if the rows to be
   * updated are not deleted while delete operation)
   * For eg:
   * CarbonIUD.getInstance().delete().delete().update()
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws InvalidLoadOptionException
   */
  public void commit() throws IOException, InterruptedException, InvalidLoadOptionException {
    if (filterColumnToValueMappingForDelete.size() != 0) {
      closeDelete();
    }
    if (filterColumnToValueMappingForUpdate.size() != 0 && !ifRowsDeleted()) {
      closeUpdate();
    }
  }

  private void updateExecution(String path)
      throws IOException, InterruptedException, InvalidLoadOptionException {
    Expression filterExpression =
        getExpression(path, this.filterColumnToValueMappingForUpdate.get(path));
    update(path, filterExpression, this.updateColumnToValueMapping.get(path));
  }

  private void deleteExecution(String path) throws IOException, InterruptedException {
    Expression filterExpression =
        getExpression(path, this.filterColumnToValueMappingForDelete.get(path));
    delete(path, filterExpression);
  }

  /**
   * This method prepares the column to value mapping for update operation
   * for eg: UPDATE updColumn = updValue WHERE column = value
   */
  private void prepareUpdate(String path, String column, String value, String updColumn,
      String updValue) {
    prepareDelete(path, column, value, filterColumnToValueMappingForUpdate);
    updColumn = updColumn.toLowerCase().trim();
    if (this.updateColumnToValueMapping.containsKey(path)) {
      this.updateColumnToValueMapping.get(path).put(updColumn, updValue);
    } else {
      Map<String, String> columnToValue = new HashMap<>();
      columnToValue.put(updColumn, updValue);
      this.updateColumnToValueMapping.put(path, columnToValue);
    }
  }

  /**
   * This method prepares the column to value mapping for delete operation
   * for eg: DELETE WHERE column = value
   */
  private void prepareDelete(String path, String column, String value,
      Map<String, Map<String, Set<String>>> filterColumnToValueMapping) {
    column = column.toLowerCase().trim();
    if (filterColumnToValueMapping.containsKey(path)) {
      Map<String, Set<String>> columnToValueMapping = filterColumnToValueMapping.get(path);
      if (columnToValueMapping.containsKey(column)) {
        columnToValueMapping.get(column).add(value);
      } else {
        Set<String> columnValues = new HashSet<>();
        columnValues.add(value);
        columnToValueMapping.put(column, columnValues);
      }
    } else {
      Map<String, Set<String>> columnToValueMapping = new HashMap<>();
      Set<String> columnValues = new HashSet<>();
      columnValues.add(value);
      columnToValueMapping.put(column, columnValues);
      filterColumnToValueMapping.put(path, columnToValueMapping);
    }
  }

  /**
   * This method will convert the given columnToValue mapping into expression object
   * If columnToValue mapping have following entries:
   * name --> {karan, kunal, vikram}
   * age --> {24}
   * the expression will look like this for above entries:
   * ((name = karan || name = kunal || name = vikram) && (age = 24))
   */
  private Expression getExpression(String path, Map<String, Set<String>> columnToValueMapping)
      throws IOException {
    List<String> indexFiles = getCarbonIndexFile(path);
    Schema schema = CarbonSchemaReader.readSchema(indexFiles.get(0)).asOriginOrder();
    Field[] fields = schema.getFields();
    List<Expression> listOfExpressions = new ArrayList<>();
    for (Map.Entry<String, Set<String>> column : columnToValueMapping.entrySet()) {
      DataType dataType = getColumnDataType(fields, column.getKey());
      List<Expression> listOfOrExpressions = new ArrayList<>();
      for (String value : column.getValue()) {
        listOfOrExpressions.add(
            new EqualToExpression(new ColumnExpression(column.getKey(), dataType),
                new LiteralExpression(value, dataType)));
      }
      Expression OrFilterExpression = null;
      if (listOfOrExpressions.size() > 0) {
        OrFilterExpression = listOfOrExpressions.get(0);
      }
      for (int i = 1; i < listOfOrExpressions.size(); i++) {
        OrFilterExpression = new OrExpression(OrFilterExpression, listOfOrExpressions.get(i));
      }
      listOfExpressions.add(OrFilterExpression);
    }
    Expression filterExpression = null;
    if (listOfExpressions.size() > 0) {
      filterExpression = listOfExpressions.get(0);
    }
    for (int i = 1; i < listOfExpressions.size(); i++) {
      filterExpression = new AndExpression(filterExpression, listOfExpressions.get(i));
    }
    return filterExpression;
  }

  private int getColumnIndex(Field[] fields, String column) {
    int index = -1;
    for (Field field : fields) {
      if (field.getFieldName().equals(column)) {
        index = field.getSchemaOrdinal();
        break;
      }
    }
    if (index == -1) {
      throw new RuntimeException("ColumnName doesn't exists");
    }
    return index;
  }

  private List<String> getCarbonIndexFile(String path) {
    List<String> indexFiles = null;
    try (Stream<Path> walk = Files.walk(Paths.get(path))) {
      indexFiles = walk.map(x -> x.toString())
          .filter(f -> f.endsWith(CarbonCommonConstants.UPDATE_INDEX_FILE_EXT))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (indexFiles == null || indexFiles.size() < 1) {
      throw new RuntimeException("Carbon index file does not exists.");
    }
    return indexFiles;
  }

  private DataType getColumnDataType(Field[] fields, String column) {
    DataType type = null;
    for (Field field : fields) {
      if (field.getFieldName().equals(column)) {
        type = field.getDataType();
        break;
      }
    }
    if (null == type) {
      throw new RuntimeException("ColumnName doesn't exists");
    }
    if (type.isComplexType()) {
      throw new RuntimeException("IUD operation not supported for Complex data types");
    }
    return type;
  }

  private boolean ifRowsDeleted() {
    for (Map.Entry<String, Map<String, Set<String>>> path : this.filterColumnToValueMappingForUpdate
        .entrySet()) {
      if (!this.filterColumnToValueMappingForDelete.containsKey(path.getKey())) {
        return false;
      } else {
        for (Map.Entry<String, Set<String>> column : this.filterColumnToValueMappingForDelete
            .get(path.getKey()).entrySet()) {
          if (!this.filterColumnToValueMappingForUpdate.get(path.getKey())
              .containsKey(column.getKey())) {
            return false;
          } else {
            for (String value : this.filterColumnToValueMappingForUpdate.get(path.getKey())
                .get(column.getKey())) {
              if (!column.getValue().contains(value)) {
                return false;
              }
            }
          }
        }
      }
    }
    return true;
  }
}
