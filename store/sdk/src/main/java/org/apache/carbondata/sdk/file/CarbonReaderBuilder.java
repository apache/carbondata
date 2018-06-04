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
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

@InterfaceAudience.User
@InterfaceStability.Evolving
public class CarbonReaderBuilder {

  private String tablePath;
  private String[] projectionColumns;
  private Expression filterExpression;
  private String tableName;
  private boolean isTransactionalTable;

  /**
   * It will be true if use the projectAllColumns method，
   * it will be false if use the projection method
   */
  private boolean isProjectAllColumns = true;

  /**
   * Construct a CarbonReaderBuilder with table path and table name
   *
   * @param tablePath table path
   * @param tableName table name
   */
  CarbonReaderBuilder(String tablePath, String tableName) {
    this.tablePath = tablePath;
    this.tableName = tableName;
  }

  /**
   * Configure the projection column names of carbon reader
   *
   * @param projectionColumnNames projection column names
   * @return CarbonReaderBuilder object
   */
  public CarbonReaderBuilder projection(String[] projectionColumnNames) {
    Objects.requireNonNull(projectionColumnNames);
    this.projectionColumns = projectionColumnNames;
    isProjectAllColumns = false;
    return this;
  }

  /**
   * Configure the transactional status of table
   * If set to false, then reads the carbondata and carbonindex files from a flat folder structure.
   * If set to true, then reads the carbondata and carbonindex files from segment folder structure.
   * Default value is false
   *
   * @param isTransactionalTable whether is transactional table or not
   * @return CarbonReaderBuilder object
   */
  public CarbonReaderBuilder isTransactionalTable(boolean isTransactionalTable) {
    Objects.requireNonNull(isTransactionalTable);
    this.isTransactionalTable = isTransactionalTable;
    return this;
  }

  /**
   * Project all Columns for carbon reader
   *
   * @return CarbonReaderBuilder object
   * @throws IOException
   */
  public CarbonReaderBuilder projectAllColumns() throws IOException {
    CarbonTable carbonTable = CarbonTable
        .buildFromTablePath(tableName, tablePath, isTransactionalTable);

    List<ColumnSchema> colList = carbonTable.getTableInfo().getFactTable().getListOfColumns();
    List<String> projectColumn = new ArrayList<String>();
    for (ColumnSchema cols : colList) {
      if (cols.getSchemaOrdinal() != -1) {
        projectColumn.add(cols.getColumnUniqueId());
      }
    }
    projectionColumns = new String[projectColumn.size()];
    int i = 0;
    for (String columnName : projectColumn) {
      projectionColumns[i] = columnName;
      i++;
    }
    isProjectAllColumns = true;
    return this;
  }

  /**
   * Configure the filter expression for carbon reader
   *
   * @param filterExpression filter expression
   * @return CarbonReaderBuilder object
   */
  public CarbonReaderBuilder filter(Expression filterExpression) {
    Objects.requireNonNull(filterExpression);
    this.filterExpression = filterExpression;
    return this;
  }

  /**
   * Set the access key for S3
   *
   * @param key   the string of access key for different S3 type,like: fs.s3a.access.key
   * @param value the value of access key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setAccessKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the access key for S3.
   *
   * @param value the value of access key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setAccessKey(String value) {
    return setAccessKey(Constants.ACCESS_KEY, value);
  }

  /**
   * Set the secret key for S3
   *
   * @param key   the string of secret key for different S3 type,like: fs.s3a.secret.key
   * @param value the value of secret key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setSecretKey(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the secret key for S3
   *
   * @param value the value of secret key
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setSecretKey(String value) {
    return setSecretKey(Constants.SECRET_KEY, value);
  }

  /**
   * Set the endpoint for S3
   *
   * @param key   the string of endpoint for different S3 type,like: fs.s3a.endpoint
   * @param value the value of endpoint
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setEndPoint(String key, String value) {
    FileFactory.getConfiguration().set(key, value);
    return this;
  }

  /**
   * Set the endpoint for S3
   *
   * @param value the value of endpoint
   * @return CarbonWriterBuilder object
   */
  public CarbonReaderBuilder setEndPoint(String value) {
    return setEndPoint(Constants.ENDPOINT, value);
  }

  /**
   * Build CarbonReader
   *
   * @param <T>
   * @return CarbonReader
   * @throws IOException
   * @throws InterruptedException
   */
  public <T> CarbonReader<T> build() throws IOException, InterruptedException {
    CarbonTable table = CarbonTable.buildFromTablePath(tableName, tablePath, isTransactionalTable);

    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    final Job job = new Job(new Configuration());
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
    if (filterExpression != null) {
      format.setFilterPredicates(job.getConfiguration(), filterExpression);
    }
    if (isProjectAllColumns) {
      projectAllColumns();
    }
    format.setColumnProjection(job.getConfiguration(), projectionColumns);

    final List<InputSplit> splits =
        format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));

    List<RecordReader<Void, T>> readers = new ArrayList<>(splits.size());
    for (InputSplit split : splits) {
      TaskAttemptContextImpl attempt =
          new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
      RecordReader reader = format.createRecordReader(split, attempt);
      reader.initialize(split, attempt);
      reader.close();
      readers.add(reader);
    }

    return new CarbonReader<>(readers);
  }
}
