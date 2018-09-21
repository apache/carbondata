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
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;

import org.apache.hadoop.conf.Configuration;
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
  private Configuration hadoopConf;

  /**
   * Construct a CarbonReaderBuilder with table path and table name
   *
   * @param tablePath table path
   * @param tableName table name
   */
  CarbonReaderBuilder(String tablePath, String tableName) {
    this.tablePath = tablePath;
    this.tableName = tableName;
    ThreadLocalSessionInfo.setCarbonSessionInfo(new CarbonSessionInfo());
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
   * To support hadoop configuration
   *
   * @param conf hadoop configuration support, can set s3a AK,SK,end point and other conf with this
   * @return updated CarbonReaderBuilder
   */
  public CarbonReaderBuilder withHadoopConf(Configuration conf) {
    if (conf != null) {
      this.hadoopConf = conf;
    }
    return this;
  }

  /**
   * Build CarbonReader
   *
   * @param <T>
   * @return CarbonReader
   * @throws IOException
   * @throws InterruptedException
   */
  public <T> CarbonReader<T> build()
      throws IOException, InterruptedException {
    if (hadoopConf == null) {
      hadoopConf = FileFactory.getConfiguration();
    }
    CarbonTable table;
    if (filterExpression != null) {
      table = CarbonTable.buildTable(tablePath, tableName, hadoopConf);
    } else {
      table = CarbonTable.buildDummyTable(tablePath);
    }
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    final Job job = new Job(hadoopConf);
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
    if (filterExpression != null) {
      format.setFilterPredicates(job.getConfiguration(), filterExpression);
    }

    if (projectionColumns != null) {
      // set the user projection
      int len = projectionColumns.length;
      //      TODO : Handle projection of complex child columns
      for (int i = 0; i < len; i++) {
        if (projectionColumns[i].contains(".")) {
          throw new UnsupportedOperationException(
              "Complex child columns projection NOT supported through CarbonReader");
        }
      }
      format.setColumnProjection(job.getConfiguration(), projectionColumns);
    }

    try {
      final List<InputSplit> splits =
          format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));

      List<RecordReader<Void, T>> readers = new ArrayList<>(splits.size());
      for (InputSplit split : splits) {
        TaskAttemptContextImpl attempt =
            new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        RecordReader reader = format.createRecordReader(split, attempt);
        try {
          reader.initialize(split, attempt);
          readers.add(reader);
        } catch (Exception e) {
          reader.close();
          throw e;
        }
      }
      return new CarbonReader<>(readers);
    } catch (Exception ex) {
      // Clear the datamap cache as it can get added in getSplits() method
      DataMapStoreManager.getInstance()
          .clearDataMaps(table.getAbsoluteTableIdentifier());
      throw ex;
    }
  }

}
