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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonProjection;
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

  CarbonReaderBuilder(String tablePath) {
    this.tablePath = tablePath;
  }

  public CarbonReaderBuilder projection(String[] projectionColumnNames) {
    Objects.requireNonNull(projectionColumnNames);
    this.projectionColumns = projectionColumnNames;
    return this;
  }

  public CarbonReaderBuilder filter(Expression fileterExpression) {
    Objects.requireNonNull(fileterExpression);
    this.filterExpression = fileterExpression;
    return this;
  }

  public <T> CarbonReader<T> build() throws IOException, InterruptedException {
    CarbonTable table = CarbonTable.buildFromTablePath("_temp", tablePath);

    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    final Job job = new Job(new Configuration());
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
    if (filterExpression != null) {
      format.setFilterPredicates(job.getConfiguration(), filterExpression);
    }
    if (projectionColumns != null) {
      format.setColumnProjection(job.getConfiguration(), new CarbonProjection(projectionColumns));
    }

    final List<InputSplit> splits =
        format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));

    List<RecordReader<Void, T>> readers = new ArrayList<>(splits.size());
    for (InputSplit split : splits) {
      TaskAttemptContextImpl attempt =
          new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
      RecordReader reader = format.createRecordReader(split, attempt);
      reader.initialize(split, attempt);
      readers.add(reader);
    }

    return new CarbonReader<>(readers);
  }
}
