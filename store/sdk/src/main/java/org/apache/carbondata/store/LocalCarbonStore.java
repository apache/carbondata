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

package org.apache.carbondata.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

/**
 * A CarbonStore implementation that works locally, without other compute framework dependency.
 * It can be used to read data in local disk.
 *
 * Note that this class is experimental, it is not intended to be used in production.
 */
@InterfaceAudience.Internal
class LocalCarbonStore extends MetaCachedCarbonStore {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LocalCarbonStore.class.getName());

  @Override
  public Iterator<CarbonRow> scan(AbsoluteTableIdentifier tableIdentifier, String[] projectColumns)
      throws IOException {
    return scan(tableIdentifier, projectColumns, null);
  }

  @Override
  public Iterator<CarbonRow> scan(AbsoluteTableIdentifier tableIdentifier, String[] projectColumns,
      Expression filter) throws IOException {
    Objects.requireNonNull(tableIdentifier);
    Objects.requireNonNull(projectColumns);

    CarbonTable table = getTable(tableIdentifier.getTablePath());
    if (table.isStreamingSink() || table.isHivePartitionTable()) {
      throw new UnsupportedOperationException("streaming and partition table is not supported");
    }
    // TODO: use InputFormat to prune data and read data

    final CarbonTableInputFormat format = new CarbonTableInputFormat();
    final Job job = new Job(new Configuration());
    CarbonInputFormat.setTableInfo(job.getConfiguration(), table.getTableInfo());
    CarbonInputFormat.setTablePath(job.getConfiguration(), table.getTablePath());
    CarbonInputFormat.setTableName(job.getConfiguration(), table.getTableName());
    CarbonInputFormat.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
    CarbonInputFormat.setCarbonReadSupport(job.getConfiguration(), CarbonRowReadSupport.class);
    CarbonInputFormat
        .setColumnProjection(job.getConfiguration(), new CarbonProjection(projectColumns));
    if (filter != null) {
      CarbonInputFormat
          .setFilterPredicates(job.getConfiguration(), new DataMapFilter(table, filter));
    }

    final List<InputSplit> splits =
        format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));

    List<RecordReader<Void, Object>> readers = new ArrayList<>(splits.size());

    List<CarbonRow> rows = new ArrayList<>();

    try {
      for (InputSplit split : splits) {
        TaskAttemptContextImpl attempt =
            new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        RecordReader reader = format.createRecordReader(split, attempt);
        reader.initialize(split, attempt);
        readers.add(reader);
      }

      for (RecordReader<Void, Object> reader : readers) {
        while (reader.nextKeyValue()) {
          rows.add((CarbonRow) reader.getCurrentValue());
        }
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      for (RecordReader<Void, Object> reader : readers) {
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    }
    return rows.iterator();
  }

  @Override
  public Iterator<CarbonRow> sql(String sqlString) {
    throw new UnsupportedOperationException();
  }
}
