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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.sdk.store.CarbonStore;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.impl.rpc.model.Scan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

/**
 * Provides base functionality of CarbonStore, it contains basic implementation of metadata
 * management, data pruning and data scan logic.
 */
@InterfaceAudience.Internal
public abstract class CarbonStoreBase implements CarbonStore {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(CarbonStoreBase.class.getCanonicalName());

  MetaProcessor metaProcessor;
  private StoreConf storeConf;

  CarbonStoreBase(StoreConf storeConf) {
    this.storeConf = storeConf;
    this.metaProcessor = new MetaProcessor(this);
  }

  @Override
  public void createTable(TableDescriptor descriptor) throws CarbonException {
    Objects.requireNonNull(descriptor);
    metaProcessor.createTable(descriptor);
  }

  @Override
  public void dropTable(TableIdentifier table) throws CarbonException {
    Objects.requireNonNull(table);
    try {
      metaProcessor.dropTable(table);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public List<TableDescriptor> listTable() throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableDescriptor getDescriptor(TableIdentifier table) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(TableIdentifier table, TableDescriptor newTable) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  public String getTablePath(String tableName, String databaseName) {
    Objects.requireNonNull(tableName);
    Objects.requireNonNull(databaseName);
    return String.format("%s/%s", storeConf.storeLocation(), tableName);
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of host address to list of block.
   * This should be invoked in driver side.
   */
  public static List<Distributable> pruneBlock(CarbonTable table, String[] columns,
      Expression filter) throws IOException {
    Objects.requireNonNull(table);
    Objects.requireNonNull(columns);
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat format;
    try {
      format = CarbonInputFormatUtil.createCarbonTableInputFormat(
          job, table, columns, filter, null, null, true);
    } catch (InvalidConfigurationException e) {
      throw new IOException(e.getMessage());
    }

    // We will do FG pruning in reader side, so don't do it here
    CarbonInputFormat.setFgDataMapPruning(job.getConfiguration(), false);
    List<InputSplit> splits = format.getSplits(job);
    List<Distributable> blockInfos = new ArrayList<>(splits.size());
    for (InputSplit split : splits) {
      blockInfos.add((Distributable) split);
    }
    return blockInfos;
  }

  /**
   * Scan data and return matched rows. This should be invoked in worker side.
   * @param table carbon table
   * @param scan scan parameter
   * @return matched rows
   * @throws IOException if IO error occurs
   */
  public static List<CarbonRow> scan(CarbonTable table, Scan scan) throws IOException {
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(System.nanoTime());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);

    CarbonMultiBlockSplit mbSplit = scan.getSplit();
    long limit = scan.getLimit();
    QueryModel queryModel = createQueryModel(table, scan);

    LOGGER.info(String.format("[QueryId:%d] %s, number of block: %d", scan.getRequestId(),
        queryModel.toString(), mbSplit.getAllSplits().size()));

    // read all rows by the reader
    List<CarbonRow> rows = new LinkedList<>();
    try (CarbonRecordReader<CarbonRow> reader = new IndexedRecordReader(scan.getRequestId(),
        table, queryModel)) {
      reader.initialize(mbSplit, null);

      // loop to read required number of rows.
      // By default, if user does not specify the limit value, limit is Long.MaxValue
      long rowCount = 0;
      while (reader.nextKeyValue() && rowCount < limit) {
        rows.add(reader.getCurrentValue());
        rowCount++;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    LOGGER.info(String.format("[QueryId:%d] scan completed, return %d rows",
        scan.getRequestId(), rows.size()));
    return rows;
  }

  private static QueryModel createQueryModel(CarbonTable table, Scan scan) {
    String[] projectColumns = scan.getProjectColumns();
    Expression filter = null;
    if (scan.getFilterExpression() != null) {
      filter = scan.getFilterExpression();
    }
    return new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();
  }
}
