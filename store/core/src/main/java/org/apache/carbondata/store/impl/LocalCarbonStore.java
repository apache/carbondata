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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVRecordReaderIterator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.sdk.store.CarbonStore;
import org.apache.carbondata.sdk.store.KeyedRow;
import org.apache.carbondata.sdk.store.PrimaryKey;
import org.apache.carbondata.sdk.store.Row;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.util.StoreUtil;
import org.apache.carbondata.store.impl.service.model.ScanRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * A CarbonStore implementation that works locally, without other compute framework dependency.
 * It can be used to read data in local disk.
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class LocalCarbonStore extends TableManager implements CarbonStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LocalCarbonStore.class.getName());

  private StoreConf storeConf;
  private Configuration configuration;
  private SegmentTxnManager txnManager;

  public LocalCarbonStore(StoreConf storeConf) {
    this(storeConf, new Configuration());
  }

  public LocalCarbonStore(StoreConf storeConf, Configuration hadoopConf) {
    super(storeConf);
    this.storeConf = storeConf;
    this.txnManager = SegmentTxnManager.getInstance();
    this.configuration = new Configuration(hadoopConf);
  }

  @Override
  public void loadData(LoadDescriptor load) throws CarbonException {
    Objects.requireNonNull(load);
    CarbonLoadModel loadModel;
    try {
      TableInfo tableInfo = getTable(load.getTable());
      CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
      CarbonLoadModelBuilder modelBuilder = new CarbonLoadModelBuilder(table);
      modelBuilder.setInputPath(load.getInputPath());
      loadModel = modelBuilder.build(load.getOptions(), System.currentTimeMillis(), "0");
    } catch (InvalidLoadOptionException e) {
      LOGGER.error(e, "Invalid loadDescriptor options");
      throw new CarbonException(e);
    } catch (IOException e) {
      throw new CarbonException(e);
    }

    if (loadModel.getFactTimeStamp() == 0) {
      loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime());
    }

    try {
      txnManager.openSegment(loadModel, load.isOverwrite());
      loadData(loadModel);
      txnManager.commitSegment(loadModel);
    } catch (Exception e) {
      LOGGER.error(e, "Failed to load data");
      try {
        txnManager.closeSegment(loadModel);
      } catch (IOException ex) {
        LOGGER.error(ex, "Failed to close segment");
        // Ignoring the exception
      }
      throw new CarbonException(e);
    }
  }

  @Override
  public void upsert(Iterator<KeyedRow> row, StructType schema) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(Iterator<PrimaryKey> keys) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  private void loadData(CarbonLoadModel model) throws Exception {
    DataLoadExecutor executor = null;
    try {
      JobID jobId = CarbonInputFormatUtil.getJobId(new Date(), 0);
      CarbonInputFormatUtil.createJobTrackerID(new Date());
      TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
      TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
      StoreUtil.configureCSVInputFormat(configuration, model);
      configuration.set(FileInputFormat.INPUT_DIR, model.getFactFilePath());
      // Set up the attempt context required to use in the output committer.
      TaskAttemptContext hadoopAttemptContext =
          new TaskAttemptContextImpl(configuration, taskAttemptId);

      CSVInputFormat format = new CSVInputFormat();
      List<InputSplit> splits = format.getSplits(hadoopAttemptContext);

      CarbonIterator<Object[]>[] readerIterators = new CSVRecordReaderIterator[splits.size()];
      for (int index = 0; index < splits.size(); index++) {
        readerIterators[index] = new CSVRecordReaderIterator(
            format.createRecordReader(splits.get(index), hadoopAttemptContext), splits.get(index),
            hadoopAttemptContext);
      }

      executor = new DataLoadExecutor();
      executor.execute(model, storeConf.storeTempLocation(), readerIterators);
    } finally {
      if (executor != null) {
        executor.close();
        StoreUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId());
      }
    }
  }

  @Override
  public List<CarbonRow> scan(ScanDescriptor scanDescriptor) throws CarbonException {
    Objects.requireNonNull(scanDescriptor);
    try {
      TableInfo tableInfo = getTable(scanDescriptor.getTableIdentifier());
      CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
      List<Distributable> blocks = pruneBlock(table, scanDescriptor.getFilter());
      CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(blocks, "");
      ScanRequest scan =
          new ScanRequest(0, split, tableInfo, scanDescriptor.getProjection(),
              scanDescriptor.getFilter(), scanDescriptor.getLimit());
      return scan(table, scan);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public Row lookup(PrimaryKey key) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Row> lookup(TableIdentifier tableIdentifier, String filterExpression)
      throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }
}
