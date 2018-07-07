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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVRecordReaderIterator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.api.descriptor.LoadDescriptor;
import org.apache.carbondata.store.api.descriptor.SelectDescriptor;
import org.apache.carbondata.store.api.exception.StoreException;
import org.apache.carbondata.store.impl.distributed.rpc.model.Scan;
import org.apache.carbondata.store.util.StoreUtil;

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
public class LocalCarbonStore extends CarbonStoreBase {

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
  public void loadData(LoadDescriptor load) throws IOException, StoreException {
    Objects.requireNonNull(load);
    CarbonTable table = metaProcessor.getTable(load.getTable());
    CarbonLoadModelBuilder modelBuilder = new CarbonLoadModelBuilder(table);
    modelBuilder.setInputPath(load.getInputPath());
    CarbonLoadModel loadModel;
    try {
      loadModel = modelBuilder.build(load.getOptions(), System.currentTimeMillis(), "0");
    } catch (InvalidLoadOptionException e) {
      LOGGER.error(e, "Invalid loadDescriptor options");
      throw new StoreException(e.getMessage());
    }

    if (loadModel.getFactTimeStamp() == 0) {
      loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime());
    }

    try {
      txnManager.openSegment(loadModel, load.isOverwrite());
      loadData(loadModel);
      txnManager.commitSegment(loadModel);
    } catch (Exception e) {
      txnManager.closeSegment(loadModel);
      LOGGER.error(e, "Failed to load data");
      throw new StoreException(e);
    }
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
  public List<CarbonRow> select(SelectDescriptor select) throws IOException {
    Objects.requireNonNull(select);
    CarbonTable table = metaProcessor.getTable(select.getTable());
    List<Distributable> blocks = pruneBlock(table, select.getProjection(), select.getFilter());
    CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(blocks, "");
    Scan scan = new Scan(
        0, split, table.getTableInfo(), select.getProjection(), select.getFilter(),
        select.getLimit());
    return scan(table, scan);
  }

  @Override
  public void close() throws IOException {

  }
}
