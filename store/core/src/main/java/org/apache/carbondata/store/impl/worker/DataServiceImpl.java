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

package org.apache.carbondata.store.impl.worker;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.scan.executor.impl.SearchModeDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.SearchModeVectorDetailQueryExecutor;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVRecordReaderIterator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.sdk.store.util.StoreUtil;
import org.apache.carbondata.store.impl.DataOperation;
import org.apache.carbondata.store.impl.Status;
import org.apache.carbondata.store.impl.service.DataService;
import org.apache.carbondata.store.impl.service.model.BaseResponse;
import org.apache.carbondata.store.impl.service.model.LoadDataRequest;
import org.apache.carbondata.store.impl.service.model.ScanRequest;
import org.apache.carbondata.store.impl.service.model.ScanResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

@InterfaceAudience.Internal
public class DataServiceImpl implements DataService {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataServiceImpl.class.getCanonicalName());

  // temp location for loading (writing sort temp files)
  private String[] storeTempLocation;
  private Configuration hadoopConf;

  DataServiceImpl(Worker worker) {
    this.hadoopConf = worker.getHadoopConf();
    this.storeTempLocation = worker.getConf().storeTempLocation();
  }

  @Override
  public BaseResponse loadData(LoadDataRequest request) {
    DataLoadExecutor executor = null;
    try {
      CarbonLoadModel model = request.getModel();

      JobID jobId = CarbonInputFormatUtil.getJobId(new Date(), 0);
      CarbonInputFormatUtil.createJobTrackerID(new Date());
      TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
      TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
      Configuration configuration = new Configuration(hadoopConf);
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
      executor.execute(model, storeTempLocation, readerIterators);

      return new BaseResponse(Status.SUCCESS.ordinal(), "");
    } catch (IOException e) {
      LOGGER.error(e, "Failed to handle load data");
      return new BaseResponse(Status.FAILURE.ordinal(), e.getMessage());
    } catch (InterruptedException e) {
      LOGGER.error(e, "Interrupted handle load data ");
      return new BaseResponse(Status.FAILURE.ordinal(), e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed to execute load data ");
      return new BaseResponse(Status.FAILURE.ordinal(), e.getMessage());
    } finally {
      if (executor != null) {
        executor.close();
        StoreUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId());
      }
    }
  }

  @Override
  public ScanResponse scan(ScanRequest scan) {
    try {
      LOGGER.info(String.format("[QueryId:%d] receive search request", scan.getRequestId()));
      List<CarbonRow> rows = DataOperation.scan(scan.getTableInfo(), scan);
      LOGGER.info(String.format("[QueryId:%d] sending success response", scan.getRequestId()));
      return createSuccessResponse(scan, rows);
    } catch (IOException e) {
      LOGGER.error(e);
      LOGGER.info(String.format("[QueryId:%d] sending failure response", scan.getRequestId()));
      return createFailureResponse(scan, e);
    }
  }

  /**
   * create a failure response
   */
  private ScanResponse createFailureResponse(ScanRequest scan, Throwable throwable) {
    return new ScanResponse(scan.getRequestId(), Status.FAILURE.ordinal(),
        throwable.getMessage(), new Object[0][]);
  }

  /**
   * create a success response with result rows
   */
  private ScanResponse createSuccessResponse(ScanRequest scan, List<CarbonRow> rows) {
    Iterator<CarbonRow> itor = rows.iterator();
    Object[][] output = new Object[rows.size()][];
    int i = 0;
    while (itor.hasNext()) {
      output[i++] = itor.next().getData();
    }
    return new ScanResponse(scan.getRequestId(), Status.SUCCESS.ordinal(), "", output);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Shutting down worker...");
    SearchModeDetailQueryExecutor.shutdownThreadPool();
    SearchModeVectorDetailQueryExecutor.shutdownThreadPool();
    LOGGER.info("Worker shut down");
  }
}
