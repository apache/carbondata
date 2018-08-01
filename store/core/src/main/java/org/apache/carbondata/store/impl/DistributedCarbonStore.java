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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.sdk.store.Loader;
import org.apache.carbondata.sdk.store.Scanner;
import org.apache.carbondata.sdk.store.SelectOption;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.sdk.store.exception.ExecutionTimeoutException;
import org.apache.carbondata.store.impl.master.Schedulable;
import org.apache.carbondata.store.impl.master.Scheduler;
import org.apache.carbondata.store.impl.rpc.model.BaseResponse;
import org.apache.carbondata.store.impl.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.impl.rpc.model.QueryResponse;
import org.apache.carbondata.store.impl.rpc.model.Scan;

/**
 * A CarbonStore that leverage multiple servers via RPC calls (Master and Workers)
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
class DistributedCarbonStore extends CarbonStoreBase {
  private static LogService LOGGER =
      LogServiceFactory.getLogService(DistributedCarbonStore.class.getCanonicalName());
  private SegmentTxnManager txnManager;
  private Scheduler scheduler;
  private Random random = new Random();

  DistributedCarbonStore(StoreConf storeConf) throws IOException {
    super(storeConf);
    this.scheduler = new Scheduler(storeConf);
    txnManager = SegmentTxnManager.getInstance();
  }

  @Override
  public void loadData(LoadDescriptor load) throws CarbonException {
    Objects.requireNonNull(load);
    CarbonTable table = null;
    try {
      table = metaProcessor.getTable(load.getTable());
    } catch (IOException e) {
      throw new CarbonException(e);
    }
    CarbonLoadModelBuilder builder = new CarbonLoadModelBuilder(table);
    builder.setInputPath(load.getInputPath());
    CarbonLoadModel loadModel;
    try {
      loadModel = builder.build(load.getOptions(), System.currentTimeMillis(), "0");
    } catch (InvalidLoadOptionException e) {
      LOGGER.error(e, "Invalid loadDescriptor options");
      throw new CarbonException(e);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to loadDescriptor data");
      throw new CarbonException(e);
    }

    Schedulable worker = scheduler.pickNexWorker();
    try {
      if (loadModel.getFactTimeStamp() == 0) {
        loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime());
      }
      txnManager.openSegment(loadModel, load.isOverwrite());
      LoadDataRequest request = new LoadDataRequest(loadModel);
      BaseResponse response = scheduler.sendRequest(worker, request);
      if (Status.SUCCESS.ordinal() == response.getStatus()) {
        txnManager.commitSegment(loadModel);
      } else {
        txnManager.closeSegment(loadModel);
        throw new CarbonException(response.getMessage());
      }
    } catch (IOException e) {
      throw new CarbonException(e);
    } finally {
      worker.workload.decrementAndGet();
    }
  }

  @Override
  public Loader newLoader(LoadDescriptor load) throws CarbonException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CarbonRow> scan(ScanDescriptor select) throws CarbonException {
    return scan(select, null);
  }

  @Override
  public List<CarbonRow> scan(ScanDescriptor scanDescriptor, SelectOption option)
      throws CarbonException {
    Objects.requireNonNull(scanDescriptor);
    try {
      CarbonTable carbonTable = metaProcessor.getTable(scanDescriptor.getTableIdentifier());
      Objects.requireNonNull(carbonTable);
      Objects.requireNonNull(scanDescriptor.getProjection());
      if (scanDescriptor.getLimit() < 0) {
        throw new IllegalArgumentException("limit should be positive");
      }

      // prune data and get a mapping of worker hostname to list of blocks,
      // then add these blocks to the Scan and fire the RPC call
      List<Distributable> blockInfos = pruneBlock(carbonTable, scanDescriptor.getFilter());
      return doScan(carbonTable, scanDescriptor.getProjection(), scanDescriptor.getFilter(),
          scanDescriptor.getLimit(), blockInfos);
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }

  @Override
  public Scanner newScanner() throws CarbonException {
    return new ScannerImpl(this);
  }

  /**
   * Execute scan by firing RPC call to worker, return the result rows
   *
   * @param table       table to search
   * @param requiredColumns     projection column names
   * @param filter      filter expression
   * @param limit max number of rows required
   * @return CarbonRow
   */
  List<CarbonRow> doScan(CarbonTable table, String[] requiredColumns, Expression filter,
      long limit, List<Distributable> blockInfos)
      throws IOException {
    int queryId = random.nextInt();
    List<CarbonRow> output = new ArrayList<>();
    Map<String, List<Distributable>> nodeBlockMapping =
        CarbonLoaderUtil.nodeBlockMapping(
            blockInfos, -1, scheduler.getAllWorkerAddresses(),
            CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST, null);

    Set<Map.Entry<String, List<Distributable>>> entries = nodeBlockMapping.entrySet();
    List<Future<QueryResponse>> futures = new ArrayList<>(entries.size());
    List<Schedulable> workers = new ArrayList<>(entries.size());
    for (Map.Entry<String, List<Distributable>> entry : entries) {
      CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(entry.getValue(), entry.getKey());
      Scan scan =
          new Scan(queryId, split, table.getTableInfo(), requiredColumns, filter, limit);

      // Find an Endpoind and send the request to it
      // This RPC is non-blocking so that we do not need to wait before send to next worker
      Schedulable worker = scheduler.pickWorker(entry.getKey());
      workers.add(worker);
      futures.add(scheduler.sendRequestAsync(worker, scan));
    }

    int rowCount = 0;
    int length = futures.size();
    for (int i = 0; i < length; i++) {
      Future<QueryResponse> future = futures.get(i);
      Schedulable worker = workers.get(i);
      if (rowCount < limit) {
        // wait for worker
        QueryResponse response = null;
        try {
          response = future
              .get((long) (CarbonProperties.getInstance().getQueryTimeout()), TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
          throw new IOException("exception in worker: " + e.getMessage());
        } catch (TimeoutException t) {
          throw new ExecutionTimeoutException();
        } finally {
          worker.workload.decrementAndGet();
        }
        LOGGER.info("[QueryId: " + queryId + "] receive search response from worker " + worker);
        rowCount += onSuccess(queryId, response, output, limit);
      }
    }
    return output;
  }

  private int onSuccess(int queryId, QueryResponse result, List<CarbonRow> output, long globalLimit)
      throws IOException {
    // in case of RPC success, collect all rows in response message
    if (result.getQueryId() != queryId) {
      throw new IOException(
          "queryId in response does not match request: " + result.getQueryId() + " != " + queryId);
    }
    if (result.getStatus() != Status.SUCCESS.ordinal()) {
      throw new IOException("failure in worker: " + result.getMessage());
    }
    int rowCount = 0;
    Object[][] rows = result.getRows();
    for (Object[] row : rows) {
      output.add(new CarbonRow(row));
      rowCount++;
      if (rowCount >= globalLimit) {
        break;
      }
    }
    LOGGER.info("[QueryId:" + queryId + "] accumulated result size " + rowCount);
    return rowCount;
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Shutting down all workers...");
    scheduler.stopAllWorkers();
    LOGGER.info("All workers are shut down");
    try {
      LOGGER.info("Stopping master...");
      scheduler.stopService();
      LOGGER.info("Master stopped");
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
