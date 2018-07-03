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

package org.apache.carbondata.store.master;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.net.BindException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.store.conf.StoreConf;
import org.apache.carbondata.store.exception.ExecutionTimeoutException;
import org.apache.carbondata.store.exception.StoreException;
import org.apache.carbondata.store.rpc.RegistryService;
import org.apache.carbondata.store.rpc.ServiceFactory;
import org.apache.carbondata.store.rpc.StoreService;
import org.apache.carbondata.store.rpc.impl.RegistryServiceImpl;
import org.apache.carbondata.store.rpc.impl.Status;
import org.apache.carbondata.store.rpc.model.BaseResponse;
import org.apache.carbondata.store.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.rpc.model.QueryRequest;
import org.apache.carbondata.store.rpc.model.QueryResponse;
import org.apache.carbondata.store.rpc.model.RegisterWorkerRequest;
import org.apache.carbondata.store.rpc.model.RegisterWorkerResponse;
import org.apache.carbondata.store.rpc.model.ShutdownRequest;
import org.apache.carbondata.store.scheduler.Schedulable;
import org.apache.carbondata.store.scheduler.Scheduler;
import org.apache.carbondata.store.util.StoreUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

/**
 * Master of CarbonSearch.
 * It provides a Registry service for worker to register.
 * And it provides search API to fire RPC call to workers.
 */

public class Master {

  private static Master instance = null;

  private static LogService LOGGER = LogServiceFactory.getLogService(Master.class.getName());

  private Map<String, SoftReference<CarbonTable>> cacheTables;

  // worker host address map to EndpointRef
  private StoreConf conf;
  private Configuration hadoopConf;
  private Random random = new Random();
  private RPC.Server registryServer = null;
  private Scheduler scheduler = new Scheduler();

  private Master(StoreConf conf) {
    cacheTables = new HashMap<>();
    this.conf = conf;
    this.hadoopConf = this.conf.newHadoopConf();
  }

  /**
   * start service and listen on port passed in constructor
   */
  public void startService() throws IOException {
    if (registryServer == null) {

      BindException exception;
      // we will try to create service at worse case 100 times
      int numTry = 100;
      String host = conf.masterHost();
      int port = conf.masterPort();
      LOGGER.info("building registry-service on " + host + ":" + port);

      RegistryService registryService = new RegistryServiceImpl(this);
      do {
        try {
          registryServer = new RPC.Builder(hadoopConf).setBindAddress(host).setPort(port)
              .setProtocol(RegistryService.class).setInstance(registryService).build();

          registryServer.start();
          numTry = 0;
          exception = null;
        } catch (BindException e) {
          // port is occupied, increase the port number and try again
          exception = e;
          LOGGER.error(e, "start registry-service failed");
          port = port + 1;
          numTry = numTry - 1;
        }
      } while (numTry > 0);
      if (exception != null) {
        // we have tried many times, but still failed to find an available port
        throw exception;
      }
      LOGGER.info("registry-service started");
    } else {
      LOGGER.info("Search mode master has already started");
    }
  }

  public void stopService() throws InterruptedException {
    if (registryServer != null) {
      registryServer.stop();
      registryServer.join();
      registryServer = null;
    }
  }

  public void stopAllWorkers() throws IOException {
    for (Schedulable worker : getWorkers()) {
      try {
        worker.service.shutdown(new ShutdownRequest("user"));
      } catch (Throwable throwable) {
        throw new IOException(throwable);
      }
      scheduler.removeWorker(worker.getAddress());
    }
  }

  /**
   * A new searcher is trying to register, add it to the map and connect to this searcher
   */
  public RegisterWorkerResponse addWorker(RegisterWorkerRequest request) throws IOException {
    LOGGER.info(
        "Receive Register request from worker " + request.getHostAddress() + ":" + request.getPort()
            + " with " + request.getCores() + " cores");
    String workerId = UUID.randomUUID().toString();
    String workerAddress = request.getHostAddress();
    int workerPort = request.getPort();
    LOGGER.info(
        "connecting to worker " + request.getHostAddress() + ":" + request.getPort() + ", workerId "
            + workerId);

    StoreService searchService = ServiceFactory.createStoreService(workerAddress, workerPort);
    scheduler.addWorker(
        new Schedulable(workerId, workerAddress, workerPort, request.getCores(), searchService));
    LOGGER.info("worker " + request + " registered");
    return new RegisterWorkerResponse(workerId);
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

  private void onFailure(Throwable e) throws IOException {
    throw new IOException("exception in worker: " + e.getMessage());
  }

  private void onTimeout() {
    throw new ExecutionTimeoutException();
  }

  public String getTableFolder(String database, String tableName) {
    return conf.storeLocation() + File.separator + database + File.separator + tableName;
  }

  public CarbonTable getTable(String database, String tableName) throws StoreException {
    String tablePath = getTableFolder(database, tableName);
    CarbonTable carbonTable;
    SoftReference<CarbonTable> reference = cacheTables.get(tablePath);
    if (reference != null) {
      carbonTable = reference.get();
      if (carbonTable != null) {
        return carbonTable;
      }
    }

    try {
      org.apache.carbondata.format.TableInfo tableInfo =
          CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(tablePath));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo tableInfo1 = schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "", "", "");
      tableInfo1.setTablePath(tablePath);
      carbonTable = CarbonTable.buildFromTableInfo(tableInfo1);
      cacheTables.put(tablePath, new SoftReference<>(carbonTable));
      return carbonTable;
    } catch (IOException e) {
      String message = "Failed to get table from " + tablePath;
      LOGGER.error(e, message);
      throw new StoreException(message);
    }
  }

  public boolean createTable(TableInfo tableInfo, boolean ifNotExists) throws IOException {
    AbsoluteTableIdentifier identifier = tableInfo.getOrCreateAbsoluteTableIdentifier();
    boolean tableExists = FileFactory.isFileExist(identifier.getTablePath());
    if (tableExists) {
      if (ifNotExists) {
        return true;
      } else {
        throw new IOException(
            "car't create table " + tableInfo.getDatabaseName() + "." + tableInfo.getFactTable()
                .getTableName() + ", because it already exists");
      }
    }

    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    String databaseName = tableInfo.getDatabaseName();
    String tableName = tableInfo.getFactTable().getTableName();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(tableInfo, databaseName, tableName);

    String schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    try {
      if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
        boolean isDirCreated = FileFactory.mkdirs(schemaMetadataPath, fileType);
        if (!isDirCreated) {
          throw new IOException("Failed to create the metadata directory " + schemaMetadataPath);
        }
      }
      ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
      thriftWriter.open(FileWriteOperation.OVERWRITE);
      thriftWriter.write(thriftTableInfo);
      thriftWriter.close();
      return true;
    } catch (IOException e) {
      LOGGER.error(e, "Failed to handle create table");
      throw e;
    }
  }

  private void openSegment(CarbonLoadModel loadModel, boolean isOverwriteTable) throws IOException {
    try {
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, isOverwriteTable);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to handle load data");
      throw e;
    }
  }

  private void closeSegment(CarbonLoadModel loadModel) throws IOException {
    try {
      CarbonLoaderUtil.updateTableStatusForFailure(loadModel, "");
    } catch (IOException e) {
      LOGGER.error(e, "Failed to close segment");
      throw e;
    }
  }

  private void commitSegment(CarbonLoadModel loadModel) throws IOException {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String segmentId = loadModel.getSegmentId();
    String segmentFileName = SegmentFileStore
        .writeSegmentFile(carbonTable, segmentId, String.valueOf(loadModel.getFactTimeStamp()));

    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    String tablePath = absoluteTableIdentifier.getTablePath();
    String metadataPath = CarbonTablePath.getMetadataPath(tablePath);
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(tablePath);

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    int retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
    int maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info("Acquired lock for tablepath" + tablePath + " for table status updation");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metadataPath);
        LoadMetadataDetails loadMetadataDetails = null;
        for (LoadMetadataDetails detail : listOfLoadFolderDetailsArray) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentId.equals(detail.getLoadName())) {
            loadMetadataDetails = detail;
            detail.setSegmentFile(segmentFileName);
            break;
          }
        }
        if (loadMetadataDetails == null) {
          throw new IOException("can not find segment: " + segmentId);
        }

        CarbonLoaderUtil.populateNewLoadMetaEntry(loadMetadataDetails, SegmentStatus.SUCCESS,
            loadModel.getFactTimeStamp(), true);
        CarbonLoaderUtil
            .addDataIndexSizeIntoMetaEntry(loadMetadataDetails, segmentId, carbonTable);

        SegmentStatusManager
            .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table path " + tablePath);
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + tablePath);
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + tablePath + " during table status updation");
      }
    }
  }

  public boolean loadData(CarbonLoadModel loadModel, boolean isOverwrite) throws IOException {
    Schedulable worker = scheduler.pickNexWorker();
    try {
      if (loadModel.getFactTimeStamp() == 0) {
        loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime());
      }
      openSegment(loadModel, isOverwrite);
      LoadDataRequest request = new LoadDataRequest(loadModel);
      BaseResponse response = scheduler.sendRequest(worker, request);
      if (Status.SUCCESS.ordinal() == response.getStatus()) {
        commitSegment(loadModel);
        return true;
      } else {
        closeSegment(loadModel);
        throw new IOException(response.getMessage());
      }
    } finally {
      worker.workload.decrementAndGet();
    }
  }

  /**
   * Execute search by firing RPC call to worker, return the result rows
   *
   * @param table       table to search
   * @param columns     projection column names
   * @param filter      filter expression
   * @param globalLimit max number of rows required in Master
   * @param localLimit  max number of rows required in Worker
   * @return CarbonRow array
   */
  public CarbonRow[] search(CarbonTable table, String[] columns, Expression filter,
      long globalLimit, long localLimit) throws IOException {
    Objects.requireNonNull(table);
    Objects.requireNonNull(columns);
    if (globalLimit < 0 || localLimit < 0) {
      throw new IllegalArgumentException("limit should be positive");
    }

    int queryId = random.nextInt();

    List<CarbonRow> output = new ArrayList<>();

    // prune data and get a mapping of worker hostname to list of blocks,
    // then add these blocks to the QueryRequest and fire the RPC call
    Map<String, List<Distributable>> nodeBlockMapping = pruneBlock(table, columns, filter);
    Set<Map.Entry<String, List<Distributable>>> entries = nodeBlockMapping.entrySet();
    List<Future<QueryResponse>> futures = new ArrayList<>(entries.size());
    List<Schedulable> workers = new ArrayList<>(entries.size());
    for (Map.Entry<String, List<Distributable>> entry : entries) {
      CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(entry.getValue(), entry.getKey());
      QueryRequest request =
          new QueryRequest(queryId, split, table.getTableInfo(), columns, filter, localLimit);

      // Find an Endpoind and send the request to it
      // This RPC is non-blocking so that we do not need to wait before send to next worker
      Schedulable worker = scheduler.pickWorker(entry.getKey());
      workers.add(worker);
      futures.add(scheduler.sendRequestAsync(worker, request));
    }

    int rowCount = 0;
    int length = futures.size();
    for (int i = 0; i < length; i++) {
      Future<QueryResponse> future = futures.get(i);
      Schedulable worker = workers.get(i);
      if (rowCount < globalLimit) {
        // wait for worker
        QueryResponse response = null;
        try {
          response = future
              .get((long) (CarbonProperties.getInstance().getQueryTimeout()), TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
          onFailure(e);
        } catch (TimeoutException t) {
          onTimeout();
        } finally {
          worker.workload.decrementAndGet();
        }
        LOGGER.info("[QueryId: " + queryId + "] receive search response from worker " + worker);
        rowCount += onSuccess(queryId, response, output, globalLimit);
      }
    }
    CarbonRow[] rows = new CarbonRow[output.size()];
    return output.toArray(rows);
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of host address to list of block
   */
  private Map<String, List<Distributable>> pruneBlock(CarbonTable table, String[] columns,
      Expression filter) throws IOException {
    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat format;
    try {
      format = CarbonInputFormatUtil
          .createCarbonTableInputFormat(job, table, columns, filter, null, null, true);
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
    return CarbonLoaderUtil.nodeBlockMapping(blockInfos, -1, getWorkerAddresses(),
        CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST, null);
  }

  /**
   * return hostname of all workers
   */
  public List<Schedulable> getWorkers() {
    return scheduler.getAllWorkers();
  }

  private List<String> getWorkerAddresses() {
    return scheduler.getAllWorkerAddresses();
  }

  public static synchronized Master getInstance(StoreConf conf) {
    if (instance == null) {
      instance = new Master(conf);
    }
    return instance;
  }

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: Master <log4j file> <properties file>");
      return;
    }

    StoreUtil.initLog4j(args[0]);
    StoreConf conf = new StoreConf(args[1]);
    Master master = getInstance(conf);
    master.stopService();
  }

}