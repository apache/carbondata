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

package org.apache.carbondata.acid.transaction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.acid.FileBasedSegmentStore;
import org.apache.carbondata.acid.SegmentStore;
import org.apache.carbondata.acid.SegmentStoreFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

import static org.apache.carbondata.core.util.CarbonUtil.closeStreams;

public final class CarbonFileBasedTransactionManager implements TransactionManager {

  private static final InheritableThreadLocal<TransactionDetail> baseTransactionThreadLocal =
      new InheritableThreadLocal<>();

  private static final InheritableThreadLocal<TransactionDetail> currentTransactionThreadLocal =
      new InheritableThreadLocal<>();

  private static Map<String, TransactionDetail> tableGroupsLatestTransaction =
      new ConcurrentHashMap<>();

  @Override
  public TransactionDetail registerTransaction(boolean isGlobalTransaction,
      List<TransactionOperation> operations, TableGroupDetail tableGroupDetail) {
    TransactionDetail detail =
        new TransactionDetail(UUID.randomUUID().toString(), isGlobalTransaction, operations,
            tableGroupDetail);
    baseTransactionThreadLocal.set(getLatestCommittedTransaction(tableGroupDetail));
    currentTransactionThreadLocal.set(detail);
    return detail;
  }

  @Override
  public void commitTransaction(TransactionDetail transactionDetail) {
    String lockPath = transactionDetail.getTableGroupDetail().getTransactionFileLockPath();
    ICarbonLock transactionFileLock =
        CarbonLockFactory.getCarbonLockObjWithInputPath(LockUsage.TRANSACTION_LOG_LOCK, lockPath);
    try {
      int retryCount = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
      int maxTimeout = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
      if (transactionFileLock.lockWithRetries(retryCount, maxTimeout)) {
        transactionDetail.setTimeStamp(new Timestamp(System.currentTimeMillis()));
        TransactionDetail latestCommittedTransaction =
            getLatestCommittedTransaction(transactionDetail.getTableGroupDetail());
        TransactionDetail baseTransaction =
            getBaseTransaction(transactionDetail.getTableGroupDetail());
        if (latestCommittedTransaction != null && !baseTransaction.getTransactionId()
            .equals(latestCommittedTransaction.getTransactionId())) {
          // If base transaction of current transaction is not same as latest committed transaction,
          // there can be a conflict due to concurrent operations.
          // So, check and resolve the conflict.
          if (!checkAndResolveConflict(baseTransaction,
              getCurrentTransaction(transactionDetail.getTableGroupDetail()))) {
            throw new RuntimeException(
                "Please retry the operation, "
                    + "unable to commit the transaction due to conflict from concurrent operation");
          }
        }
        TransactionDetail[] transactionDetails =
            readTransactionLog(transactionDetail.getTableGroupDetail());
        List<TransactionDetail> details = new ArrayList<>();
        // Adding it at the HEAD, so that the recent transaction will be on top
        details.add(transactionDetail);
        details.addAll(Arrays.asList(transactionDetails));
        writeTransactionLog(details,
            transactionDetail.getTableGroupDetail().getTransactionLogAbsolutePath()
                + CarbonCommonConstants.FILE_SEPARATOR + transactionDetail.getTableGroupDetail()
                .getTableGroupId() + "_transaction_log");
      } else {
        throw new RuntimeException(
            "Unable to acquire Transaction file lock, please retry the operation");
      }
    } catch (IOException ex) {
      throw new RuntimeException(
          "Failed to commit the transaction" + ex.getMessage());
    } finally {
      CarbonLockUtil.fileUnlock(transactionFileLock, LockUsage.TRANSACTION_LOG_LOCK);
    }
    currentTransactionThreadLocal.remove();
    baseTransactionThreadLocal.remove();
    tableGroupsLatestTransaction
        .put(transactionDetail.getTableGroupDetail().getTableGroupId(), transactionDetail);
  }

  @Override
  public TransactionDetail getLatestCommittedTransaction(TableGroupDetail tableGroupDetail) {
    return tableGroupsLatestTransaction.get(tableGroupDetail.getTableGroupId());
  }

  @Override
  public TransactionDetail getCurrentTransaction(TableGroupDetail tableGroupDetail) {
    return currentTransactionThreadLocal.get();
  }

  @Override
  public TransactionDetail getBaseTransaction(TableGroupDetail tableGroupDetail) {
    return baseTransactionThreadLocal.get();
  }

  @Override
  public TransactionDetail getTransactionById(String transactionId,
      TableGroupDetail tableGroupDetail) {
    // TODO: need to read the file and get the transaction id
    // can add some cache to avoid IO for every time during time travel operations ?
    // may be need to keep a map by transaction id and BST by timestamp
    return null;
  }

  @Override
  public TransactionDetail getTransactionByTimeStamp(Timestamp timestamp,
      TableGroupDetail tableGroupDetail) {
    // TODO: need to read the file and get the transaction id
    // can add some cache to avoid IO for every time during time travel operations ?
    // may be need to keep a map by transaction id and BST by timestamp
    return null;
  }

  /**
   * will be called during the drop table to remove latestCommittedTransaction entry from the map
   *
   * @param tableGroupId For a single table, it will be dbName_tableName.
   *                     For across table, it will the id specified by the user.
   */
  @Override
  public void clearTransactions(String tableGroupId) {
    tableGroupsLatestTransaction.remove(tableGroupId);
  }

  private void writeTransactionLog(List<TransactionDetail> details, String path)
      throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(path);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the metadata file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(
          new OutputStreamWriter(dataOutputStream, CarbonCommonConstants.DEFAULT_CHARSET));
      String metadataInstance = gsonObjectToWrite.toJson(details);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      //      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }
  }

  private TransactionDetail[] readTransactionLog(TableGroupDetail tableGroupDetail) {
    String filePath =
        tableGroupDetail.getTransactionLogAbsolutePath() + CarbonCommonConstants.UNDERSCORE
            + tableGroupDetail.getTableGroupId() + "_transaction_log";
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    TransactionDetail[] transactionDetails;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(filePath);
    try {
      if (!FileFactory.isFileExist(filePath)) {
        return new TransactionDetail[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream, CarbonCommonConstants.DEFAULT_CHARSET);
      buffReader = new BufferedReader(inStream);
      transactionDetails = gsonObjectToRead.fromJson(buffReader, TransactionDetail[].class);
    } catch (IOException e) {
      return new TransactionDetail[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }
    return transactionDetails;
  }

  private boolean checkAndResolveConflict(TransactionDetail baseTransaction,
      TransactionDetail currentTransaction) throws IOException {
    // TODO: Need to see if conflict resolve logic can be improved based on operation type
    // TODO: for insert overwrite and alter schema,
    //  list of affected segment may not conflict with current one,
    //  so need to add check based on operation also.
    List<TransactionDetail> latestCommittedTransactions =
        getTransactionsAfterBaseTransaction(baseTransaction);
    Set<String> latestCommittedTransactionSegments = new HashSet<>();
    for (TransactionDetail transaction : latestCommittedTransactions) {
      latestCommittedTransactionSegments.addAll(transaction.getInvolvedSegments());
    }
    if (!Collections.disjoint(currentTransaction.getInvolvedSegments(), latestCommittedTransactionSegments)) {
      return false;
    }
    // Update the current table status file,
    // by adding current transaction on top of latest committed transaction table status file.
    SegmentStore segmentStore = SegmentStoreFactory.create(currentTransaction, baseTransaction);
    if (segmentStore instanceof FileBasedSegmentStore) {
      List<AbsoluteTableIdentifier> identifiers =
          currentTransaction.getTableGroupDetail().getIdentifiers();
      for (AbsoluteTableIdentifier identifier : identifiers) {
        ((FileBasedSegmentStore) segmentStore)
            .updateCurrentTableStatusWithNewBase(identifier, currentTransaction,
                getLatestCommittedTransaction(currentTransaction.getTableGroupDetail()));
      }
    } else {
      // TODO: handle for db or other implementation
    }
    return true;
  }

  private List<TransactionDetail> getTransactionsAfterBaseTransaction(
      TransactionDetail baseTransaction) {
    TransactionDetail[] transactionDetails =
        readTransactionLog(baseTransaction.getTableGroupDetail());
    // latest transactions will be in the beginning of array
    List<TransactionDetail> latestCommittedTransactions = new ArrayList<>();
    for (TransactionDetail transaction : transactionDetails) {
      if (transaction.getTransactionId().equals(baseTransaction.getTransactionId())) {
        break;
      }
      latestCommittedTransactions.add(transaction);
    }
    return latestCommittedTransactions;
  }

}
