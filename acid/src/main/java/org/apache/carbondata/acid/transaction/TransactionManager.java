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

import java.sql.Timestamp;
import java.util.List;

// interface for handling transactions
public interface TransactionManager {

  /**
   * To register a transaction
   *
   * @param isGlobalTransaction If the transaction is across table (or controlled
   *                            by DDL like START and COMMIT TRANSACTIONS)
   *                            then isGlobalTransaction should be true.
   * @param operations          Single transaction can be associated with one or more operations,
   *                            need to pass that operation types to register the transaction.
   * @param tableGroupDetail        For a single table, it will be dbName_tableName.
   *                            For across table, it will be the id specified by the user.
   * @return TransactionDetail object
   */
  TransactionDetail registerTransaction(boolean isGlobalTransaction,
      List<TransactionOperation> operations, TableGroupDetail tableGroupDetail);

  /**
   * To commit the transaction
   *
   * @param transactionDetail that required to be committed
   */
  void commitTransaction(TransactionDetail transactionDetail);

  /**
   * To get the latest committed transaction for that table group.
   *
   * @param tableGroupDetail
   *
   * @return TransactionDetail object.
   */
  TransactionDetail getLatestCommittedTransaction(TableGroupDetail tableGroupDetail);

  /**
   * To get the transaction based on transaction id, used for time travel.
   *
   * @param transactionId    transaction id of the required transaction
   * @param tableGroupDetail
   * @return TransactionDetail object
   */
  TransactionDetail getTransactionById(String transactionId, TableGroupDetail tableGroupDetail);

  /**
   * To get the transaction based on timestamp, used for time travel.
   *
   * @param timestamp        timestamp to get the mapping transaction registered at that time.
   * @param tableGroupDetail
   * @return TransactionDetail object
   */
  TransactionDetail getTransactionByTimeStamp(Timestamp timestamp,
      TableGroupDetail tableGroupDetail);

  /**
   *
   * @param tableGroupDetail
   * @return
   */
  TransactionDetail getCurrentTransaction(TableGroupDetail tableGroupDetail);

  /**
   *
   * @param tableGroupDetail
   * @return
   */
  TransactionDetail getBaseTransaction(TableGroupDetail tableGroupDetail);

  /**
   * In some scenarios(like drop table or table group),
   * all the transactions and in-memory info about transactions can be cleaned for table group
   * using this interface
   *
   * @param tableGroupId
   */
  void clearTransactions(String tableGroupId);

}
