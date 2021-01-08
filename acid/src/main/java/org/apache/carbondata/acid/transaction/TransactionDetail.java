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

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.annotations.Expose;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;

// holder for current transaction information
public class TransactionDetail implements Serializable {

  private String transactionId;

  private boolean isGlobalTransaction;

  private Timestamp timeStamp;

  private List<TransactionOperation> operations;

  // TODO make it invisible in the transaction log
  private TableGroupDetail tableGroupDetail;

  private Set<String> involvedSegments;

  public TransactionDetail(String transactionId, Boolean isGlobalTransaction,
      List<TransactionOperation> operations, TableGroupDetail tableGroupDetail) {
    this.transactionId = transactionId;
    this.isGlobalTransaction = isGlobalTransaction;
    this.operations = operations;
    this.tableGroupDetail = tableGroupDetail;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public Timestamp getTimeStamp() {
    return timeStamp;
  }

  public List<TransactionOperation> getOperations() {
    return operations;
  }

  public void setTimeStamp(Timestamp timeStamp) {
    this.timeStamp = timeStamp;
  }

  public void setInvolvedSegments(Set<String> involvedSegments) {
    this.involvedSegments = involvedSegments;
  }

  public void setInvolvedSegments(String segment) {
    if (this.involvedSegments == null) {
      this.involvedSegments = new HashSet<>();
    }
    involvedSegments.add(segment);
  }

  public TableGroupDetail getTableGroupDetail() {
    return tableGroupDetail;
  }

  public boolean isGlobalTransaction() {
    return isGlobalTransaction;
  }

  public Set<String> getInvolvedSegments() {
    return involvedSegments;
  }
}
