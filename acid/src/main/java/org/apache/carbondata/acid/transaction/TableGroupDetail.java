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
import java.util.List;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

// holder of table group information
public class TableGroupDetail implements Serializable {

  private String tableGroupId;

  private String transactionLogAbsolutePath;

  private String transactionFileLockPath;

  // For across table transaction, there can be more than one identifiers
  private List<AbsoluteTableIdentifier> identifiers;

  public TableGroupDetail(String tableGroupId, String transactionLogAbsolutePath,
      String transactionFileLockPath, List<AbsoluteTableIdentifier> identifiers) {
    this.tableGroupId = tableGroupId;
    this.transactionLogAbsolutePath = transactionLogAbsolutePath;
    this.transactionFileLockPath = transactionFileLockPath;
    this.identifiers = identifiers;
  }

  public String getTableGroupId() {
    return tableGroupId;
  }

  public String getTransactionLogAbsolutePath() {
    return transactionLogAbsolutePath;
  }

  public String getTransactionFileLockPath() {
    return transactionFileLockPath;
  }

  public List<AbsoluteTableIdentifier> getIdentifiers() {
    return identifiers;
  }
}
