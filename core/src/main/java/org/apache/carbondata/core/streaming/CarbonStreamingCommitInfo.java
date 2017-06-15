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

package org.apache.carbondata.core.streaming;

/*
 * Commit info for streaming writes
 * The commit info can be used to recover valid offset in the file
 * in the case of write failure.
 */

public class CarbonStreamingCommitInfo {

  private String dataBase;

  private String table;

  private long commitTime;

  private long segmentID;

  private String partitionID;

  private long batchID;

  private String fileOffset;

  private long transactionID;     // future use

  public  CarbonStreamingCommitInfo(

      String dataBase,

      String table,

      long commitTime,

      long segmentID,

      String partitionID,

      long batchID) {

    this.dataBase = dataBase;

    this.table = table;

    this.commitTime = commitTime;

    this.segmentID = segmentID;

    this.partitionID = partitionID;

    this.batchID = batchID;

    this.transactionID = -1;
  }

  public String getDataBase() {
    return dataBase;
  }

  public String getTable() {
    return table;
  }

  public long getCommitTime() {
    return commitTime;
  }

  public long getSegmentID() {
    return segmentID;
  }

  public String getPartitionID() {
    return partitionID;
  }

  public long getBatchID() {
    return batchID;
  }

  public String getFileOffset() {
    return fileOffset;
  }

  public long getTransactionID() {
    return transactionID;
  }

  @Override
  public String toString() {
    return dataBase + "." + table + "." + segmentID + "$" + partitionID;
  }
}
