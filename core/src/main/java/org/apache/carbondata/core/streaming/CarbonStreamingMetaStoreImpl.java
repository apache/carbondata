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

import java.io.IOException;

/* JSON format can be used to store the metadata */

public class CarbonStreamingMetaStoreImpl implements CarbonStreamingMetaStore {

  /**
   * get commit info from metastore
   */
  public CarbonStreamingCommitInfo getStreamingCommitInfo(
          String dataBase,
          String table,
          long segmentID,
          String partitionID) throws IOException {

    return null;

  }

  /**
   * Update commit info in metastore
   */
  public void updateStreamingCommitInfo(
          CarbonStreamingMetaStore commitInfo) throws IOException {

  }

  /**
   * Recover streaming data using valid offset in commit info
   */
  public void recoverStreamingData(
          CarbonStreamingCommitInfo commitInfo) throws IOException {

  }

}
