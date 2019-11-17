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

package org.apache.carbondata.core.scan.executor.infos;

import java.util.Arrays;

import org.apache.carbondata.core.mutate.CarbonUpdateUtil;

/**
 * class to hold information about delete delta files
 */
public class DeleteDeltaInfo {

  /**
   * delete delta files
   */
  private String[] deleteDeltaFile;

  /**
   * latest delete delta file timestamp
   */
  private long latestDeleteDeltaFileTimestamp;

  public DeleteDeltaInfo(String[] deleteDeltaFile) {
    this.deleteDeltaFile = deleteDeltaFile;
    this.latestDeleteDeltaFileTimestamp =
        CarbonUpdateUtil.getLatestDeleteDeltaTimestamp(deleteDeltaFile);
  }

  public String[] getDeleteDeltaFile() {
    return deleteDeltaFile;
  }

  public long getLatestDeleteDeltaFileTimestamp() {
    return latestDeleteDeltaFileTimestamp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(deleteDeltaFile);
    result =
        prime * result + (int) (latestDeleteDeltaFileTimestamp ^ (latestDeleteDeltaFileTimestamp
            >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DeleteDeltaInfo other = (DeleteDeltaInfo) obj;
    if (!Arrays.equals(deleteDeltaFile, other.deleteDeltaFile)) {
      return false;
    }
    if (latestDeleteDeltaFileTimestamp != other.latestDeleteDeltaFileTimestamp) {
      return false;
    }
    return true;
  }

}
