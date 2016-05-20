/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.load;

import java.io.Serializable;

public class LoadMetadataDetails implements Serializable {

  private static final long serialVersionUID = 1106104914918491724L;
  private String timestamp;
  private String loadStatus;
  private String loadName;
  private String partitionCount;
  /**
   * Segment modification or deletion time stamp
   */
  private String modificationOrdeletionTimesStamp;
  private String loadStartTime;

  private String mergedLoadName;
  /**
   * visibility is used to determine whether to the load is visible or not.
   */
  private String visibility = "true";

  public String getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(String partitionCount) {
    this.partitionCount = partitionCount;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getLoadStatus() {
    return loadStatus;
  }

  public void setLoadStatus(String loadStatus) {
    this.loadStatus = loadStatus;
  }

  public String getLoadName() {
    return loadName;
  }

  public void setLoadName(String loadName) {
    this.loadName = loadName;
  }

  /**
   * @return the modificationOrdeletionTimesStamp
   */
  public String getModificationOrdeletionTimesStamp() {
    return modificationOrdeletionTimesStamp;
  }

  /**
   * @param modificationOrdeletionTimesStamp the modificationOrdeletionTimesStamp to set
   */
  public void setModificationOrdeletionTimesStamp(String modificationOrdeletionTimesStamp) {
    this.modificationOrdeletionTimesStamp = modificationOrdeletionTimesStamp;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((loadName == null) ? 0 : loadName.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof LoadMetadataDetails)) {
      return false;
    }
    LoadMetadataDetails other = (LoadMetadataDetails) obj;
    if (loadName == null) {
      if (other.loadName != null) {
        return false;
      }
    } else if (!loadName.equals(other.loadName)) {
      return false;
    }
    return true;
  }

  /**
   * @return the startLoadTime
   */
  public String getLoadStartTime() {
    return loadStartTime;
  }

  /**
   * @param loadStartTime
   */
  public void setLoadStartTime(String loadStartTime) {
    this.loadStartTime = loadStartTime;
  }

  /**
   * @return the mergedLoadName
   */
  public String getMergedLoadName() {
    return mergedLoadName;
  }

  /**
   * @param mergedLoadName the mergedLoadName to set
   */
  public void setMergedLoadName(String mergedLoadName) {
    this.mergedLoadName = mergedLoadName;
  }

  /**
   * @return the visibility
   */
  public String getVisibility() {
    return visibility;
  }

  /**
   * @param visibility the visibility to set
   */
  public void setVisibility(String visibility) {
    this.visibility = visibility;
  }

}
