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

package org.apache.carbondata.core.mutate;

import java.io.Serializable;
import java.util.Objects;

/**
 * VO class for storing details related to Update operation.
 */
public class UpdateVO implements Serializable {
  private static final long serialVersionUID = 1L;

  private Long factTimestamp;

  private Long updateDeltaStartTimestamp;

  private String segmentId;

  public Long getLatestUpdateTimestamp() {
    return latestUpdateTimestamp;
  }

  public void setLatestUpdateTimestamp(Long latestUpdateTimestamp) {
    this.latestUpdateTimestamp = latestUpdateTimestamp;
  }

  private Long latestUpdateTimestamp;

  public Long getFactTimestamp() {
    return factTimestamp;
  }

  public void setFactTimestamp(Long factTimestamp) {
    this.factTimestamp = factTimestamp;
  }

  public Long getUpdateDeltaStartTimestamp() {
    return updateDeltaStartTimestamp;
  }

  public void setUpdateDeltaStartTimestamp(Long updateDeltaStartTimestamp) {
    this.updateDeltaStartTimestamp = updateDeltaStartTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UpdateVO updateVO = (UpdateVO) o;
    if (!Objects.equals(factTimestamp, updateVO.factTimestamp)) {
      return false;
    }
    if (!Objects.equals(updateDeltaStartTimestamp, updateVO.updateDeltaStartTimestamp)) {
      return false;
    }
    return Objects.equals(latestUpdateTimestamp, updateVO.latestUpdateTimestamp);
  }

  @Override
  public int hashCode() {
    int result = factTimestamp != null ? factTimestamp.hashCode() : 0;
    result = 31 * result + (updateDeltaStartTimestamp != null ?
        updateDeltaStartTimestamp.hashCode() :
        0);
    result = 31 * result + (latestUpdateTimestamp != null ? latestUpdateTimestamp.hashCode() : 0);
    return result;
  }

  /**
   * This will return the update timestamp if its present or it will return the fact timestamp.
   * @return
   */
  public Long getCreatedOrUpdatedTimeStamp() {
    if (null == latestUpdateTimestamp) {
      return factTimestamp;
    }
    return latestUpdateTimestamp;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }
}
