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

package org.apache.carbondata.core.statusmanager;

import java.io.Serializable;

public class SegmentRefreshInfo implements Serializable {

  private Long segmentUpdatedTimestamp = 0L;
  private Integer countOfFileInSegment;
  private Long segmentFileTimestamp = 0L;

  public SegmentRefreshInfo(Long segmentUpdatedTimestamp, Integer countOfFileInSegment,
      Long segmentFileTimestamp) {
    if (segmentUpdatedTimestamp != null) {
      this.segmentUpdatedTimestamp = segmentUpdatedTimestamp;
    }
    this.countOfFileInSegment = countOfFileInSegment;
    this.segmentFileTimestamp = segmentFileTimestamp;
  }

  public Long getSegmentUpdatedTimestamp() {
    return segmentUpdatedTimestamp;
  }

  public void setSegmentUpdatedTimestamp(Long segmentUpdatedTimestamp) {
    this.segmentUpdatedTimestamp = segmentUpdatedTimestamp;
  }

  public void setCountOfFileInSegment(Integer countOfFileInSegment) {
    this.countOfFileInSegment = countOfFileInSegment;
  }

  public Long getSegmentFileTimestamp() {
    return segmentFileTimestamp;
  }

  public boolean compare(Object o) {
    if (!(o instanceof SegmentRefreshInfo)) return false;

    SegmentRefreshInfo that = (SegmentRefreshInfo) o;
    return segmentUpdatedTimestamp > that.segmentUpdatedTimestamp
        || segmentFileTimestamp > that.segmentFileTimestamp || !countOfFileInSegment
        .equals(that.countOfFileInSegment);
  }

  @Override
  public int hashCode() {
    int result = segmentUpdatedTimestamp.hashCode();
    result = 31 * result + countOfFileInSegment.hashCode();
    return result;
  }
}
