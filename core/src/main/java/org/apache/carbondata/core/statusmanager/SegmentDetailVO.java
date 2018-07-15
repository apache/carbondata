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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SegmentDetailVO implements Serializable {

  public static final String SEGMENT_ID = "segmentid";

  public static final String STATUS = "status";

  public static final String LOAD_START_TIME = "load_start_time";

  public static final String LOAD_END_TIME = "load_end_time";

  public static final String IS_DELETED = "is_deleted";

  public static final String DATA_SIZE = "data_size";

  public static final String INDEX_SIZE = "index_size";

  public static final String UPDATE_DELTA_START_TIMESTAMP = "update_delta_start_timestamp";

  public static final String UPDATE_DELTA_END_TIMESTAMP = "update_delta_end_timestamp";

  public static final String UPDATE_STATUS_FILENAME = "update_status_fileName";

  public static final String MODIFICATION_OR_DELETION_TIMESTAMP =
      "modification_or_deletion_timesStamp";

  public static final String MERGED_SEGMENT_IDS = "merged_segment_ids";

  public static final String VISIBILITY = "visibility";

  public static final String MAJOR_COMPACTED = "major_compacted";

  public static final String FILE_FORMAT = "fileFormat";

  public static final String SEGMENT_FILE_NAME = "segment_file_name";

  public static final String TRANSACTION_ID = "transaction_id";

  private static final long serialVersionUID = 2473467379760429846L;

  public Map<String, Object> fields;

  public SegmentDetailVO() {
    this.fields = new HashMap<>();
  }

  public SegmentDetailVO setSegmentId(String segmentId) {
    fields.put(SEGMENT_ID, segmentId);
    return this;
  }

  public String getSegmentId() {
    Object value = fields.get(SEGMENT_ID);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setStatus(String status) {
    fields.put(STATUS, status);
    return this;
  }

  public String getStatus() {
    Object value = fields.get(STATUS);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setLoadStartTime(Long startTime) {
    fields.put(LOAD_START_TIME, startTime);
    return this;
  }

  public Long getLoadStartTime() {
    Object value = fields.get(LOAD_START_TIME);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setLoadEndTime(Long loadEndTime) {
    fields.put(LOAD_END_TIME, loadEndTime);
    return this;
  }

  public Long getLoadEndTime() {
    Object value = fields.get(LOAD_END_TIME);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setIsDeleted(Boolean isDeleted) {
    fields.put(IS_DELETED, isDeleted);
    return this;
  }

  public Boolean getIsDeleted() {
    Object value = fields.get(IS_DELETED);
    return value != null ? (Boolean) value : false;
  }

  public SegmentDetailVO setDataSize(Long dataSize) {
    fields.put(DATA_SIZE, dataSize);
    return this;
  }

  public Long getDataSize() {
    Object value = fields.get(DATA_SIZE);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setIndexSize(Long indexSize) {
    fields.put(INDEX_SIZE, indexSize);
    return this;
  }

  public Long getIndexSize() {
    Object value = fields.get(INDEX_SIZE);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setUpdateDeltaStartTimestamp(Long updateDeltaStartTimestamp) {
    fields.put(UPDATE_DELTA_START_TIMESTAMP, updateDeltaStartTimestamp);
    return this;
  }

  public Long getUpdateDeltaStartTimestamp() {
    Object value = fields.get(UPDATE_DELTA_START_TIMESTAMP);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setUpdateDeltaEndTimestamp(Long updateDeltaEndTimestamp) {
    fields.put(UPDATE_DELTA_END_TIMESTAMP, updateDeltaEndTimestamp);
    return this;
  }

  public Long getUpdateDeltaEndTimestamp() {
    Object value = fields.get(UPDATE_DELTA_END_TIMESTAMP);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setUpdateStatusFilename(String updateStatusFilename) {
    fields.put(UPDATE_STATUS_FILENAME, updateStatusFilename);
    return this;
  }

  public String getUpdateStatusFilename() {
    Object value = fields.get(UPDATE_STATUS_FILENAME);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setModificationOrDeletionTimestamp(Long modificationOrDeletionTimestamp) {
    fields.put(MODIFICATION_OR_DELETION_TIMESTAMP, modificationOrDeletionTimestamp);
    return this;
  }

  public Long getModificationOrDeletionTimestamp() {
    Object value = fields.get(MODIFICATION_OR_DELETION_TIMESTAMP);
    return value != null ? (Long) value : null;
  }

  public SegmentDetailVO setMergedSegmentIds(String mergedSegmentIds) {
    fields.put(MERGED_SEGMENT_IDS, mergedSegmentIds);
    return this;
  }

  public String getMergedSegmentIds() {
    Object value = fields.get(MERGED_SEGMENT_IDS);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setVisibility(boolean visibility) {
    fields.put(VISIBILITY, visibility);
    return this;
  }

  public Boolean getVisibility() {
    Object value = fields.get(VISIBILITY);
    return value != null ? (Boolean) value : true;
  }

  public SegmentDetailVO setMajorCompacted(String majorCompacted) {
    fields.put(MAJOR_COMPACTED, majorCompacted);
    return this;
  }

  public String getMajorCompacted() {
    Object value = fields.get(MAJOR_COMPACTED);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setFileFormat(String fileFormat) {
    fields.put(FILE_FORMAT, fileFormat);
    return this;
  }

  public String getFileFormat() {
    Object value = fields.get(FILE_FORMAT);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setSegmentFileName(String segmentFileName) {
    fields.put(SEGMENT_FILE_NAME, segmentFileName);
    return this;
  }

  public String getSegmentFileName() {
    Object value = fields.get(SEGMENT_FILE_NAME);
    return value != null ? value.toString() : null;
  }

  public String getTransactionId() {
    Object value = fields.get(TRANSACTION_ID);
    return value != null ? value.toString() : null;
  }

  public SegmentDetailVO setTransactionId(String transactionId) {
    fields.put(TRANSACTION_ID, transactionId);
    return this;
  }

  public Map<String, Object> getAllFields() {
    return fields;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SegmentDetailVO detailVO = (SegmentDetailVO) o;
    return Objects.equals(fields.get(SEGMENT_ID), detailVO.fields.get(SEGMENT_ID));
  }

  @Override public int hashCode() {

    return Objects.hash(fields.get(SEGMENT_ID));
  }
}
