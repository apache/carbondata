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

import com.google.gson.annotations.SerializedName;

/**
 * Status of one segment. This enum is serialized into table_status file, so
 * please ensure the SerializedName is backward compatible when modifying this enum
 */
public enum SegmentStatus {

  /**
   * Data load success, it is visible for read
   */
  @SerializedName("Success")
  SUCCESS("Success"),

  /**
   * Data load failed
   */
  @SerializedName("Failure")
  LOAD_FAILURE("Failure"),

  /**
   * Data load partial success
   */
  @SerializedName("Partial Success")
  LOAD_PARTIAL_SUCCESS("Partial Success"),

  /**
   * Segment has been deleted by user or compactor
   */
  @SerializedName("Marked for Delete")
  MARKED_FOR_DELETE("Marked for Delete"),

  /**
   * Segment has been updated by user
   */
  @SerializedName("Marked for Update")
  MARKED_FOR_UPDATE("Marked for Update"),

  /**
   * Segment is compacted
   */
  @SerializedName("Compacted")
  COMPACTED("Compacted"),

  /**
   * Insert overwrite operation is in progress
   */
  @SerializedName("Overwrite In Progress")  // This string can't be modified due to compatibility
  INSERT_OVERWRITE_IN_PROGRESS("Insert Overwrite In Progress"),

  /**
   * insert into operation is in progress
   */
  @SerializedName("In Progress")  // This string can't be modified due to compatibility
  INSERT_IN_PROGRESS("Insert In Progress"),

  /**
   * Streaming ingest in progress, for streaming segment
   */
  @SerializedName("Streaming")
  STREAMING("Streaming"),

  /**
   * Streaming ingest finish, for streaming segment
   */
  @SerializedName("Streaming Finish")
  STREAMING_FINISH("Streaming Finish"),

  /**
   * This status is not used, keep it here just for backward compatibility
   */
  @SerializedName("Update")
  UPDATE("Update");

  private String message;

  SegmentStatus(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return message;
  }
}
