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

package org.apache.carbondata.core.locks;

/**
 * This enum is used to define the usecase of the lock.
 * Each enum value is one specific lock case.
 */
public class LockUsage {
  public static final String LOCK = ".lock";
  public static final String METADATA_LOCK = "meta.lock";
  public static final String COMPACTION_LOCK = "compaction.lock";
  public static final String HANDOFF_LOCK = "handoff.lock";
  public static final String SYSTEMLEVEL_COMPACTION_LOCK = "system_level_compaction.lock";
  public static final String ALTER_PARTITION_LOCK = "alter_partition.lock";
  public static final String TABLE_STATUS_LOCK = "tablestatus.lock";
  public static final String TABLE_UPDATE_STATUS_LOCK = "tableupdatestatus.lock";
  public static final String DELETE_SEGMENT_LOCK = "delete_segment.lock";
  public static final String CLEAN_FILES_LOCK = "clean_files.lock";
  public static final String DROP_TABLE_LOCK = "droptable.lock";
  public static final String STREAMING_LOCK = "streaming.lock";
}
