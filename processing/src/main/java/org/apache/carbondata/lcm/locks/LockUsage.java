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
package org.apache.carbondata.lcm.locks;

/**
 * This enum is used to define the usecase of the lock.
 * Each enum value is one specific lock case.
 */
public class LockUsage {
  public static String LOCK = ".lock";
  public static String METADATA_LOCK = "meta.lock";
  public static String COMPACTION_LOCK = "compaction.lock";
  public static String SYSTEMLEVEL_COMPACTION_LOCK = "system_level_compaction.lock";
  public static String TABLE_STATUS_LOCK = "tablestatus.lock";
  public static String DELETE_SEGMENT_LOCK = "delete_segment.lock";
  public static String CLEAN_FILES_LOCK = "clean_files.lock";

}
