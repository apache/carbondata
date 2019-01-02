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
package org.apache.carbondata.core.indexstore.blockletindex;

/**
 * holder for blocklet info indexes in a DataMap row
 */
public interface BlockletDataMapRowIndexes {

  // Each DataMapRow Indexes for blocklet and block dataMap
  int MIN_VALUES_INDEX = 0;

  int MAX_VALUES_INDEX = 1;

  int ROW_COUNT_INDEX = 2;

  int FILE_PATH_INDEX = 3;

  int VERSION_INDEX = 4;

  int SCHEMA_UPADATED_TIME_INDEX = 5;

  int BLOCK_FOOTER_OFFSET = 6;

  int LOCATIONS = 7;

  int BLOCK_LENGTH = 8;

  int BLOCK_MIN_MAX_FLAG = 9;

  // below variables are specific for blockletDataMap
  int BLOCKLET_INFO_INDEX = 10;

  int BLOCKLET_PAGE_COUNT_INDEX = 11;

  int BLOCKLET_ID_INDEX = 12;

  // Summary dataMap row indexes
  int TASK_MIN_VALUES_INDEX = 0;

  int TASK_MAX_VALUES_INDEX = 1;

  int SUMMARY_INDEX_FILE_NAME = 2;

  int SUMMARY_SEGMENTID = 3;

  int TASK_MIN_MAX_FLAG = 4;

  int SUMMARY_INDEX_PATH = 5;
}
