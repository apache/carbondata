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

package org.apache.carbondata.core.stats;

public interface QueryStatisticsConstants {

  // driver side
  String SQL_PARSE = "Time taken to parse sql In Driver Side";

  String LOAD_META = "Time taken to load meta data In Driver Side";

  String LOAD_BLOCKS_DRIVER = "Time taken to load the Block(s) In Driver Side "
      + "with Block count ";

  String BLOCK_ALLOCATION = "Total Time taken in block(s) allocation";

  String BLOCK_IDENTIFICATION = "Time taken to identify Block(s) to scan";

  // executor side
  String EXECUTOR_PART = "Total Time taken to execute the query in executor Side";

  String LOAD_BLOCKS_EXECUTOR = "Time taken to load the Block(s) In Executor";

  String SCAN_BLOCKS_NUM = "The num of blocks scanned";

  String SCAN_BLOCKlET_TIME = "Time taken to scan blocks";

  String READ_BLOCKlET_TIME = "Time taken to read blocks";

  String LOAD_DICTIONARY = "Time taken to load the Dictionary In Executor";

  String PREPARE_RESULT = "Total Time taken to prepare query result";

  String RESULT_SIZE = "The size of query result";

  String TOTAL_BLOCKLET_NUM = "The num of total blocklet";

  String VALID_SCAN_BLOCKLET_NUM = "The num of valid scanned blocklet";

  String BLOCKLET_SCANNED_NUM = "The num of blocklet scanned";

  String VALID_PAGE_SCANNED = "The number of valid page scanned";

  String TOTAL_PAGE_SCANNED = "The number of total page scanned";

  String PAGE_SCANNED = "The number of page scanned";

  /**
   * measure filling time includes time taken for reading all measures data from a given offset
   * and adding each column data to an array. Includes total time for 1 query result iterator.
   */
  String MEASURE_FILLING_TIME = "measure filling time";

  /**
   * key column filling time includes time taken for reading all dimensions data from a given offset
   * and filling each column data to byte array. Includes total time for 1 query result iterator.
   */
  String KEY_COLUMN_FILLING_TIME = "key column filling time";

  /**
   * Time taken to uncompress a page data and decode dimensions and measures data in that page
   */
  String PAGE_UNCOMPRESS_TIME = "page uncompress time";

  /**
   * total of measure filling time, dimension filling time and page uncompressing time
   */
  String RESULT_PREP_TIME = "result preparation time";

  // clear no-use statistics timeout
  long CLEAR_STATISTICS_TIMEOUT = 60 * 1000 * 1000000L;

}


