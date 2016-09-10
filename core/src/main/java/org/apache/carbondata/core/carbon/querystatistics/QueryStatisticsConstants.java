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

package org.apache.carbondata.core.carbon.querystatistics;

public interface QueryStatisticsConstants {

  // driver side
  String SQL_PARSE = "Time taken to parse sql In Driver Side";

  String LOAD_META = "Time taken to load meta data In Driver Side";

  String LOAD_BLOCKS_DRIVER = "Time taken to load the Block(s) In Driver Side";

  String BLOCK_ALLOCATION = "Total Time taken in block(s) allocation";

  String BLOCK_IDENTIFICATION = "Time taken to identify Block(s) to scan";

  // executor side
  String EXECUTOR_PART =
      "Total Time taken to execute the query in executor Side";

  String LOAD_BLOCKS_EXECUTOR = "Time taken to load the Block(s) In Executor";

  String SCAN_BLOCKS_NUM = "The num of blocks scanned";

  String SCAN_BLOCKS_TIME = "Time taken to scan blocks";

  String LOAD_DICTIONARY = "Time taken to load the Dictionary In Executor";

  String PREPARE_RESULT = "Total Time taken to prepare query result";

  String RESULT_SIZE = "The size of query result";

  // clear no-use statistics timeout
  long CLEAR_STATISTICS_TIMEOUT = 60 * 1000 * 1000000L;

}


