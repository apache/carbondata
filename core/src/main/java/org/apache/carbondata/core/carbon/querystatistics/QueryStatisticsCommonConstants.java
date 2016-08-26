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

public final class QueryStatisticsCommonConstants {

  public static final String JDBC_CONNECTION = "Time taken to connect JDBC";

  public static final String SQL_PARSE = "Time taken to parse sql In Driver Side";

  public static final String LOAD_META = "Time taken to load meta data In Driver Side";

  public static final String BLOCK_IDENTIFICATION = "Time taken to identify Block(s) to scan";

  public static final String SCHEDULE_TIME = "Time taken to schedule task to executor";

  public static final String DRIVER_PART = "Total time taken in driver part";

  public static final String EXECUTOR_PART =
      "Total Time taken to execute the query in executor Side";

  public static final String LOAD_INDEX = "Time taken to load the Block(s) In Executor";

  public static final String SCAN_DATA =
      "Total Time taken to Scan(read data from file and process)";

  public static final String DICTIONARY_LOAD = "Time taken to load the Dictionary In Executor";

  public static final String PREPARE_RESULT = "Total Time taken to prepare query result";

  public static final String PRINT_RESULT = "Time taken to print result at beeline";

  public static final String TOTAL_TIME = "Total taken to query";

  private QueryStatisticsCommonConstants() {
  }
}


