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

package org.apache.carbondata.processing.loading.constants;

/**
 * Constants used in data loading.
 */
public final class DataLoadProcessorConstants {

  public static final String FACT_TIME_STAMP = "FACT_TIME_STAMP";

  public static final String COMPLEX_DELIMITERS = "COMPLEX_DELIMITERS";

  public static final String SERIALIZATION_NULL_FORMAT = "SERIALIZATION_NULL_FORMAT";

  public static final String BAD_RECORDS_LOGGER_ENABLE = "BAD_RECORDS_LOGGER_ENABLE";

  public static final String BAD_RECORDS_LOGGER_ACTION = "BAD_RECORDS_LOGGER_ACTION";

  public static final String IS_EMPTY_DATA_BAD_RECORD = "IS_EMPTY_DATA_BAD_RECORD";

  public static final String SKIP_EMPTY_LINE = "SKIP_EMPTY_LINE";

  public static final String FACT_FILE_PATH = "FACT_FILE_PATH";

  // to indicate that it is optimized insert flow without rearrange of each data rows
  public static final String NO_REARRANGE_OF_ROWS = "NO_REARRANGE_OF_ROWS";
}
