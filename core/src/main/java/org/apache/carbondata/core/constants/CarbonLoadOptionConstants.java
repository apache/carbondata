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

package org.apache.carbondata.core.constants;

import org.apache.carbondata.core.util.CarbonProperty;

/**
 * Load options constant
 */
public final class CarbonLoadOptionConstants {
  /**
   * option to enable and disable the logger
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE =
      "carbon.options.bad.records.logger.enable";

  public static String CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT = "false";
  /**
   * property to pass the bad records action
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_BAD_RECORDS_ACTION =
      "carbon.options.bad.records.action";
  /**
   * load option to specify weather empty data to be treated as bad record
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD =
      "carbon.options.is.empty.data.bad.record";
  public static final String CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT = "false";

  /**
   * option to specify whether to skip empty lines in load
   */
  @CarbonProperty public static final String CARBON_OPTIONS_SKIP_EMPTY_LINE =
      "carbon.options.is.empty.data.bad.record";

  /**
   * option to specify the dateFormat in load option for all date columns in table
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_DATEFORMAT =
      "carbon.options.dateformat";
  public static final String CARBON_OPTIONS_DATEFORMAT_DEFAULT = "";

  /**
   * option to specify the timestampFormat in load option for all timestamp columns in table
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_TIMESTAMPFORMAT =
          "carbon.options.timestampformat";
  public static final String CARBON_OPTIONS_TIMESTAMPFORMAT_DEFAULT = "";
  /**
   * option to specify the sort_scope
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_SORT_SCOPE =
      "carbon.options.sort.scope";
  /**
   * option to specify the batch sort size inmb
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_BATCH_SORT_SIZE_INMB =
      "carbon.options.batch.sort.size.inmb";
  /**
   * Option to enable/ disable single_pass
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_SINGLE_PASS =
      "carbon.options.single.pass";
  public static final String CARBON_OPTIONS_SINGLE_PASS_DEFAULT = "false";

  /**
   * specify bad record path option
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_BAD_RECORD_PATH =
      "carbon.options.bad.record.path";
  /**
   * specify bad record path option
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS =
      "carbon.options.global.sort.partitions";

  /**
   * specify serialization null format, it is used describe which character in side the csv file
   * is treated as null.
   */
  @CarbonProperty
  public static final String CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT =
      "carbon.options.serialization.null.format";

  public static final String CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT_DEFAULT = "\\N";

  /**
   *  Max number of dictionary values that can be given with external dictionary
   */
  public static final int MAX_EXTERNAL_DICTIONARY_SIZE = 10000000;

}
