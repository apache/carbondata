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

  /**
   * enable block size based block allocation while loading data. By default, carbondata assigns
   * blocks to node based on block number. If this option is set to `true`, carbondata will
   * consider block size first and make sure that all the nodes will process almost equal size of
   * data. This option is especially useful when you encounter skewed data.
   */
  @CarbonProperty
  public static final String ENABLE_CARBON_LOAD_SKEWED_DATA_OPTIMIZATION
      = "carbon.load.skewedDataOptimization.enabled";
  public static final String ENABLE_CARBON_LOAD_SKEWED_DATA_OPTIMIZATION_DEFAULT = "false";

  /**
   * field delimiter for each field in one bound
   */
  public static final String SORT_COLUMN_BOUNDS_FIELD_DELIMITER = ",";

  /**
   * row delimiter for each sort column bounds
   */
  public static final String SORT_COLUMN_BOUNDS_ROW_DELIMITER = ";";

  @CarbonProperty
  public static final String ENABLE_CARBON_LOAD_DIRECT_WRITE_HDFS
      = "carbon.load.directWriteHdfs.enabled";
  public static final String ENABLE_CARBON_LOAD_DIRECT_WRITE_HDFS_DEFAULT = "false";

  /**
   * If the sort memory is insufficient, spill inmemory pages to disk.
   * The total amount of pages is at most the specified percentage of total sort memory. Default
   * value 0 means that no pages will be spilled and the newly incoming pages will be spilled,
   * whereas value 100 means that all pages will be spilled and newly incoming pages will be loaded
   * into sort memory, valid value is from 0 to 100.
   */
  @CarbonProperty
  public static final String CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE
      = "carbon.load.sortmemory.spill.percentage";
  public static final String CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE_DEFAULT = "0";

  /**
   *  if loading data is too small, the original loading method will produce many small files.
   *  enable set the node load minimum amount of data,avoid producing many small files.
   *  This option is especially useful when you encounter a lot of small amounts of data.
   */
  @CarbonProperty
  public static final String ENABLE_CARBON_LOAD_NODE_DATA_MIN_SIZE
      = "carbon.load.min.size.enabled";
  public static final String ENABLE_CARBON_LOAD_NODE_DATA_MIN_SIZE_DEFAULT = "false";
}
