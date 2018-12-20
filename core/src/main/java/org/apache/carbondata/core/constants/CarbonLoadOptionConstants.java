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

import org.apache.carbondata.core.util.Property;

/**
 * Load options constant
 */
public final class CarbonLoadOptionConstants {

  public static final Property CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE =
      Property.buildBooleanProperty()
          .key("carbon.options.bad.records.logger.enable")
          .defaultValue(false)
          .dynamicConfigurable(true)
          .doc("option to enable and disable the logger")
          .build();

  public static final Property CARBON_OPTIONS_BAD_RECORDS_ACTION =
      Property.buildStringProperty()
          .key("carbon.options.bad.records.action")
          .defaultValue("FAIL")
          .dynamicConfigurable(true)
          .doc("property to pass the bad records action")
          .build();

  public static final Property CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD =
      Property.buildBooleanProperty()
          .key("carbon.options.is.empty.data.bad.record")
          .defaultValue(false)
          .dynamicConfigurable(true)
          .doc("load option to specify weather empty data to be treated as bad record")
          .build();

  public static final Property CARBON_OPTIONS_SKIP_EMPTY_LINE =
      Property.buildBooleanProperty()
          .key("carbon.options.is.empty.data.bad.record")
          .defaultValue(false)
          .dynamicConfigurable(true)
          .doc("option to specify whether to skip empty lines in load")
          .build();

  public static final Property CARBON_OPTIONS_DATEFORMAT =
      Property.buildStringProperty()
          .key("carbon.options.date.format")
          .defaultValue("")
          .dynamicConfigurable(true)
          .doc("option to specify the dateFormat in load option for all date columns in table")
          .build();

  public static final Property CARBON_OPTIONS_TIMESTAMPFORMAT =
      Property.buildStringProperty()
          .key("carbon.options.timestamp.format")
          .defaultValue("")
          .dynamicConfigurable(true)
          .doc("option to specify the timestampFormat in load option for " +
              "all timestamp columns in table")
          .build();

  public static final Property CARBON_OPTIONS_SORT_SCOPE =
      Property.buildStringProperty()
          .key("carbon.options.sort.scope")
          .defaultValue("LOCAL_SORT")
          .dynamicConfigurable(true)
          .doc("option to specify the sort_scope")
          .build();

  public static final Property CARBON_OPTIONS_BATCH_SORT_SIZE_INMB =
      Property.buildIntProperty()
          .key("carbon.options.batch.sort.size.inmb")
          .defaultValue(0)
          .dynamicConfigurable(true)
          .doc("option to specify the batch sort size inmb")
          .build();

  public static final Property CARBON_OPTIONS_SINGLE_PASS =
      Property.buildBooleanProperty()
          .key("carbon.options.single.pass")
          .defaultValue(false)
          .dynamicConfigurable(true)
          .doc("Option to enable/ disable single_pass")
          .build();

  public static final Property CARBON_OPTIONS_BAD_RECORD_PATH =
      Property.buildStringProperty()
          .key("carbon.options.bad.record.path")
          .defaultValue("")
          .dynamicConfigurable(true)
          .doc("specify bad record path option")
          .build();

  public static final Property CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS =
      Property.buildIntProperty()
          .key("carbon.options.global.sort.partitions")
          .defaultValue(0)
          .dynamicConfigurable(true)
          .doc("specify bad record path option")
          .build();

  public static final Property CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT =
      Property.buildStringProperty()
          .key("carbon.options.serialization.null.format")
          .defaultValue("\\N")
          .dynamicConfigurable(true)
          .doc("specify serialization null format, it is used describe which character in " +
              "side the csv file is treated as null.")
          .build();

  /**
   *  Max number of dictionary values that can be given with external dictionary
   */
  public static final int MAX_EXTERNAL_DICTIONARY_SIZE = 10000000;

  public static final Property ENABLE_CARBON_LOAD_SKEWED_DATA_OPTIMIZATION =
      Property.buildBooleanProperty()
          .key("carbon.load.skewedDataOptimization.enabled")
          .defaultValue(false)
          .doc("enable block size based block allocation while loading data. By default, " +
              " carbondata assigns blocks to node based on block number. If this option is " +
              " set to `true`, carbondata will consider block size first and make sure that " +
              " all the nodes will process almost equal size of data. " +
              " This option is especially useful when you encounter skewed data.")
          .build();

  /**
   * field delimiter for each field in one bound
   */
  public static final String SORT_COLUMN_BOUNDS_FIELD_DELIMITER = ",";

  /**
   * row delimiter for each sort column bounds
   */
  public static final String SORT_COLUMN_BOUNDS_ROW_DELIMITER = ";";

  public static final Property ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH =
      Property.buildBooleanProperty()
          .key("carbon.load.directWriteToStorePath.enabled")
          .defaultValue(false)
          .doc("")
          .build();

  public static final Property CARBON_LOAD_SORT_MEMORY_SPILL_PERCENTAGE =
      Property.buildIntProperty()
          .key("carbon.load.sortmemory.spill.percentage")
          .defaultValue(0)
          .doc("If the sort memory is insufficient, spill inmemory pages to disk." +
              " The total amount of pages is at most the specified percentage of total " +
              " sort memory. " +
              " Default value 0 means that no pages will be spilled and the newly incoming " +
              " pages will be spilled, whereas value 100 means that all pages will be " +
              " spilled and newly incoming pages will be loaded into sort memory, " +
              "valid value is from 0 to 100.")
          .build();

}
