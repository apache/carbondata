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

package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.CarbonProperties

/**
 * To initialize dynamic values default param
 */
class CarbonSQLConf(sparkSession: SparkSession) {

  val carbonProperties = CarbonProperties.getInstance()

  /**
   * To initialize dynamic param defaults along with usage docs
   */
  def addDefaultCarbonParams(): Unit = {
    val ENABLE_UNSAFE_SORT =
        SQLConfigBuilder(CarbonCommonConstants.ENABLE_UNSAFE_SORT)
        .doc("To enable/ disable unsafe sort.")
        .booleanConf
        .createWithDefault(carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
          CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT).toBoolean)
    val CARBON_CUSTOM_BLOCK_DISTRIBUTION =
      SQLConfigBuilder(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION)
        .doc("To set carbon task distribution.")
        .stringConf
        .createWithDefault(carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
            CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT))
    val BAD_RECORDS_LOGGER_ENABLE =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE)
        .doc("To enable/ disable carbon bad record logger.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants
          .CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT.toBoolean)
    val BAD_RECORDS_ACTION =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION)
        .doc("To configure the bad records action.")
        .stringConf
        .createWithDefault(carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
            CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT))
    val IS_EMPTY_DATA_BAD_RECORD =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD)
        .doc("Property to decide weather empty data to be considered bad/ good record.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT
          .toBoolean)
    val SORT_SCOPE =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE)
        .doc("Property to specify sort scope.")
        .stringConf
        .createWithDefault(carbonProperties.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    val BATCH_SORT_SIZE_INMB =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB)
        .doc("Property to specify batch sort size in MB.")
        .stringConf
        .createWithDefault(carbonProperties
          .getProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
            CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))
    val SINGLE_PASS =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS)
        .doc("Property to enable/disable single_pass.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT.toBoolean)
    val BAD_RECORD_PATH =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH)
        .doc("Property to configure the bad record location.")
        .stringConf
        .createWithDefault(carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))
    val GLOBAL_SORT_PARTITIONS =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS)
        .doc("Property to configure the global sort partitions.")
        .stringConf
        .createWithDefault(carbonProperties
          .getProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
            CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT))
    val DATEFORMAT =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT)
        .doc("Property to configure data format for date type columns.")
        .stringConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)
    val CARBON_INPUT_SEGMENTS = SQLConfigBuilder(
      "carbon.input.segments.<database_name>.<table_name>")
      .doc("Property to configure the list of segments to query.").stringConf
      .createWithDefault(carbonProperties
        .getProperty("carbon.input.segments.<database_name>.<table_name>", "*"))
  }
  /**
   * to set the dynamic properties default values
   */
  def addDefaultCarbonSessionParams(): Unit = {
    sparkSession.conf.set(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
        CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT).toBoolean)
    sparkSession.conf.set(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION,
      carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
          CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
      CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT.toBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
      carbonProperties.getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
      CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT.toBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
      carbonProperties.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
      carbonProperties.getProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
        CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
      CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT.toBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
      carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
      carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS,
      carbonProperties.getProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
        CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
      CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)
  }
}
