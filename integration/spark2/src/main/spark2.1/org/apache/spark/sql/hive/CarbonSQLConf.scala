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
        SQLConfigBuilder(CarbonCommonConstants.ENABLE_UNSAFE_SORT.getName)
        .doc("To enable/ disable unsafe sort.")
        .booleanConf
        .createWithDefault(carbonProperties.getPropertyOrDefault(
          CarbonCommonConstants.ENABLE_UNSAFE_SORT).toBoolean)
    val CARBON_CUSTOM_BLOCK_DISTRIBUTION =
      SQLConfigBuilder(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION.getName)
        .doc("To set carbon task distribution.")
        .stringConf
        .createWithDefault(carbonProperties
          .getPropertyOrDefault(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION))
    val BAD_RECORDS_LOGGER_ENABLE =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE.getName)
        .doc("To enable/ disable carbon bad record logger.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants
          .CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE.getDefaultValueBoolean)
    val BAD_RECORDS_ACTION =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION.getName)
        .doc("To configure the bad records action.")
        .stringConf
        .createWithDefault(carbonProperties
          .getPropertyOrDefault(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION))
    val IS_EMPTY_DATA_BAD_RECORD =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD.getName)
        .doc("Property to decide weather empty data to be considered bad/ good record.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD
          .getDefaultValueString
          .toBoolean)
    val SORT_SCOPE =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE.getName)
        .doc("Property to specify sort scope.")
        .stringConf
        .createWithDefault(carbonProperties.getPropertyOrDefault(
          CarbonCommonConstants.LOAD_SORT_SCOPE))
    val BATCH_SORT_SIZE_INMB =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB.getName)
        .doc("Property to specify batch sort size in MB.")
        .stringConf
        .createWithDefault(carbonProperties
          .getPropertyOrDefault(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB))
    val SINGLE_PASS =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS.getName)
        .doc("Property to enable/disable single_pass.")
        .booleanConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS
          .getDefaultValueBoolean)
    val BAD_RECORD_PATH =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH.getName)
        .doc("Property to configure the bad record location.")
        .stringConf
        .createWithDefault(carbonProperties.getPropertyOrDefault(CarbonCommonConstants.CARBON_BADRECORDS_LOC))
    val GLOBAL_SORT_PARTITIONS =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS.getName)
        .doc("Property to configure the global sort partitions.")
        .stringConf
        .createWithDefault(carbonProperties
          .getPropertyOrDefault(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    val DATEFORMAT =
      SQLConfigBuilder(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT.getName)
        .doc("Property to configure data format for date type columns.")
        .stringConf
        .createWithDefault(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT.getDefaultValueString)
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
    sparkSession.conf.set(CarbonCommonConstants.ENABLE_UNSAFE_SORT.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.ENABLE_UNSAFE_SORT).toBoolean)
    sparkSession.conf.set(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION.getName,
      carbonProperties
        .getPropertyOrDefault(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE.getName,
      CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE.getDefaultValueBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD.getName,
      CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD.getDefaultValueBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.LOAD_SORT_SCOPE))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS.getName,
      CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS.getDefaultValueBoolean)
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.CARBON_BADRECORDS_LOC))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.CARBON_BADRECORDS_LOC))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS.getName,
      carbonProperties.getPropertyOrDefault(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    sparkSession.conf.set(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT.getName,
      CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT.getDefaultValueString)
  }
}
