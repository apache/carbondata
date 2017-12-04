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

package org.apache.carbondata.spark.util

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, MalformedCarbonCommandException}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.TableOptionConstant
import org.apache.carbondata.spark.load.ValidateUtil

/**
 * the util object of data loading
 */
object DataLoadingUtil {

  /**
   * get data loading options and initialise default value
   */
  def getDataLoadingOptions(
      carbonProperty: CarbonProperties,
      options: immutable.Map[String, String]): mutable.Map[String, String] = {
    val optionsFinal = scala.collection.mutable.Map[String, String]()
    optionsFinal.put("delimiter", options.getOrElse("delimiter", ","))
    optionsFinal.put("quotechar", options.getOrElse("quotechar", "\""))
    optionsFinal.put("fileheader", options.getOrElse("fileheader", ""))
    optionsFinal.put("escapechar", options.getOrElse("escapechar", "\\"))
    optionsFinal.put("commentchar", options.getOrElse("commentchar", "#"))
    optionsFinal.put("columndict", options.getOrElse("columndict", null))

    optionsFinal.put(
      "serialization_null_format",
      options.getOrElse("serialization_null_format", "\\N"))

    optionsFinal.put(
      "bad_records_logger_enable",
      options.getOrElse(
        "bad_records_logger_enable",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT)))

    val badRecordActionValue = carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)

    optionsFinal.put(
      "bad_records_action",
      options.getOrElse(
        "bad_records_action",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
          badRecordActionValue)))

    optionsFinal.put(
      "is_empty_data_bad_record",
      options.getOrElse(
        "is_empty_data_bad_record",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
          CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT)))

    optionsFinal.put(
      "skip_empty_line",
      options.getOrElse(
        "skip_empty_line",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_SKIP_EMPTY_LINE)))

    optionsFinal.put("all_dictionary_path", options.getOrElse("all_dictionary_path", ""))

    optionsFinal.put(
      "complex_delimiter_level_1",
      options.getOrElse("complex_delimiter_level_1", "\\$"))

    optionsFinal.put(
      "complex_delimiter_level_2",
      options.getOrElse("complex_delimiter_level_2", "\\:"))

    optionsFinal.put(
      "dateformat",
      options.getOrElse(
        "dateformat",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
          CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)))

    optionsFinal.put(
      "timestampformat",
      options.getOrElse(
        "timestampformat",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT,
          CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT_DEFAULT)))

    optionsFinal.put(
      "global_sort_partitions",
      options.getOrElse(
        "global_sort_partitions",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS,
          null)))

    optionsFinal.put("maxcolumns", options.getOrElse("maxcolumns", null))

    optionsFinal.put(
      "batch_sort_size_inmb",
      options.getOrElse(
        "batch_sort_size_inmb",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
          carbonProperty.getProperty(
            CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
            CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))))

    optionsFinal.put(
      "bad_record_path",
      options.getOrElse(
        "bad_record_path",
        carbonProperty.getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
          carbonProperty.getProperty(
            CarbonCommonConstants.CARBON_BADRECORDS_LOC,
            CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))))

    val useOnePass = options.getOrElse(
      "single_pass",
      carbonProperty.getProperty(
        CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
        CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT)).trim.toLowerCase match {
      case "true" =>
        true
      case "false" =>
        // when single_pass = false  and if either alldictionarypath
        // or columnDict is configured the do not allow load
        if (StringUtils.isNotEmpty(optionsFinal("all_dictionary_path")) ||
            StringUtils.isNotEmpty(optionsFinal("columndict"))) {
          throw new MalformedCarbonCommandException(
            "Can not use all_dictionary_path or columndict without single_pass.")
        } else {
          false
        }
      case illegal =>
        val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
        LOGGER.error(s"Can't use single_pass, because illegal syntax found: [$illegal] " +
                     "Please set it as 'true' or 'false'")
        false
    }
    optionsFinal.put("single_pass", useOnePass.toString)
    optionsFinal
  }

  /**
   * check whether using default value or not
   */
  private def checkDefaultValue(value: String, default: String) = {
    if (StringUtils.isEmpty(value)) {
      default
    } else {
      value
    }
  }

  /**
   * build CarbonLoadModel for data loading
   */
  def buildCarbonLoadModel(
      table: CarbonTable,
      carbonProperty: CarbonProperties,
      options: immutable.Map[String, String],
      optionsFinal: mutable.Map[String, String],
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration): Unit = {
    carbonLoadModel.setTableName(table.getTableName)
    carbonLoadModel.setDatabaseName(table.getDatabaseName)
    carbonLoadModel.setTablePath(table.getTablePath)
    carbonLoadModel.setTableName(table.getTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    val sort_scope = optionsFinal("sort_scope")
    val single_pass = optionsFinal("single_pass")
    val bad_records_logger_enable = optionsFinal("bad_records_logger_enable")
    val bad_records_action = optionsFinal("bad_records_action")
    val bad_record_path = optionsFinal("bad_record_path")
    val global_sort_partitions = optionsFinal("global_sort_partitions")
    val timestampformat = optionsFinal("timestampformat")
    val dateFormat = optionsFinal("dateformat")
    val delimeter = optionsFinal("delimiter")
    val complex_delimeter_level1 = optionsFinal("complex_delimiter_level_1")
    val complex_delimeter_level2 = optionsFinal("complex_delimiter_level_2")
    val all_dictionary_path = optionsFinal("all_dictionary_path")
    val column_dict = optionsFinal("columndict")
    ValidateUtil.validateDateTimeFormat(timestampformat, "TimestampFormat")
    ValidateUtil.validateDateTimeFormat(dateFormat, "DateFormat")
    ValidateUtil.validateSortScope(table, sort_scope)

    if (bad_records_logger_enable.toBoolean ||
        LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
      if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
        CarbonException.analysisException("Invalid bad records location.")
      }
    }
    carbonLoadModel.setBadRecordsLocation(bad_record_path)

    ValidateUtil.validateGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal("escapechar"), "\\"))
    carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal("quotechar"), "\""))
    carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal("commentchar"), "#"))

    // if there isn't file header in csv file and load sql doesn't provide FILEHEADER option,
    // we should use table schema to generate file header.
    var fileHeader = optionsFinal("fileheader")
    val headerOption = options.get("header")
    if (headerOption.isDefined) {
      // whether the csv file has file header
      // the default value is true
      val header = try {
        headerOption.get.toBoolean
      } catch {
        case ex: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "'header' option should be either 'true' or 'false'. " + ex.getMessage)
      }
      if (header) {
        if (fileHeader.nonEmpty) {
          throw new MalformedCarbonCommandException(
            "When 'header' option is true, 'fileheader' option is not required.")
        }
      } else {
        if (fileHeader.isEmpty) {
          fileHeader = table.getCreateOrderColumn(table.getTableName)
            .asScala.map(_.getColName).mkString(",")
        }
      }
    }

    carbonLoadModel.setTimestampformat(timestampformat)
    carbonLoadModel.setDateFormat(dateFormat)
    carbonLoadModel.setDefaultTimestampFormat(carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))

    carbonLoadModel.setDefaultDateFormat(carbonProperty.getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))

    carbonLoadModel.setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," +
        optionsFinal("serialization_null_format"))

    carbonLoadModel.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + bad_records_logger_enable)

    carbonLoadModel.setBadRecordsAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + bad_records_action)

    carbonLoadModel.setIsEmptyDataBadRecord(
        DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," +
        optionsFinal("is_empty_data_bad_record"))

    carbonLoadModel.setSkipEmptyLine(optionsFinal("skip_empty_line"))

    carbonLoadModel.setSortScope(sort_scope)
    carbonLoadModel.setBatchSortSizeInMb(optionsFinal("batch_sort_size_inmb"))
    carbonLoadModel.setGlobalSortPartitions(global_sort_partitions)
    carbonLoadModel.setUseOnePass(single_pass.toBoolean)

    if (delimeter.equalsIgnoreCase(complex_delimeter_level1) ||
        complex_delimeter_level1.equalsIgnoreCase(complex_delimeter_level2) ||
        delimeter.equalsIgnoreCase(complex_delimeter_level2)) {
      CarbonException.analysisException(s"Field Delimiter and Complex types delimiter are same")
    } else {
      carbonLoadModel.setComplexDelimiterLevel1(
        CarbonUtil.delimiterConverter(complex_delimeter_level1))
      carbonLoadModel.setComplexDelimiterLevel2(
        CarbonUtil.delimiterConverter(complex_delimeter_level2))
    }
    // set local dictionary path, and dictionary file extension
    carbonLoadModel.setAllDictPath(all_dictionary_path)
    carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimeter))
    carbonLoadModel.setCsvHeader(fileHeader)
    carbonLoadModel.setColDictFilePath(column_dict)
    carbonLoadModel.setCsvHeaderColumns(
      CommonUtil.getCsvHeaderColumns(carbonLoadModel, hadoopConf))

    val validatedMaxColumns = CommonUtil.validateMaxColumns(
      carbonLoadModel.getCsvHeaderColumns,
      optionsFinal("maxcolumns"))

    carbonLoadModel.setMaxColumns(validatedMaxColumns.toString)
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }
  }
}
