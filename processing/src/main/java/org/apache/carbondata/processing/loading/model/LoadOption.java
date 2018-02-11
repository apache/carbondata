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

package org.apache.carbondata.processing.loading.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.Maps;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Provide utilities to populate loading options
 */
@InterfaceAudience.Developer
public class LoadOption {

  private static LogService LOG = LogServiceFactory.getLogService(LoadOption.class.getName());

  /**
   * Based on the input options, fill and return data loading options with default value
   */
  public static Map<String, String> fillOptionWithDefaultValue(
      Map<String, String> options) throws InvalidLoadOptionException {
    Map<String, String> optionsFinal = new HashMap<>();
    optionsFinal.put("delimiter", Maps.getOrDefault(options, "delimiter", ","));
    optionsFinal.put("quotechar", Maps.getOrDefault(options, "quotechar", "\""));
    optionsFinal.put("fileheader", Maps.getOrDefault(options, "fileheader", ""));
    optionsFinal.put("commentchar", Maps.getOrDefault(options, "commentchar", "#"));
    optionsFinal.put("columndict", Maps.getOrDefault(options, "columndict", null));

    optionsFinal.put(
        "escapechar",
        CarbonLoaderUtil.getEscapeChar(Maps.getOrDefault(options,"escapechar", "\\")));

    optionsFinal.put(
        "serialization_null_format",
        Maps.getOrDefault(options, "serialization_null_format", "\\N"));

    optionsFinal.put(
        "bad_records_logger_enable",
        Maps.getOrDefault(
            options,
            "bad_records_logger_enable",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT)));

    String badRecordActionValue = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);

    optionsFinal.put(
        "bad_records_action",
        Maps.getOrDefault(
            options,
            "bad_records_action",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
                badRecordActionValue)));

    optionsFinal.put(
        "is_empty_data_bad_record",
        Maps.getOrDefault(
            options,
            "is_empty_data_bad_record",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT)));

    optionsFinal.put(
        "skip_empty_line",
        Maps.getOrDefault(
            options,
            "skip_empty_line",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_SKIP_EMPTY_LINE)));

    optionsFinal.put(
        "all_dictionary_path",
        Maps.getOrDefault(options, "all_dictionary_path", ""));

    optionsFinal.put(
        "complex_delimiter_level_1",
        Maps.getOrDefault(options,"complex_delimiter_level_1", "\\$"));

    optionsFinal.put(
        "complex_delimiter_level_2",
        Maps.getOrDefault(options, "complex_delimiter_level_2", "\\:"));

    optionsFinal.put(
        "dateformat",
        Maps.getOrDefault(
            options,
            "dateformat",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)));

    optionsFinal.put(
        "timestampformat",
        Maps.getOrDefault(
            options,
            "timestampformat",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT_DEFAULT)));

    optionsFinal.put(
        "global_sort_partitions",
        Maps.getOrDefault(
            options,
            "global_sort_partitions",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS,
                null)));

    optionsFinal.put("maxcolumns", Maps.getOrDefault(options, "maxcolumns", null));

    optionsFinal.put(
        "batch_sort_size_inmb",
        Maps.getOrDefault(
            options,
            "batch_sort_size_inmb",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
                CarbonProperties.getInstance().getProperty(
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))));

    optionsFinal.put(
        "bad_record_path",
        Maps.getOrDefault(
            options,
            "bad_record_path",
            CarbonProperties.getInstance().getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
                CarbonProperties.getInstance().getProperty(
                    CarbonCommonConstants.CARBON_BADRECORDS_LOC,
                    CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))));

    String useOnePass = Maps.getOrDefault(
        options,
        "single_pass",
        CarbonProperties.getInstance().getProperty(
            CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
            CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT)).trim().toLowerCase();

    boolean singlePass;

    if (useOnePass.equalsIgnoreCase("true")) {
      singlePass = true;
    } else {
      // when single_pass = false  and if either alldictionarypath
      // or columnDict is configured the do not allow load
      if (StringUtils.isNotEmpty(optionsFinal.get("all_dictionary_path")) ||
          StringUtils.isNotEmpty(optionsFinal.get("columndict"))) {
        throw new InvalidLoadOptionException(
            "Can not use all_dictionary_path or columndict without single_pass.");
      } else {
        singlePass = false;
      }
    }

    optionsFinal.put("single_pass", String.valueOf(singlePass));
    return optionsFinal;
  }

  /**
   * Return CSV header field names
   */
  public static String[] getCsvHeaderColumns(
      CarbonLoadModel carbonLoadModel,
      Configuration hadoopConf) throws IOException {
    String delimiter;
    if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter())) {
      delimiter = CarbonCommonConstants.COMMA;
    } else {
      delimiter = CarbonUtil.delimiterConverter(carbonLoadModel.getCsvDelimiter());
    }
    String csvFile = null;
    String csvHeader = carbonLoadModel.getCsvHeader();
    String[] csvColumns;
    if (StringUtils.isBlank(csvHeader)) {
      // read header from csv file
      csvFile = carbonLoadModel.getFactFilePath().split(",")[0];
      csvHeader = CarbonUtil.readHeader(csvFile, hadoopConf);
      if (StringUtils.isBlank(csvHeader)) {
        throw new CarbonDataLoadingException("First line of the csv is not valid.");
      }
      String[] headers = csvHeader.toLowerCase().split(delimiter);
      csvColumns = new String[headers.length];
      for (int i = 0; i < csvColumns.length; i++) {
        csvColumns[i] = headers[i].replaceAll("\"", "").trim();
      }
    } else {
      String[] headers = csvHeader.toLowerCase().split(CarbonCommonConstants.COMMA);
      csvColumns = new String[headers.length];
      for (int i = 0; i < csvColumns.length; i++) {
        csvColumns[i] = headers[i].trim();
      }
    }

    if (!CarbonDataProcessorUtil.isHeaderValid(carbonLoadModel.getTableName(), csvColumns,
        carbonLoadModel.getCarbonDataLoadSchema())) {
      if (csvFile == null) {
        LOG.error("CSV header in DDL is not proper."
            + " Column names in schema and CSV header are not the same.");
        throw new CarbonDataLoadingException(
            "CSV header in DDL is not proper. Column names in schema and CSV header are "
                + "not the same.");
      } else {
        LOG.error(
            "CSV header in input file is not proper. Column names in schema and csv header are not "
                + "the same. Input file : " + CarbonUtil.removeAKSK(csvFile));
        throw new CarbonDataLoadingException(
            "CSV header in input file is not proper. Column names in schema and csv header are not "
                + "the same. Input file : " + CarbonUtil.removeAKSK(csvFile));
      }
    }
    return csvColumns;
  }
}
