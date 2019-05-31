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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.Maps;
import org.apache.carbondata.common.Strings;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.processing.util.TableOptionConstant;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Builder for {@link CarbonLoadModel}
 */
@InterfaceAudience.Internal
public class CarbonLoadModelBuilder {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      CarbonLoadModelBuilder.class.getName());
  private CarbonTable table;

  public CarbonLoadModelBuilder(CarbonTable table) {
    this.table = table;
  }

  /**
   * build CarbonLoadModel for data loading
   * @param options Load options from user input
   * @param taskNo
   * @return a new CarbonLoadModel instance
   */
  public CarbonLoadModel build(Map<String, String>  options, long timestamp, String taskNo)
      throws InvalidLoadOptionException, IOException {
    Map<String, String> optionsFinal = LoadOption.fillOptionWithDefaultValue(options);

    if (!options.containsKey("fileheader")) {
      List<CarbonColumn> csvHeader = table.getCreateOrderColumn(table.getTableName());
      String[] columns = new String[csvHeader.size()];
      for (int i = 0; i < columns.length; i++) {
        columns[i] = csvHeader.get(i).getColName();
      }
      optionsFinal.put("fileheader", Strings.mkString(columns, ","));
    }
    optionsFinal.put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options, table));
    optionsFinal.put("sort_scope",
        Maps.getOrDefault(options, "sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT));
    CarbonLoadModel model = new CarbonLoadModel();
    model.setCarbonTransactionalTable(table.isTransactionalTable());
    model.setFactTimeStamp(timestamp);
    model.setTaskNo(taskNo);

    // we have provided 'fileheader', so it hadoopConf can be null
    build(options, optionsFinal, model, null);
    String timestampFormat = options.get("timestampformat");
    if (timestampFormat == null) {
      timestampFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    }
    String dateFormat = options.get("dateFormat");
    if (dateFormat == null) {
      dateFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
    }
    model.setDateFormat(dateFormat);
    model.setTimestampformat(timestampFormat);
    model.setUseOnePass(Boolean.parseBoolean(Maps.getOrDefault(options, "onepass", "false")));
    model.setDictionaryServerHost(Maps.getOrDefault(options, "dicthost", null));
    try {
      model.setDictionaryServerPort(Integer.parseInt(Maps.getOrDefault(options, "dictport", "-1")));
    } catch (NumberFormatException e) {
      throw new InvalidLoadOptionException(e.getMessage());
    }
    validateAndSetColumnCompressor(model);
    validateAndSetBinaryDecoder(model);
    return model;
  }

  /**
   * build CarbonLoadModel for data loading
   * @param options Load options from user input
   * @param optionsFinal Load options that populated with default values for optional options
   * @param carbonLoadModel The output load model
   * @param hadoopConf hadoopConf is needed to read CSV header if there 'fileheader' is not set in
   *                   user provided load options
   */
  public void build(
      Map<String, String> options,
      Map<String, String> optionsFinal,
      CarbonLoadModel carbonLoadModel,
      Configuration hadoopConf) throws InvalidLoadOptionException, IOException {
    build(options, optionsFinal, carbonLoadModel, hadoopConf, new HashMap<String, String>(), false);
  }

  /**
   * build CarbonLoadModel for data loading
   * @param options Load options from user input
   * @param optionsFinal Load options that populated with default values for optional options
   * @param carbonLoadModel The output load model
   * @param hadoopConf hadoopConf is needed to read CSV header if there 'fileheader' is not set in
   *                   user provided load options
   * @param partitions partition name map to path
   * @param isDataFrame true if build for load for dataframe
   */
  public void build(
      Map<String, String> options,
      Map<String, String> optionsFinal,
      CarbonLoadModel carbonLoadModel,
      Configuration hadoopConf,
      Map<String, String> partitions,
      boolean isDataFrame) throws InvalidLoadOptionException, IOException {
    carbonLoadModel.setTableName(table.getTableName());
    carbonLoadModel.setDatabaseName(table.getDatabaseName());
    carbonLoadModel.setTablePath(table.getTablePath());
    carbonLoadModel.setTableName(table.getTableName());
    carbonLoadModel.setCarbonTransactionalTable(table.isTransactionalTable());
    CarbonDataLoadSchema dataLoadSchema = new CarbonDataLoadSchema(table);
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema);
    String sort_scope = optionsFinal.get("sort_scope");
    String single_pass = optionsFinal.get("single_pass");
    String bad_records_logger_enable = optionsFinal.get("bad_records_logger_enable");
    String bad_records_action = optionsFinal.get("bad_records_action");
    String bad_record_path = optionsFinal.get("bad_record_path");
    String global_sort_partitions = optionsFinal.get("global_sort_partitions");
    String timestampformat = optionsFinal.get("timestampformat");
    String dateFormat = optionsFinal.get("dateformat");
    String delimiter = optionsFinal.get("delimiter");
    String complex_delimiter_level1 = optionsFinal.get("complex_delimiter_level_1");
    String complex_delimiter_level2 = optionsFinal.get("complex_delimiter_level_2");
    String complex_delimiter_level3 = optionsFinal.get("complex_delimiter_level_3");
    String complex_delimiter_level4 = optionsFinal.get("complex_delimiter_level_4");
    String all_dictionary_path = optionsFinal.get("all_dictionary_path");
    String column_dict = optionsFinal.get("columndict");
    validateDateTimeFormat(timestampformat, "TimestampFormat");
    validateDateTimeFormat(dateFormat, "DateFormat");
    validateSortScope(sort_scope);

    if (Boolean.parseBoolean(bad_records_logger_enable) ||
        LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
      if (!StringUtils.isEmpty(bad_record_path)) {
        bad_record_path = CarbonUtil.checkAndAppendHDFSUrl(bad_record_path);
      } else {
        throw new InvalidLoadOptionException(
            "Cannot redirect bad records as bad record location is not provided.");
      }
    }

    carbonLoadModel.setBadRecordsLocation(bad_record_path);

    validateGlobalSortPartitions(global_sort_partitions);
    carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal.get("escapechar"), "\\"));
    carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal.get("quotechar"), "\""));
    carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal.get("commentchar"), "#"));

    // if there isn't file header in csv file and load sql doesn't provide FILEHEADER option,
    // we should use table schema to generate file header.
    String fileHeader = optionsFinal.get("fileheader");
    String headerOption = options.get("header");
    if (headerOption != null) {
      if (!headerOption.equalsIgnoreCase("true") &&
          !headerOption.equalsIgnoreCase("false")) {
        throw new InvalidLoadOptionException(
            "'header' option should be either 'true' or 'false'.");
      }
      // whether the csv file has file header, the default value is true
      if (Boolean.valueOf(headerOption)) {
        if (!StringUtils.isEmpty(fileHeader)) {
          throw new InvalidLoadOptionException(
              "When 'header' option is true, 'fileheader' option is not required.");
        }
      } else {
        if (StringUtils.isEmpty(fileHeader)) {
          List<CarbonColumn> columns = table.getCreateOrderColumn(table.getTableName());
          List<String> columnNames = new ArrayList<>();
          List<String> partitionColumns = new ArrayList<>();
          for (int i = 0; i < columns.size(); i++) {
            if (table.getPartitionInfo() != null && table.getPartitionInfo().getColumnSchemaList()
                .contains(columns.get(i).getColumnSchema())) {
              partitionColumns.add(columns.get(i).getColName());
            } else {
              columnNames.add(columns.get(i).getColName());
            }
          }
          columnNames.addAll(partitionColumns);
          fileHeader = Strings.mkString(columnNames.toArray(new String[columnNames.size()]), ",");
        }
      }
    }

    String binaryDecoder = options.get("binary_decoder");
    carbonLoadModel.setBinaryDecoder(binaryDecoder);
    carbonLoadModel.setTimestampformat(timestampformat);
    carbonLoadModel.setDateFormat(dateFormat);
    carbonLoadModel.setDefaultTimestampFormat(
        CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));

    carbonLoadModel.setDefaultDateFormat(
        CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_DATE_FORMAT,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));

    carbonLoadModel.setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + "," +
            optionsFinal.get("serialization_null_format"));

    carbonLoadModel.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + "," + bad_records_logger_enable);

    carbonLoadModel.setBadRecordsAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName() + "," + bad_records_action.toUpperCase());

    carbonLoadModel.setIsEmptyDataBadRecord(
        DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," +
            optionsFinal.get("is_empty_data_bad_record"));

    carbonLoadModel.setSkipEmptyLine(optionsFinal.get("skip_empty_line"));

    carbonLoadModel.setSortScope(sort_scope);
    carbonLoadModel.setBatchSortSizeInMb(optionsFinal.get("batch_sort_size_inmb"));
    carbonLoadModel.setGlobalSortPartitions(global_sort_partitions);
    carbonLoadModel.setUseOnePass(Boolean.parseBoolean(single_pass));

    if (delimiter.equalsIgnoreCase(complex_delimiter_level1) ||
        complex_delimiter_level1.equalsIgnoreCase(complex_delimiter_level2) ||
        delimiter.equalsIgnoreCase(complex_delimiter_level2) ||
        delimiter.equalsIgnoreCase(complex_delimiter_level3)) {
      throw new InvalidLoadOptionException("Field Delimiter and Complex types delimiter are same");
    } else {
      carbonLoadModel.setComplexDelimiter(complex_delimiter_level1);
      carbonLoadModel.setComplexDelimiter(complex_delimiter_level2);
      carbonLoadModel.setComplexDelimiter(complex_delimiter_level3);
      carbonLoadModel.setComplexDelimiter(complex_delimiter_level4);
    }
    // set local dictionary path, and dictionary file extension
    carbonLoadModel.setAllDictPath(all_dictionary_path);
    carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter));
    carbonLoadModel.setCsvHeader(fileHeader);
    carbonLoadModel.setColDictFilePath(column_dict);

    List<String> ignoreColumns = new ArrayList<>();
    if (!isDataFrame) {
      for (Map.Entry<String, String> partition : partitions.entrySet()) {
        if (partition.getValue() != null) {
          ignoreColumns.add(partition.getKey());
        }
      }
    }

    carbonLoadModel.setCsvHeaderColumns(
        LoadOption.getCsvHeaderColumns(carbonLoadModel, hadoopConf, ignoreColumns));

    int validatedMaxColumns = validateMaxColumns(
        carbonLoadModel.getCsvHeaderColumns(),
        optionsFinal.get("maxcolumns"));

    carbonLoadModel.setMaxColumns(String.valueOf(validatedMaxColumns));
    if (carbonLoadModel.isCarbonTransactionalTable()) {
      carbonLoadModel.readAndSetLoadMetadataDetails();
    }
    carbonLoadModel.setSortColumnsBoundsStr(optionsFinal.get("sort_column_bounds"));
    carbonLoadModel.setLoadMinSize(
        optionsFinal.get(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB));
    validateAndSetLoadMinSize(carbonLoadModel);

    validateAndSetColumnCompressor(carbonLoadModel);
    validateAndSetBinaryDecoder(carbonLoadModel);

    validateRangeColumn(optionsFinal, carbonLoadModel);
  }

  private void validateRangeColumn(Map<String, String> optionsFinal,
      CarbonLoadModel carbonLoadModel) throws InvalidLoadOptionException {
    String scaleFactor = optionsFinal.get("scale_factor");
    if (scaleFactor != null) {
      try {
        int scale = Integer.parseInt(scaleFactor);
        if (scale < 1 || scale > 300) {
          throw new InvalidLoadOptionException(
              "Invalid scale_factor option, the range of scale_factor should be [1, 300]");
        }
        carbonLoadModel.setScaleFactor(scale);
      } catch (NumberFormatException ex) {
        throw new InvalidLoadOptionException(
            "Invalid scale_factor option, scale_factor should be a integer");
      }
    }
  }

  private int validateMaxColumns(String[] csvHeaders, String maxColumns)
      throws InvalidLoadOptionException {
    /*
    User configures both csvheadercolumns, maxcolumns,
      if csvheadercolumns >= maxcolumns, give error
      if maxcolumns > threashold, give error
    User configures csvheadercolumns
      if csvheadercolumns >= maxcolumns(default) then maxcolumns = csvheadercolumns+1
      if csvheadercolumns >= threashold, give error
    User configures nothing
      if csvheadercolumns >= maxcolumns(default) then maxcolumns = csvheadercolumns+1
      if csvheadercolumns >= threashold, give error
     */
    int columnCountInSchema = csvHeaders.length;
    int maxNumberOfColumnsForParsing = 0;
    Integer maxColumnsInt = getMaxColumnValue(maxColumns);
    if (maxColumnsInt != null) {
      if (columnCountInSchema >= maxColumnsInt) {
        throw new InvalidLoadOptionException(
            "csv headers should be less than the max columns " + maxColumnsInt);
      } else if (maxColumnsInt > CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
        throw new InvalidLoadOptionException(
            "max columns cannot be greater than the threshold value: " +
                CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING);
      } else {
        maxNumberOfColumnsForParsing = maxColumnsInt;
      }
    } else if (columnCountInSchema >= CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
      throw new InvalidLoadOptionException(
          "csv header columns should be less than max threashold: " +
              CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING);
    } else if (columnCountInSchema >= CSVInputFormat.DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
      maxNumberOfColumnsForParsing = columnCountInSchema + 1;
    } else {
      maxNumberOfColumnsForParsing = CSVInputFormat.DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING;
    }
    return maxNumberOfColumnsForParsing;
  }

  private Integer getMaxColumnValue(String maxColumn) {
    return (maxColumn == null) ? null : Integer.parseInt(maxColumn);
  }

  /**
   * validates both timestamp and date for illegal values
   */
  private void validateDateTimeFormat(String dateTimeLoadFormat, String dateTimeLoadOption)
      throws InvalidLoadOptionException {
    // allowing empty value to be configured for dateformat option.
    if (dateTimeLoadFormat != null && !dateTimeLoadFormat.trim().equalsIgnoreCase("")) {
      try {
        new SimpleDateFormat(dateTimeLoadFormat);
      } catch (IllegalArgumentException e) {
        throw new InvalidLoadOptionException(
            "Error: Wrong option: " + dateTimeLoadFormat + " is provided for option "
                + dateTimeLoadOption);
      }
    }
  }

  private void validateSortScope(String sortScope) throws InvalidLoadOptionException {
    if (sortScope != null) {
      // We support global sort for Hive standard partition, but don't support
      // global sort for other partition type.
      if (table.getPartitionInfo(table.getTableName()) != null &&
          !table.isHivePartitionTable() &&
          sortScope.equalsIgnoreCase(SortScopeOptions.SortScope.GLOBAL_SORT.toString())) {
        throw new InvalidLoadOptionException("Don't support use global sort on "
            + table.getPartitionInfo().getPartitionType() +  " partition table.");
      }
    }
  }

  private void validateGlobalSortPartitions(String globalSortPartitions)
      throws InvalidLoadOptionException {
    if (globalSortPartitions != null) {
      try {
        int num = Integer.parseInt(globalSortPartitions);
        if (num <= 0) {
          throw new InvalidLoadOptionException("'GLOBAL_SORT_PARTITIONS' should be greater than 0");
        }
      } catch (NumberFormatException e) {
        throw new InvalidLoadOptionException(e.getMessage());
      }
    }
  }

  private void validateAndSetColumnCompressor(CarbonLoadModel carbonLoadModel)
      throws InvalidLoadOptionException {
    try {
      String columnCompressor = carbonLoadModel.getColumnCompressor();
      if (StringUtils.isBlank(columnCompressor)) {
        columnCompressor = CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.COMPRESSOR, CarbonCommonConstants.DEFAULT_COMPRESSOR);
      }
      // check and load compressor
      CompressorFactory.getInstance().getCompressor(columnCompressor);
      carbonLoadModel.setColumnCompressor(columnCompressor);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new InvalidLoadOptionException("Failed to load the compressor");
    }
  }


  private void validateAndSetBinaryDecoder(CarbonLoadModel carbonLoadModel) {
    String binaryDecoder = carbonLoadModel.getBinaryDecoder();
    if (!CarbonLoaderUtil.isValidBinaryDecoder(binaryDecoder)) {
      throw new CarbonDataLoadingException("Binary decoder only support Base64, " +
          "Hex or no decode for string, don't support " + binaryDecoder);
    }
    if (StringUtils.isBlank(binaryDecoder)) {
      binaryDecoder = CarbonProperties.getInstance().getProperty(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER,
          CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_DEFAULT);
    }
    // check and load binary decoder
    carbonLoadModel.setBinaryDecoder(binaryDecoder);
  }

  /**
   * check whether using default value or not
   */
  private String checkDefaultValue(String value, String defaultValue) {
    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    } else {
      return value;
    }
  }

  private void validateAndSetLoadMinSize(CarbonLoadModel carbonLoadModel) {
    int size = 0;
    String loadMinSize = carbonLoadModel.getLoadMinSize();
    try {
      size = Integer.parseInt(loadMinSize);
    } catch (Exception e) {
      size = 0;
    }
    // if the value is negative, set the value is 0
    if (size > 0) {
      carbonLoadModel.setLoadMinSize(loadMinSize);
    } else {
      carbonLoadModel.setLoadMinSize(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT);
    }
  }
}
