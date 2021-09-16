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

package org.apache.carbondata.processing.loading.converter.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.log4j.Logger;

/**
 * It converts the complete row if necessary, dictionary columns are encoded with dictionary values
 * and nondictionary values are converted to binary.
 */
public class RowConverterImpl implements RowConverter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(RowConverterImpl.class.getName());

  private CarbonDataLoadConfiguration configuration;

  private DataField[] fields;

  private FieldConverter[] fieldConverters;

  private BadRecordsLogger badRecordLogger;

  private BadRecordLogHolder logHolder;

  private boolean isConvertToBinary;

  public RowConverterImpl(DataField[] fields, CarbonDataLoadConfiguration configuration,
      BadRecordsLogger badRecordLogger) {
    this.fields = fields;
    this.configuration = configuration;
    this.badRecordLogger = badRecordLogger;
  }

  public RowConverterImpl(DataField[] fields, CarbonDataLoadConfiguration configuration,
      BadRecordsLogger badRecordLogger, boolean isConvertToBinary) {
    this.fields = fields;
    this.configuration = configuration;
    this.badRecordLogger = badRecordLogger;
    this.isConvertToBinary = isConvertToBinary;
  }

  @Override
  public void initialize() {
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    boolean isEmptyBadRecord = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD)
            .toString());
    List<FieldConverter> fieldConverterList = new ArrayList<>();
    List<FieldConverter> nonSchemaFieldConverterList = new ArrayList<>();
    long lruCacheStartTime = System.currentTimeMillis();
    Map<String, String> properties =
        configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
            .getTableProperties();
    String spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
    for (int i = 0; i < fields.length; i++) {
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], i, nullFormat, isEmptyBadRecord, isConvertToBinary,
              (String) configuration.getDataLoadProperty(
                  CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER),
              configuration);
      if (spatialProperty != null && fields[i].getColumn().getColName()
          .equalsIgnoreCase(spatialProperty.trim())) {
        nonSchemaFieldConverterList.add(fieldConverter);
      } else {
        fieldConverterList.add(fieldConverter);
      }
    }
    fieldConverterList.addAll(nonSchemaFieldConverterList);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordLruCacheLoadTime((System.currentTimeMillis() - lruCacheStartTime) / 1000.0);
    fieldConverters = fieldConverterList.toArray(new FieldConverter[0]);
    logHolder = new BadRecordLogHolder();
  }

  @Override
  public CarbonRow convert(CarbonRow row) throws CarbonDataLoadingException {
    logHolder.setLogged(false);
    logHolder.clear();
    Map<String, String> properties =
        configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
            .getTableProperties();
    String spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
    boolean isSpatialColumn = false;
    for (int i = 0; i < fieldConverters.length; i++) {
      if (spatialProperty != null) {
        isSpatialColumn = fieldConverters[i].getDataField().getColumn().getColName()
            .equalsIgnoreCase(spatialProperty.trim());
      }
      if (configuration.isNonSchemaColumnsPresent() && !isSpatialColumn) {
        // Skip the conversion for schema columns if the conversion is required only for non-schema
        // columns
        continue;
      }
      fieldConverters[i].convert(row, logHolder);
      if (!logHolder.isLogged() && logHolder.isBadRecordNotAdded()) {
        String reason = logHolder.getReason();
        if (reason.equalsIgnoreCase(CarbonCommonConstants.STRING_LENGTH_EXCEEDED_MESSAGE)) {
          reason = String.format(reason, this.fields[i].getColumn().getColName());
        }
        if (!badRecordLogger.isCompFlow()) {
          badRecordLogger.addBadRecordsToBuilder(row.getRawData(), reason);
          if (badRecordLogger.isDataLoadFail()) {
            String error = "Data load failed due to bad record: " + reason;
            if (!badRecordLogger.isBadRecordLoggerEnable()) {
              error += "Please enable bad record logger to know the detail reason.";
            }
            throw new BadRecordFoundException(error);
          }
        }
        logHolder.clear();
        logHolder.setLogged(true);
        if (badRecordLogger.isBadRecordConvertNullDisable()) {
          return null;
        }
      }
    }
    // rawData will not be required after this so reset the entry to null.
    row.setRawData(null);
    return row;
  }

  @Override
  public void finish() {
    for (int i = 0; i < fieldConverters.length; i++) {
      fieldConverters[i].clear();
    }
  }

  @Override
  public RowConverter createCopyForNewThread() {
    RowConverterImpl converter =
        new RowConverterImpl(this.fields, this.configuration, this.badRecordLogger,
            this.isConvertToBinary);
    List<FieldConverter> fieldConverterList = new ArrayList<>();
    List<FieldConverter> nonSchemaFieldConverterList = new ArrayList<>();
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    boolean isEmptyBadRecord = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD)
            .toString());
    Map<String, String> properties =
        configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
            .getTableProperties();
    String spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
    for (int i = 0; i < fields.length; i++) {
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], i, nullFormat, isEmptyBadRecord, isConvertToBinary,
              (String) configuration.getDataLoadProperty(
                  CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER),
              configuration);
      if (spatialProperty != null && fields[i].getColumn().getColName()
          .equalsIgnoreCase(spatialProperty.trim())) {
        nonSchemaFieldConverterList.add(fieldConverter);
      } else {
        fieldConverterList.add(fieldConverter);
      }
    }
    fieldConverterList.addAll(nonSchemaFieldConverterList);
    converter.fieldConverters = fieldConverterList.toArray(new FieldConverter[0]);
    converter.logHolder = new BadRecordLogHolder();
    return converter;
  }

  @Override
  public FieldConverter[] getFieldConverters() {
    return fieldConverters;
  }
}
