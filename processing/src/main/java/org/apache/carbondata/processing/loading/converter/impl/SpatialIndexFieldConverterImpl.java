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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.util.CustomIndex;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.log4j.Logger;

/**
 * Converter for spatial index columns
 */
public class SpatialIndexFieldConverterImpl extends MeasureFieldConverterImpl {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MeasureFieldConverterImpl.class.getName());
  private int index;
  private int[] sourceIndexes;
  CustomIndex instance;

  public SpatialIndexFieldConverterImpl(DataField dataField, String nullFormat, int index,
      boolean isEmptyBadRecord, CarbonDataLoadConfiguration configuration) {
    super(dataField, nullFormat, index, isEmptyBadRecord);
    this.index = index;
    Map<String, String> properties =
        configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
            .getTableProperties();
    try {
      instance = CustomIndex.getCustomInstance(properties.get(
          CarbonCommonConstants.SPATIAL_INDEX + "." + dataField.getColumn().getColName()
              + ".instance"));
    } catch (IOException e) {
      LOGGER.error("Failed to get the custom instance", e);
      throw new RuntimeException(e);
    }
    String sourceColumns = properties.get(
        CarbonCommonConstants.SPATIAL_INDEX + "." + dataField.getColumn().getColName()
            + ".sourcecolumns");
    String[] sources = sourceColumns.split(",");
    sourceIndexes = new int[sources.length];
    int idx = 0;
    for (String source : sources) {
      sourceIndexes[idx++] = getDataFieldIndexByName(configuration.getDataFields(), source);
    }
  }

  private int getDataFieldIndexByName(DataField[] fields, String column) {
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].getColumn().getColName().equalsIgnoreCase(column)) {
        return i;
      }
    }
    return -1;
  }

  private String generateIndexValue(CarbonRow row) {
    List<Object> sourceValues = new ArrayList<Object>();
    for (int sourceIndex : sourceIndexes) {
      sourceValues.add(row.getData()[sourceIndex]);
    }
    String value = null;
    try {
      value = instance.generate(sourceValues);
    } catch (Exception e) {
      LOGGER.error("Failed to generate index column value", e);
    }
    return value;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    if (row.getData()[index] == null) {
      row.update(generateIndexValue(row), index);
    }
    super.convert(row, logHolder);
  }
}
