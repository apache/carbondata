/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow.converter.impl;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.FieldConverter;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

/**
 * Converter for measure
 */
public class MeasureFieldConverterImpl implements FieldConverter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MeasureFieldConverterImpl.class.getName());

  private int index;

  private DataType dataType;

  private CarbonMeasure measure;

  private String nullformat;

  public MeasureFieldConverterImpl(DataField dataField, String nullformat, int index) {
    this.dataType = dataField.getColumn().getDataType();
    this.measure = (CarbonMeasure) dataField.getColumn();
    this.nullformat = nullformat;
    this.index = index;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    String value = row.getString(index);
    Object output;
    if (value == null) {
      logHolder.setReason(
          "The value " + " \"" + value + "\"" + " with column name " + measure.getColName()
              + " and column data type " + dataType + " is not a valid " + dataType + " type.");
      row.update(null, index);
    } else if(value.equals(nullformat)) {
      row.update(null, index);
    } else {
      try {
        output = DataTypeUtil.getMeasureValueBasedOnDataType(value, dataType, measure);
        row.update(output, index);
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Cant not convert : " + value + " to Numeric type value. Value considered as null.");
        logHolder.setReason(
            "The value " + " \"" + value + "\"" + " with column name " + measure.getColName()
                + " and column data type " + dataType + " is not a valid " + dataType + " type.");
        output = null;
        row.update(output, index);
      }
    }

  }
}
