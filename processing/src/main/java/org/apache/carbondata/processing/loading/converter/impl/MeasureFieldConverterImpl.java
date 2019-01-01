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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

/**
 * Converter for measure
 */
public class MeasureFieldConverterImpl implements FieldConverter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MeasureFieldConverterImpl.class.getName());

  private int index;

  private String nullFormat;

  private boolean isEmptyBadRecord;

  private DataField dataField;

  public MeasureFieldConverterImpl(DataField dataField, String nullFormat, int index,
      boolean isEmptyBadRecord) {
    this.nullFormat = nullFormat;
    this.index = index;
    this.isEmptyBadRecord = isEmptyBadRecord;
    this.dataField = dataField;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    String value = row.getString(index);
    row.update(convert(value, logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder)
      throws RuntimeException {
    String literalValue = (String) (value);
    Object output;
    boolean isNull = CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(literalValue);
    if (literalValue == null || isNull) {
      String message = logHolder.getColumnMessageMap().get(dataField.getColumn().getColName());
      if (null == message) {
        message = CarbonDataProcessorUtil.prepareFailureReason(dataField.getColumn().getColName(),
            dataField.getColumn().getDataType());
        logHolder.getColumnMessageMap().put(dataField.getColumn().getColName(), message);
      }
      if (dataField.getColumn().isDimension()) {
        logHolder.setReason(message);
      }
      return null;
    } else if (literalValue.length() == 0) {
      if (isEmptyBadRecord) {
        String message = logHolder.getColumnMessageMap().get(dataField.getColumn().getColName());
        if (null == message) {
          message = CarbonDataProcessorUtil.prepareFailureReason(dataField.getColumn().getColName(),
              dataField.getColumn().getDataType());
          logHolder.getColumnMessageMap().put(dataField.getColumn().getColName(), message);
        }
        logHolder.setReason(message);
      }
      return null;
    } else if (literalValue.equals(nullFormat)) {
      return null;
    } else {
      try {
        // in case of no dictionary dimension
        if (dataField.getColumn().isDimension()) {
          String dateFormat = null;
          if (dataField.getColumn().getDataType() == DataTypes.DATE) {
            dateFormat = dataField.getDateFormat();
          } else if (dataField.getColumn().getDataType() == DataTypes.TIMESTAMP) {
            dateFormat = dataField.getTimestampFormat();
          }
          if (dataField.isUseActualData()) {
            output = DataTypeUtil.getNoDictionaryValueBasedOnDataType(literalValue,
                dataField.getColumn().getDataType(),
                dataField.getColumn().getColumnSchema().getScale(),
                dataField.getColumn().getColumnSchema().getPrecision(), true, dateFormat);
          } else {
            output = DataTypeUtil.getNoDictionaryValueBasedOnDataType(literalValue,
                dataField.getColumn().getDataType(),
                dataField.getColumn().getColumnSchema().getScale(),
                dataField.getColumn().getColumnSchema().getPrecision(), false, dateFormat);
          }
        } else {
          if (dataField.isUseActualData()) {
            output = DataTypeUtil
                .getMeasureValueBasedOnDataType(literalValue, dataField.getColumn().getDataType(),
                    dataField.getColumn().getColumnSchema().getScale(),
                    dataField.getColumn().getColumnSchema().getPrecision(), true);
          } else {
            output = DataTypeUtil
                .getMeasureValueBasedOnDataType(literalValue, dataField.getColumn().getDataType(),
                    dataField.getColumn().getColumnSchema().getScale(),
                    dataField.getColumn().getColumnSchema().getPrecision());
          }
        }
        return output;
      } catch (NumberFormatException e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Cannot convert value to Numeric type value. Value considered as null.");
        }
        logHolder.setReason(CarbonDataProcessorUtil
            .prepareFailureReason(dataField.getColumn().getColName(),
                dataField.getColumn().getDataType()));
        return null;
      }
    }
  }


  /**
   * Method to clean the dictionary cache. As in this MeasureFieldConverterImpl convert no
   * dictionary caches are acquired so nothing to clear. s
   */
  @Override public void clear() {
  }
}
