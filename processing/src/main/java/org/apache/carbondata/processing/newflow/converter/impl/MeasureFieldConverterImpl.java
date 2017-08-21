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
package org.apache.carbondata.processing.newflow.converter.impl;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.converter.FieldConverter;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

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

  private boolean isEmptyBadRecord;

  public MeasureFieldConverterImpl(DataField dataField, String nullformat, int index,
      boolean isEmptyBadRecord) {
    this.dataType = dataField.getColumn().getDataType();
    this.measure = (CarbonMeasure) dataField.getColumn();
    this.nullformat = nullformat;
    this.index = index;
    this.isEmptyBadRecord = isEmptyBadRecord;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    String value = row.getString(index);
    Object output;
    boolean isNull = CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(value);
    if (value == null || isNull) {
      String message = logHolder.getColumnMessageMap().get(measure.getColName());
      if (null == message) {
        message = CarbonDataProcessorUtil
            .prepareFailureReason(measure.getColName(), measure.getDataType());
        logHolder.getColumnMessageMap().put(measure.getColName(), message);
      }
      row.update(null, index);
    } else if (value.length() == 0) {
      if (isEmptyBadRecord) {
        String message = logHolder.getColumnMessageMap().get(measure.getColName());
        if (null == message) {
          message = CarbonDataProcessorUtil
              .prepareFailureReason(measure.getColName(), measure.getDataType());
          logHolder.getColumnMessageMap().put(measure.getColName(), message);
        }
        logHolder.setReason(message);
      }
      row.update(null, index);
    } else if (value.equals(nullformat)) {
      row.update(null, index);
    } else {
      try {
        output = DataTypeUtil.getMeasureValueBasedOnDataType(value, dataType, measure);
        row.update(output, index);
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Cant not convert value to Numeric type value. Value considered as null.");
        logHolder.setReason(
            CarbonDataProcessorUtil.prepareFailureReason(measure.getColName(), dataType));
        output = null;
        row.update(output, index);
      }
    }

  }
}
