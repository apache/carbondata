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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class NonDictionaryFieldConverterImpl implements FieldConverter {

  private DataType dataType;

  private int index;

  private String nullformat;

  private CarbonColumn column;

  private boolean isEmptyBadRecord;

  private DataField dataField;

  public NonDictionaryFieldConverterImpl(DataField dataField, String nullformat, int index,
      boolean isEmptyBadRecord) {
    this.dataField = dataField;
    this.dataType = dataField.getColumn().getDataType();
    this.column = dataField.getColumn();
    this.index = index;
    this.nullformat = nullformat;
    this.isEmptyBadRecord = isEmptyBadRecord;
  }

  @Override public void convert(CarbonRow row, BadRecordLogHolder logHolder) {
    String dimensionValue = row.getString(index);
    if (null == dimensionValue && column.getDataType() != DataTypes.STRING) {
      logHolder.setReason(
          CarbonDataProcessorUtil.prepareFailureReason(column.getColName(), column.getDataType()));
      updateWithNullValue(row);
    } else if (dimensionValue == null || dimensionValue.equals(nullformat)) {
      updateWithNullValue(row);
    } else {
      String dateFormat = null;
      if (dataType == DataTypes.DATE) {
        dateFormat = dataField.getDateFormat();
      } else if (dataType == DataTypes.TIMESTAMP) {
        dateFormat = dataField.getTimestampFormat();
      }
      try {
        row.update(DataTypeUtil
            .getBytesBasedOnDataTypeForNoDictionaryColumn(dimensionValue, dataType,
                dateFormat), index);
      } catch (Throwable ex) {
        if (dimensionValue.length() > 0 || (dimensionValue.length() == 0 && isEmptyBadRecord)) {
          String message = logHolder.getColumnMessageMap().get(column.getColName());
          if (null == message) {
            message = CarbonDataProcessorUtil
                .prepareFailureReason(column.getColName(), column.getDataType());
            logHolder.getColumnMessageMap().put(column.getColName(), message);
          }
          logHolder.setReason(message);
          updateWithNullValue(row);
        } else {
          updateWithNullValue(row);
        }
      }
    }
  }

  private void updateWithNullValue(CarbonRow row) {
    if (dataType == DataTypes.STRING) {
      row.update(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, index);
    } else {
      row.update(CarbonCommonConstants.EMPTY_BYTE_ARRAY, index);
    }
  }
}
