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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class PartitionFieldConverterImpl implements FieldConverter {

  private final DataField field;
  private final int index;

  public PartitionFieldConverterImpl(DataField field, int index) {
    this.field = field;
    this.index = index;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    String dimensionValue = row.getString(index);
    row.update(convert(dimensionValue, logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder) throws RuntimeException {
    try {
      return DataTypeUtil
          .getDataDataTypeForNoDictionaryColumn((String) value, field.getColumn().getDataType(),
              field.getDateFormat());
    } catch (Exception e) {
      String message = logHolder.getColumnMessageMap().get(field.getColumn().getColName());
      if (null == message) {
        message = CarbonDataProcessorUtil
            .prepareFailureReason(field.getColumn().getColName(), field.getColumn().getDataType());
        logHolder.getColumnMessageMap().put(field.getColumn().getColName(), message);
      }
      logHolder.setReason(message);
    }
    return getNullValue();
  }

  @Override
  public DataField getDataField() {
    return field;
  }

  @Override
  public void clear() {

  }

  private byte[] getNullValue() {
    if (field.isUseActualData()) {
      return null;
    } else if (field.getColumn().getDataType() == DataTypes.STRING) {
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    } else {
      return CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
  }
}
