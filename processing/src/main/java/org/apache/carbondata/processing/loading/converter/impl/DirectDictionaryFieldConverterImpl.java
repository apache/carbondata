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
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class DirectDictionaryFieldConverterImpl implements FieldConverter {

  private DirectDictionaryGenerator directDictionaryGenerator;

  private int index;
  private DataField dataField;

  private String nullFormat;

  private CarbonColumn column;
  private boolean isEmptyBadRecord;

  public DirectDictionaryFieldConverterImpl(DataField dataField, String nullFormat, int index,
      boolean isEmptyBadRecord) {
    this.nullFormat = nullFormat;
    this.column = dataField.getColumn();
    if (dataField.getColumn().getDataType() == DataTypes.DATE && dataField.getDateFormat() != null
        && !dataField.getDateFormat().isEmpty()) {
      this.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(dataField.getColumn().getDataType(),
              dataField.getDateFormat());

    } else if (dataField.getColumn().getDataType() == DataTypes.TIMESTAMP
        && dataField.getTimestampFormat() != null && !dataField.getTimestampFormat().isEmpty()) {
      this.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(dataField.getColumn().getDataType(),
              dataField.getTimestampFormat());
    } else {
      this.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(dataField.getColumn().getDataType());
    }
    this.index = index;
    this.dataField = dataField;
    this.isEmptyBadRecord = isEmptyBadRecord;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder) {
    String value = row.getString(index);
    row.update(convert(value, logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder)
      throws RuntimeException {
    String literalValue = (String) value;
    if (literalValue == null) {
      logHolder.setReason(
          CarbonDataProcessorUtil.prepareFailureReason(column.getColName(), column.getDataType()));
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    } else if (literalValue.equals(nullFormat)) {
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    } else {
      int key = directDictionaryGenerator.generateDirectSurrogateKey(literalValue);
      if (key == CarbonCommonConstants.DIRECT_DICT_VALUE_NULL) {
        if ((literalValue.length() > 0) || (literalValue.length() == 0 && isEmptyBadRecord)) {
          String message = logHolder.getColumnMessageMap().get(column.getColName());
          if (null == message) {
            message = CarbonDataProcessorUtil.prepareFailureReason(
                column.getColName(), column.getDataType());
            logHolder.getColumnMessageMap().put(column.getColName(), message);
          }
          logHolder.setReason(message);
        }
      }
      return key;
    }
  }

  @Override
  public DataField getDataField() {
    return dataField;
  }

  /**
   * Method to clean the dictionary cache. In this instance nothing to clear.
   */
  @Override
  public void clear() {
  }

}
