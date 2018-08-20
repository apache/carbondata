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

package org.apache.carbondata.spark.format;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory;

/**
 * read support for csv, it will convert csv data to carbon converted values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Internal
public class CsvToCarbonReadSupport<T> implements CarbonReadSupport<T> {
  private CarbonColumn[] carbonColumns;
  private FieldConverter[] fieldConverters;
  private BadRecordLogHolder badRecordLogHolder;
  private Object[] finalOutputValues;

  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
      throws IOException {
    this.carbonColumns = carbonColumns;
    this.finalOutputValues = new Object[carbonColumns.length];
    this.initFieldConverters(carbonTable);
  }

  private void initFieldConverters(CarbonTable carbonTable) throws IOException {
    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from(
        carbonTable.getTablePath(), carbonTable.getCarbonTableIdentifier());
    String nullFormat = "\\N";
    Map<Object, Integer>[] localCaches = new Map[carbonColumns.length];
    this.fieldConverters = new FieldConverter[carbonColumns.length];

    for (int i = 0; i < carbonColumns.length; i++) {
      localCaches[i] = new ConcurrentHashMap<>();
      DataField dataField = new DataField(carbonColumns[i]);
      String dateFormat = CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
      dataField.setDateFormat(dateFormat);
      String tsFormat = CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      dataField.setTimestampFormat(tsFormat);
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(dataField, absoluteTableIdentifier, i, nullFormat, null, false,
              localCaches[i], false, "");
      this.fieldConverters[i] = fieldConverter;
    }

    this.badRecordLogHolder = new BadRecordLogHolder();
    this.badRecordLogHolder.setLogged(false);
  }

  @Override
  public T readRow(Object[] data) {
    for (int i = 0; i < carbonColumns.length; i++) {
      Object originValue = data[i];
      finalOutputValues[i] = fieldConverters[i].convert(originValue, badRecordLogHolder);
    }
    return (T) finalOutputValues;
  }

  @Override
  public void close() {
    for (int i = 0; i < fieldConverters.length; i++) {
      fieldConverters[i].clear();
    }
  }
}
