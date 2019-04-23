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

import java.nio.charset.Charset;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Converter for binary
 */
public class BinaryFieldConverterImpl implements FieldConverter {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BinaryFieldConverterImpl.class.getName());

  private int index;
  private DataType dataType;
  private CarbonDimension dimension;
  private String nullformat;
  private boolean isEmptyBadRecord;
  private DataField dataField;
  private String binaryDecoder;
  public BinaryFieldConverterImpl(DataField dataField, String nullformat, int index,
      boolean isEmptyBadRecord,String binaryDecoder) {
    this.dataType = dataField.getColumn().getDataType();
    this.dimension = (CarbonDimension) dataField.getColumn();
    this.nullformat = nullformat;
    this.index = index;
    this.isEmptyBadRecord = isEmptyBadRecord;
    this.dataField = dataField;
    this.binaryDecoder = binaryDecoder;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    row.update(convert(row.getObject(index), logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder)
      throws RuntimeException {
    if (value instanceof String) {
      if (binaryDecoder.equalsIgnoreCase(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_BASE64)) {
        byte[] parsedValue = (String.valueOf(value))
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        if (Base64.isArrayByteBase64(parsedValue)) {
          parsedValue = Base64.decodeBase64(parsedValue);
        } else {
          throw new CarbonDataLoadingException("Binary decoder is " +
              CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_BASE64
              + ", but data is not base64");
        }
        return parsedValue;
      } else if (binaryDecoder.equalsIgnoreCase(
          CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_HEX)) {
        try {
          return Hex.decodeHex(((String) value).toCharArray());
        } catch (DecoderException e) {
          throw new CarbonDataLoadingException("Binary decode hex String failed,", e);
        }
      } else if (!StringUtils.isBlank(binaryDecoder)) {
        throw new CarbonDataLoadingException("Binary decoder only support Base64, " +
            "Hex or no decode for string, don't support " + binaryDecoder);
      } else {
        return ((String) value)
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      }
    } else if (value instanceof byte[]) {
      return value;
    } else {
      throw new CarbonDataLoadingException("Binary only support String and byte[] data type," +
          " binary decoder only support Base64, Hex or no decode for string");
    }
  }

  @Override
  public void clear() {
  }
}