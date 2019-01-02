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

package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

public class DataTypeConverterImpl implements DataTypeConverter, Serializable {

  private static final long serialVersionUID = -1718154403432354200L;

  @Override
  public Object convertFromStringToDecimal(Object data) {
    if (null == data) {
      return null;
    }
    if (data instanceof BigDecimal) {
      return data;
    }
    return new BigDecimal(data.toString());
  }

  @Override
  public Object convertFromBigDecimalToDecimal(Object data) {
    if (null == data) {
      return null;
    }
    if (data instanceof BigDecimal) {
      return data;
    }
    return new BigDecimal(data.toString());
  }

  @Override public Object convertFromDecimalToBigDecimal(Object data) {
    return convertFromBigDecimalToDecimal(data);
  }

  @Override
  public Object convertFromByteToUTF8String(byte[] data) {
    if (null == data) {
      return null;
    }
    return new String(data, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
  }

  @Override
  public byte[] convertFromByteToUTF8Bytes(byte[] data) {
    return data;
  }

  @Override
  public byte[] convertFromStringToByte(Object data) {
    if (null == data) {
      return null;
    }
    return data.toString().getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
  }

  @Override
  public Object convertFromStringToUTF8String(Object data) {
    if (null == data) {
      return null;
    }
    return data.toString();
  }

  @Override
  public Object wrapWithGenericArrayData(Object data) {
    return data;
  }

  @Override
  public Object[] unwrapGenericRowToObject(Object data) {
    Object[] splitData = (Object[]) data;
    return splitData;
  }

  @Override
  public Object wrapWithGenericRow(Object[] fields) {
    return fields;
  }

  @Override
  public Object wrapWithArrayBasedMapData(Object[] keyArray, Object[] valueArray) {
    return new Object[] { keyArray, valueArray };
  }

  @Override
  public Object[] convertCarbonSchemaToSparkSchema(CarbonColumn[] carbonColumns) {
    throw new UnsupportedOperationException();
  }
}
