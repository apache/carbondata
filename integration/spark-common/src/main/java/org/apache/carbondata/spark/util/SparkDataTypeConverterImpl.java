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

package org.apache.carbondata.spark.util;

import java.io.Serializable;

import org.apache.carbondata.core.util.DataTypeConverter;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Convert java data type to spark data type
 */
public final class SparkDataTypeConverterImpl implements DataTypeConverter, Serializable {

  private static final long serialVersionUID = -4379212832935070583L;

  @Override
  public Object convertToDecimal(Object data) {
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data.toString());
    return org.apache.spark.sql.types.Decimal.apply(javaDecVal);
  }

  @Override
  public Object convertToBigDecimal(Object data) {
    return ((org.apache.spark.sql.types.Decimal) data).toJavaBigDecimal();
  }

  @Override
  public byte[] convertFromStringToByte(Object data) {
    return UTF8String.fromString((String) data).getBytes();
  }

  @Override
  public Object convertFromByteToUTF8String(Object data) {
    return UTF8String.fromBytes((byte[]) data);
  }

  @Override
  public byte[] convertFromByteToUTF8Bytes(byte[] data) {
    return UTF8String.fromBytes(data).getBytes();
  }

  @Override
  public Object convertFromStringToUTF8String(Object data) {
    return UTF8String.fromString((String) data);
  }

  @Override
  public Object wrapWithGenericArrayData(Object data) {
    return new GenericArrayData(data);
  }

  @Override
  public Object wrapWithGenericRow(Object[] fields) {
    return new GenericInternalRow(fields);
  }
}
