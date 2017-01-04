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
package org.apache.carbondata.core.util;

import org.apache.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.apache.carbondata.core.datastorage.store.compression.WriterCompressModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.decimal.UnCompressMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalByte;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal
    .UnCompressNonDecimalDefault;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalFloat;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalInt;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalLong;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinByte;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinDefault;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinFloat;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinInt;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinLong;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalMaxMinShort;
import org.apache.carbondata.core.datastorage.store.compression.nondecimal.UnCompressNonDecimalShort;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneByte;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneDefault;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneFloat;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneInt;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneLong;
import org.apache.carbondata.core.datastorage.store.compression.none.UnCompressNoneShort;

import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.TestCase.*;
import static org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class ValueCompressionUtilTest {

  @Test public void testGetSize() {
    DataType[] dataTypes =
        { DataType.DATA_BIGINT, DataType.DATA_INT, DataType.DATA_BYTE, DataType.DATA_SHORT,
            DataType.DATA_FLOAT };
    int[] expectedSizes = { 8, 4, 1, 2, 4 };
    for (int i = 0; i < dataTypes.length; i++) {
      assertEquals(expectedSizes[i], ValueCompressionUtil.getSize(dataTypes[i]));
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataInt() {
    double[] values = { 25, 12, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_INT, 22, 0);
    int[] expectedResult = { -3, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_BYTE, 22, 0);
    byte[] expectedResult = { 2, 1, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataShort() {
    double[] values = { 200, 21, 22 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_SHORT, 22, 0);
    short[] expectedResult = { -178, 1, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataLong() {
    double[] values = { 2000, 2100, 2002 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_LONG, 2125, 0);
    long[] expectedResult = { 125, 25, 123 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataFloat() {
    double[] values = { 20.121, 21.223, 22.345 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_FLOAT, 22.345, 3);
    float[] expectedResult = { 2.224f, 1.122f, 0f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataDouble() {
    double[] values = { 20.121, 21.223, 22.345 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DATA_DOUBLE, 102.345, 3);
    double[] expectedResult = { 82.224, 81.122, 80.0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForBigInt() {
    double[] values = { 20.121, 21.223, 22.345 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.DATA_BIGINT, 22, 0);
    long[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.DATA_BYTE,
            22, 0);
    byte[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataShort() {
    double[] values = { 200000, 21, 22 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.DATA_SHORT, 22, 0);
    short[] expectedResult = { 3392, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataInt() {
    double[] values = { 20, 21, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.DATA_INT,
            22, 0);
    int[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataLong() {
    double[] values = { 20, 21, 22 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.DATA_LONG,
            22, 0);
    long[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataFloat() {
    double[] values = { 20.121, 21.223, 22.345 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.DATA_FLOAT, 22, 3);
    float[] expectedResult = { 20.121f, 21.223f, 22.345f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataDouble() {
    double[] values = { 20.121, 21.223, 22.345 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.DATA_DOUBLE, 22, 3);
    double[] expectedResult = { 20.121, 21.223, 22.345 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForFloat() {
    double[] values = { 20.1, 21.2, 22.3 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_FLOAT, 22, 1);
    float[] expectedResult = { 201f, 212f, 223f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForByte() {
    double[] values = { 20.1, 21.2, 22.3 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_BYTE, 22, 1);
    byte[] expectedResult = { -55, -44, -33 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForShort() {
    double[] values = { 20.1, 21.2, 22.3 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_SHORT, 22, 1);
    short[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForInt() {
    double[] values = { 20.1, 21.2, 22.3 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_INT, 22, 1);
    int[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForLong() {
    double[] values = { 20.1, 21.2, 22.3 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_LONG, 22, 1);
    long[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForDouble() {
    double[] values = { 20.1, 21.2, 22.3 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DATA_DOUBLE, 22, 1);
    double[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_BYTE, 22, 1);
    byte[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForInt() {
    double[] values = { 20, 21, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_INT, 22, 1);
    int[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForDouble() {
    double[] values = { 20, 21, 22 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_DOUBLE, 22, 1);
    double[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForShort() {
    double[] values = { 20000, 21, 22 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_SHORT, 22, 1);
    short[] expectedResult = { -3172, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForLong() {
    double[] values = { 20, 21, 22 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_LONG, 22, 1);
    long[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForFloat() {
    double[] values = { 20, 21, 22 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DATA_FLOAT, 22, 1);
    float[] expectedResult = { 20f, 10f, 0f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToUnCompressNone() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_BIGINT, DataType.DATA_BIGINT);
    assertEquals(result.getClass(), UnCompressNoneDefault.class);
  }

  @Test public void testToUnCompressNoneForByte() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_BYTE, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneByte.class);
  }

  @Test public void testToUnCompressNoneForLong() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_LONG, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneLong.class);
  }

  @Test public void testToUnCompressNoneForShort() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_SHORT, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneShort.class);
  }

  @Test public void testToUnCompressNoneForFloat() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_FLOAT, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneFloat.class);
  }

  @Test public void testToUnCompressNoneForInt() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_INT, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneInt.class);
  }

  @Test public void testToUnCompressNoneForDouble() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNone(DataType.DATA_DOUBLE, DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNoneDefault.class);
  }

  @Test public void testToUnCompressMaxMinForDouble() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_DOUBLE, null);
    assertEquals(result.getClass(), UnCompressMaxMinDefault.class);
  }

  @Test public void testToUnCompressMaxMinForInt() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_INT, null);
    assertEquals(result.getClass(), UnCompressMaxMinInt.class);
  }

  @Test public void testToUnCompressMaxMinForFloat() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_FLOAT, null);
    assertEquals(result.getClass(), UnCompressMaxMinFloat.class);
  }

  @Test public void testToUnCompressMaxMinForLong() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_LONG, null);
    assertEquals(result.getClass(), UnCompressMaxMinLong.class);
  }

  @Test public void testToUnCompressMaxMinForByte() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_BYTE, null);
    assertEquals(result.getClass(), UnCompressMaxMinByte.class);
  }

  @Test public void testToUnCompressMaxMinForShort() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressDecimalMaxMin(DataType.DATA_SHORT, null);
    assertEquals(result.getClass(), UnCompressMaxMinShort.class);
  }

  @Test public void testToUnCompressNonDecimalForDouble() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_DOUBLE);
    assertEquals(result.getClass(), UnCompressNonDecimalDefault.class);
  }

  @Test public void testToUnCompressNonDecimalForInt() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_INT);
    assertEquals(result.getClass(), UnCompressNonDecimalInt.class);
  }

  @Test public void testToUnCompressNonDecimalForFloat() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNonDecimalFloat.class);
  }

  @Test public void testToUnCompressNonDecimalForLong() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_LONG);
    assertEquals(result.getClass(), UnCompressNonDecimalLong.class);
  }

  @Test public void testToUnCompressNonDecimalForByte() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_BYTE);
    assertEquals(result.getClass(), UnCompressNonDecimalByte.class);
  }

  @Test public void testToUnCompressNonDecimalForShort() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimal(DataType.DATA_SHORT);
    assertEquals(result.getClass(), UnCompressNonDecimalShort.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForDouble() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_DOUBLE);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinDefault.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForInt() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_INT);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinInt.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForFloat() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_FLOAT);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinFloat.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForLong() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_LONG);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinLong.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForByte() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_BYTE);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinByte.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForShort() {
    ValueCompressonHolder.UnCompressValue result =
        ValueCompressionUtil.getUnCompressNonDecimalMaxMin(DataType.DATA_SHORT);
    assertEquals(result.getClass(), UnCompressNonDecimalMaxMinShort.class);
  }

  @Test public void testToConvertToBytesForInt() {
    int[] input = { 120000, 200000, 300000 };
    byte[] result = ValueCompressionUtil.convertToBytes(input);
    byte[] expectedResult = { 0, 1, -44 };
    for (int i = 0; i < input.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToInt() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(123);
    int[] result = ValueCompressionUtil.convertToIntArray(byteBuffer, 4);
    int[] expectedResult = { 123 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToBytesForFloat() {
    float[] input = { 120.001f, 200.012f, 300.123f };
    byte[] result = ValueCompressionUtil.convertToBytes(input);
    byte[] expectedResult = { 66, -16, 0, -125, 67, 72, 3, 18, 67, -106, 15, -66 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToFloat() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putFloat(123.23f);
    float[] result = ValueCompressionUtil.convertToFloatArray(byteBuffer, 4);
    float[] expectedResult = { 123.23f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToBytesForShort() {
    short[] input = { 20000, -30000, 23 };
    byte[] result = ValueCompressionUtil.convertToBytes(input);
    byte[] expectedResult = { 78, 32, -118, -48, 0, 23 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToShort() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2);
    byteBuffer.putShort((short) 3);
    short[] result = ValueCompressionUtil.convertToShortArray(byteBuffer, 2);
    short[] expectedResult = { 3 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToBytesForLong() {
    long[] input = { 2000000, 300654 };
    byte[] result = ValueCompressionUtil.convertToBytes(input);
    byte[] expectedResult = { 0, 0, 0, 0, 0, 30, -124, -128, 0, 0, 0, 0, 0, 4, -106, 110 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToLong() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(321654987);
    long[] result = ValueCompressionUtil.convertToLongArray(byteBuffer, 8);
    long[] expectedResult = { 321654987 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToBytesForDouble() {
    double[] input = { 20.236, 30.001 };
    byte[] result = ValueCompressionUtil.convertToBytes(input);
    byte[] expectedResult = { 64, 52, 60, 106, 126, -7, -37, 35, 64, 62, 0, 65, -119, 55, 75, -57 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToConvertToDouble() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putDouble(3216.54987);
    double[] result = ValueCompressionUtil.convertToDoubleArray(byteBuffer, 8);
    double[] expectedResult = { 3216.54987 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i]);
    }
  }

  @Test public void testToGetValueCompressionModel() {
    Object[] maxValues = { 10l, 20l, 30l };
    Object[] minValues = { 1l, 2l, 3l };
    int[] decimalLength = { 0, 0, 0 };
    Object[] uniqueValues = { 5, new Long[]{2l,4l}, 2l };
    char[] types = { 'l', 'l', 'l' };
    byte[] dataTypeSelected = { 1, 2, 4 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 3, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[1].getCompType());
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[2].getCompType());
  }

  @Test public void testToGetValueCompressionModelForDefaultAggregatorType() {
    Object[] maxValues = { 10.0 };
    Object[] minValues = { 1.0 };
    int[] decimalLength = { 0 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForShortAndByte() {
    Object[] maxValues = { 32600.00 };
    Object[] minValues = { 32500.00 };
    int[] decimalLength = { 0 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForIntAndShort() {
    Object[] maxValues = { 1111111111.0 };
    Object[] minValues = { 1111078433.0 };
    int[] decimalLength = { 0 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForByteAndInt() {
    Object[] maxValues = { -32766.00 };
    Object[] minValues = { 32744.0 };
    int[] decimalLength = { 0 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForByteAndIntAndDecimal1() {
    Object[] maxValues = { -32766.00 };
    Object[] minValues = { 32744.0 };
    int[] decimalLength = { 1 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForByteAndIntAndDataTypeSelected0() {
    Object[] maxValues = { -32766.00 };
    Object[] minValues = { 32744.0 };
    int[] decimalLength = { 1 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 0 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

  @Test public void testToGetValueCompressionModelForFloatAndDataTypeSelected1() {
    Object[] maxValues = { 32725566.00 };
    Object[] minValues = { 32744.0 };
    int[] decimalLength = { 1 };
    Object[] uniqueValues = { 5 };
    char[] types = { 'n' };
    byte[] dataTypeSelected = { 1 };
    MeasureMetaDataModel measureMetaDataModel =
        new MeasureMetaDataModel(maxValues, minValues, decimalLength, 1, uniqueValues, types,
            dataTypeSelected);
    WriterCompressModel writerCompressModel =
        ValueCompressionUtil.getWriterCompressModel(measureMetaDataModel);
    assertEquals(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE,
        writerCompressModel.getCompressionFinder()[0].getCompType());
  }

}
