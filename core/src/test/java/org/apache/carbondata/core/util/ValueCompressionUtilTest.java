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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.decimal.*;
import org.apache.carbondata.core.datastore.compression.nondecimal.*;
import org.apache.carbondata.core.datastore.compression.none.*;
import org.apache.carbondata.core.metadata.datatype.DataType;

import org.junit.Test;


public class ValueCompressionUtilTest {

  @Test public void testGetSize() {
    DataType[] dataTypes =
        { DataType.LONG, DataType.INT, DataType.BOOLEAN, DataType.SHORT,
            DataType.FLOAT };
    int[] expectedSizes = { 8, 4, 1, 2, 4 };
    for (int i = 0; i < dataTypes.length; i++) {
      assertEquals(expectedSizes[i], ValueCompressionUtil.getSize(dataTypes[i]));
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataInt() {
    double[] values = { 25, 12, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.INT, 22, 0);
    int[] expectedResult = { -3, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.BYTE, 22, 0);
    byte[] expectedResult = { 2, 1, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataShort() {
    double[] values = { 200, 21, 22 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.SHORT, 22, 0);
    short[] expectedResult = { -178, 1, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataLong() {
    double[] values = { 2000, 2100, 2002 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.LONG, 2125, 0);
    long[] expectedResult = { 125, 25, 123 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataFloat() {
    double[] values = { 20.121, 21.223, 22.345 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.FLOAT, 22.345, 3);
    float[] expectedResult = { 2.224f, 1.122f, 0f };
    for (int i = 0; i < result.length; i++) {
    	assertTrue(result[i]-expectedResult[i]==0);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMin_MaxForDataDouble() {
    double[] values = { 20.121, 21.223, 22.345 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_DOUBLE, values,
            DataType.DOUBLE, 102.345, 3);
    double[] expectedResult = { 82.224, 81.122, 80.0 };
    for (int i = 0; i < result.length; i++) {
      assertTrue(result[i]-expectedResult[i]==0);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForBigInt() {
    double[] values = { 20.121, 21.223, 22.345 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.LONG, 22, 0);
    long[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.BYTE,
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
            DataType.SHORT, 22, 0);
    short[] expectedResult = { 3392, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataInt() {
    double[] values = { 20, 21, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.INT,
            22, 0);
    int[] expectedResult = { 20, 21, 22 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataLong() {
    double[] values = { 20, 21, 22 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values, DataType.LONG,
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
            DataType.FLOAT, 22, 3);
    float[] expectedResult = { 20.121f, 21.223f, 22.345f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],3);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNoneForDataDouble() {
    double[] values = { 20.121, 21.223, 22.345 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.ADAPTIVE, values,
            DataType.DOUBLE, 22, 3);
    double[] expectedResult = { 20.121, 21.223, 22.345 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],3);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForFloat() {
    double[] values = { 20.1, 21.2, 22.3 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.FLOAT, 22, 1);
    float[] expectedResult = { 201f, 212f, 223f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],0);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForByte() {
    double[] values = { 20.1, 21.2, 22.3 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.BYTE, 22, 1);
    byte[] expectedResult = { -55, -44, -33 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForShort() {
    double[] values = { 20.1, 21.2, 22.3 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.SHORT, 22, 1);
    short[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForInt() {
    double[] values = { 20.1, 21.2, 22.3 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.INT, 22, 1);
    int[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForLong() {
    double[] values = { 20.1, 21.2, 22.3 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.LONG, 22, 1);
    long[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeNon_DecimalForDouble() {
    double[] values = { 20.1, 21.2, 22.3 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.BIGINT, values,
            DataType.DOUBLE, 22, 1);
    double[] expectedResult = { 201, 212, 223 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],0);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForByte() {
    double[] values = { 20, 21, 22 };
    byte[] result = (byte[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.BYTE, 22, 1);
    byte[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForInt() {
    double[] values = { 20, 21, 22 };
    int[] result = (int[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.INT, 22, 1);
    int[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForDouble() {
    double[] values = { 20, 21, 22 };
    double[] result = (double[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.DOUBLE, 22, 1);
    double[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],0);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForShort() {
    double[] values = { 20000, 21, 22 };
    short[] result = (short[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.SHORT, 22, 1);
    short[] expectedResult = { -3172, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForLong() {
    double[] values = { 20, 21, 22 };
    long[] result = (long[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.LONG, 22, 1);
    long[] expectedResult = { 20, 10, 0 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i]);
    }
  }

  @Test public void testToGetCompressedValuesWithCompressionTypeMax_Min_NDCForFloat() {
    double[] values = { 20, 21, 22 };
    float[] result = (float[]) ValueCompressionUtil
        .getCompressedValues(ValueCompressionUtil.COMPRESSION_TYPE.DELTA_NON_DECIMAL, values,
            DataType.FLOAT, 22, 1);
    float[] expectedResult = { 20f, 10f, 0f };
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], expectedResult[i],0);
    }
  }

  @Test public void testToUnCompressNone() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.LONG, DataType.LONG);
    assertEquals(result.getClass(), CompressionNoneLong.class);
  }

  @Test public void testToUnCompressNoneForByte() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.BYTE, DataType.FLOAT);
    assertEquals(result.getClass(), CompressionNoneByte.class);
  }

  @Test public void testToUnCompressNoneForLong() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.LONG, DataType.FLOAT);
    assertEquals(result.getClass(), CompressionNoneLong.class);
  }

  @Test public void testToUnCompressNoneForShort() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.SHORT, DataType.FLOAT);
    assertEquals(result.getClass(), CompressionNoneShort.class);
  }

  @Test public void testToUnCompressNoneForInt() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.INT, DataType.FLOAT);
    assertEquals(result.getClass(), CompressionNoneInt.class);
  }

  @Test public void testToUnCompressNoneForDouble() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNone(DataType.DOUBLE, DataType.FLOAT);
    assertEquals(result.getClass(), CompressionNoneDefault.class);
  }

  @Test public void testToUnCompressMaxMinForDouble() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionDecimalMaxMin(DataType.DOUBLE, null);
    assertEquals(result.getClass(), CompressionMaxMinDefault.class);
  }

  @Test public void testToUnCompressMaxMinForInt() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionDecimalMaxMin(DataType.INT, null);
    assertEquals(result.getClass(), CompressionMaxMinInt.class);
  }

  @Test public void testToUnCompressMaxMinForLong() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionDecimalMaxMin(DataType.LONG, null);
    assertEquals(result.getClass(), CompressionMaxMinLong.class);
  }

  @Test public void testToUnCompressMaxMinForByte() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionDecimalMaxMin(DataType.BYTE, null);
    assertEquals(result.getClass(), CompressionMaxMinByte.class);
  }

  @Test public void testToUnCompressMaxMinForShort() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionDecimalMaxMin(DataType.SHORT, null);
    assertEquals(result.getClass(), CompressionMaxMinShort.class);
  }

  @Test public void testToUnCompressNonDecimalForDouble() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimal(DataType.DOUBLE);
    assertEquals(result.getClass(), CompressionNonDecimalDefault.class);
  }

  @Test public void testToUnCompressNonDecimalForInt() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimal(DataType.INT);
    assertEquals(result.getClass(), CompressionNonDecimalInt.class);
  }

  @Test public void testToUnCompressNonDecimalForLong() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimal(DataType.LONG);
    assertEquals(result.getClass(), CompressionNonDecimalLong.class);
  }

  @Test public void testToUnCompressNonDecimalForByte() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimal(DataType.BYTE);
    assertEquals(result.getClass(), CompressionNonDecimalByte.class);
  }

  @Test public void testToUnCompressNonDecimalForShort() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimal(DataType.SHORT);
    assertEquals(result.getClass(), CompressionNonDecimalShort.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForDouble() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimalMaxMin(DataType.DOUBLE);
    assertEquals(result.getClass(), CompressionNonDecimalMaxMinDefault.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForInt() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimalMaxMin(DataType.INT);
    assertEquals(result.getClass(), CompressionNonDecimalMaxMinInt.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForLong() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimalMaxMin(DataType.LONG);
    assertEquals(result.getClass(), CompressionNonDecimalMaxMinLong.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForByte() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimalMaxMin(DataType.BYTE);
    assertEquals(result.getClass(), CompressionNonDecimalMaxMinByte.class);
  }

  @Test public void testToUnCompressNonDecimalMaxMinForShort() {
    ValueCompressionHolder result =
        ValueCompressionUtil.getCompressionNonDecimalMaxMin(DataType.SHORT);
    assertEquals(result.getClass(), CompressionNonDecimalMaxMinShort.class);
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

  @Test public void testToConvertToShort() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(2);
    byteBuffer.putShort((short) 3);
    short[] result = ValueCompressionUtil.convertToShortArray(byteBuffer, 2);
    short[] expectedResult = { 3 };
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

  @Test public void testToConvertToDouble() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putDouble(3216.54987);
    double[] result = ValueCompressionUtil.convertToDoubleArray(byteBuffer, 8);
    double[] expectedResult = { 3216.54987 };
    for (int i = 0; i < result.length; i++) {
      assertEquals(expectedResult[i], result[i],5);
    }
  }

}
