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

import junit.framework.TestCase;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * This test will test the functionality of the Byte Util
 * for the comparision of 2 byte buffers
 */
public class ByteUtilTest extends TestCase {

  String dimensionValue1 = "aaaaaaaa1235";
  String dimensionValue2 = "aaaaaaaa1234";
  private ByteBuffer buff1;
  private ByteBuffer buff2;

  /**
   * This method will form one single byte [] for all the high card dims.
   *
   * @param byteBufferArr ByteBuffer array
   * @return byte array
   */
  public static byte[] packByteBufferIntoSingleByteArray(ByteBuffer[] byteBufferArr) {
    // for empty array means there is no data to remove dictionary.
    if (null == byteBufferArr || byteBufferArr.length == 0) {
      return null;
    }
    int noOfCol = byteBufferArr.length;
    short toDetermineLengthOfByteArr = 2;
    short offsetLen = (short) (noOfCol * 2 + toDetermineLengthOfByteArr);
    int totalBytes = calculateTotalBytes(byteBufferArr) + offsetLen;

    ByteBuffer buffer = ByteBuffer.allocate(totalBytes);

    // write the length of the byte [] as first short
    buffer.putShort((short) (totalBytes - toDetermineLengthOfByteArr));
    // writing the offset of the first element.
    buffer.putShort(offsetLen);

    // prepare index for byte []
    for (int index = 0; index < byteBufferArr.length - 1; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      // short lengthOfbytes = individualCol.getShort();
      int noOfBytes = individualCol.capacity();

      buffer.putShort((short) (offsetLen + noOfBytes));
      offsetLen += noOfBytes;
      individualCol.rewind();
    }

    // put actual data.
    for (int index = 0; index < byteBufferArr.length; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      buffer.put(individualCol.array());
    }

    buffer.rewind();
    return buffer.array();

  }

  /**
   * To calculate the total bytes in byte Buffer[].
   *
   * @param byteBufferArr
   * @return
   */
  private static int calculateTotalBytes(ByteBuffer[] byteBufferArr) {
    int total = 0;
    for (int index = 0; index < byteBufferArr.length; index++) {
      total += byteBufferArr[index].capacity();
    }
    return total;
  }

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void testLessThan() {
    dimensionValue1 = "aaaaa6aa1235";
    dimensionValue2 = "aaaaa5aa1234";

    prepareBuffers();
    assertFalse(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) < 0);
  }

  @Test
  public void testIntConversion() {
    byte[] data = new byte[4];
    ByteUtil.setInt(data, 0, 968);
    assertEquals(ByteUtil.toInt(data, 0), 968);
  }

  @Test
  public void testEqualToCase() {
    dimensionValue1 = "aaaaaaaa1234";
    dimensionValue2 = "aaaaaaaa1234";

    prepareBuffers();
    assertTrue(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) == 0);
  }

  @Test
  public void testLessThanInBoundaryCondition() {
    dimensionValue1 = "aaaaaaaa12341";
    dimensionValue2 = "aaaaaaaa12344";

    prepareBuffers();
    assertTrue(UnsafeComparer.INSTANCE.compareTo(buff1, buff2) < 0);
  }

  /**
   * This will prepare the byte buffers in the required format for comparision.
   */
  private void prepareBuffers() {
    ByteBuffer[] out1 = new ByteBuffer[1];
    ByteBuffer buffer = ByteBuffer.allocate(dimensionValue1.length());
    buffer.put(dimensionValue1.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    buffer.rewind();
    out1[0] = buffer;

    ByteBuffer[] out2 = new ByteBuffer[1];

    ByteBuffer buffer2 = ByteBuffer.allocate(dimensionValue2.length());
    buffer2.put(dimensionValue2.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    buffer2.rewind();
    out2[0] = buffer2;

    byte[] arr1 = packByteBufferIntoSingleByteArray(out1);
    byte[] arr2 = packByteBufferIntoSingleByteArray(out2);

    buff1 = ByteBuffer.wrap(arr1);

    buff1.position(4);
    buff1.limit(buff1.position() + dimensionValue1.length());

    buff2 = ByteBuffer.wrap(arr2);
    buff2.position(4);
    buff2.limit(buff2.position() + dimensionValue2.length());
  }

  @Test
  public void testToBytes() {
    assertTrue(ByteUtil.toBoolean(ByteUtil.toBytes(true)));
    assertFalse(ByteUtil.toBoolean(ByteUtil.toBytes(false)));

    assertEquals(Short.MAX_VALUE,
        ByteUtil.toShort(ByteUtil.toBytes(Short.MAX_VALUE), 0, 2));
    assertEquals((short) (Short.MAX_VALUE / 2),
        ByteUtil.toShort(ByteUtil.toBytes((short) (Short.MAX_VALUE / 2)), 0, 2));
    assertEquals((short) 0,
        ByteUtil.toShort(ByteUtil.toBytes((short) 0), 0, 2));
    assertEquals((short) (Short.MIN_VALUE / 2),
        ByteUtil.toShort(ByteUtil.toBytes((short) (Short.MIN_VALUE / 2)), 0, 2));
    assertEquals(Short.MIN_VALUE,
        ByteUtil.toShort(ByteUtil.toBytes(Short.MIN_VALUE), 0, 2));

    assertEquals(Integer.MAX_VALUE,
        ByteUtil.toInt(ByteUtil.toBytes(Integer.MAX_VALUE), 0, 4));
    assertEquals(Integer.MAX_VALUE / 2,
        ByteUtil.toInt(ByteUtil.toBytes(Integer.MAX_VALUE / 2), 0, 4));
    assertEquals(0,
        ByteUtil.toInt(ByteUtil.toBytes(0), 0, 4));
    assertEquals(Integer.MIN_VALUE / 2,
        ByteUtil.toInt(ByteUtil.toBytes(Integer.MIN_VALUE / 2), 0, 4));
    assertEquals(Integer.MIN_VALUE,
        ByteUtil.toInt(ByteUtil.toBytes(Integer.MIN_VALUE), 0, 4));

    assertEquals(Long.MAX_VALUE,
        ByteUtil.toLong(ByteUtil.toBytes(Long.MAX_VALUE), 0, 8));
    assertEquals(Long.MAX_VALUE / 2,
        ByteUtil.toLong(ByteUtil.toBytes(Long.MAX_VALUE / 2), 0, 8));
    assertEquals(0L,
        ByteUtil.toLong(ByteUtil.toBytes(0L), 0, 8));
    assertEquals(Long.MIN_VALUE / 2,
        ByteUtil.toLong(ByteUtil.toBytes(Long.MIN_VALUE / 2), 0, 8));
    assertEquals(Long.MIN_VALUE,
        ByteUtil.toLong(ByteUtil.toBytes(Long.MIN_VALUE), 0, 8));

    assertEquals(Double.MAX_VALUE,
        ByteUtil.toDouble(ByteUtil.toBytes(Double.MAX_VALUE), 0, 8));
    assertEquals(Double.MAX_VALUE / 2,
        ByteUtil.toDouble(ByteUtil.toBytes(Double.MAX_VALUE / 2), 0, 8));
    assertEquals((double) 0,
        ByteUtil.toDouble(ByteUtil.toBytes((double) 0), 0, 8));
    assertEquals(Double.MIN_VALUE / 2,
        ByteUtil.toDouble(ByteUtil.toBytes(Double.MIN_VALUE / 2), 0, 8));
    assertEquals(Double.MIN_VALUE,
        ByteUtil.toDouble(ByteUtil.toBytes(Double.MIN_VALUE), 0, 8));
  }

  @Test
  public void testToXorBytes() {
    assertEquals(Short.MAX_VALUE,
        ByteUtil.toXorShort(ByteUtil.toXorBytes(Short.MAX_VALUE), 0, 2));
    assertEquals((short) (Short.MAX_VALUE / 2),
        ByteUtil.toXorShort(ByteUtil.toXorBytes((short) (Short.MAX_VALUE / 2)), 0, 2));
    assertEquals((short) 0,
        ByteUtil.toXorShort(ByteUtil.toXorBytes((short) 0), 0, 2));
    assertEquals((short) (Short.MIN_VALUE / 2),
        ByteUtil.toXorShort(ByteUtil.toXorBytes((short) (Short.MIN_VALUE / 2)), 0, 2));
    assertEquals(Short.MIN_VALUE,
        ByteUtil.toXorShort(ByteUtil.toXorBytes(Short.MIN_VALUE), 0, 2));

    assertEquals(Integer.MAX_VALUE,
        ByteUtil.toXorInt(ByteUtil.toXorBytes(Integer.MAX_VALUE), 0, 4));
    assertEquals(Integer.MAX_VALUE / 2,
        ByteUtil.toXorInt(ByteUtil.toXorBytes(Integer.MAX_VALUE / 2), 0, 4));
    assertEquals(0,
        ByteUtil.toXorInt(ByteUtil.toXorBytes(0), 0, 4));
    assertEquals(Integer.MIN_VALUE / 2,
        ByteUtil.toXorInt(ByteUtil.toXorBytes(Integer.MIN_VALUE / 2), 0, 4));
    assertEquals(Integer.MIN_VALUE,
        ByteUtil.toXorInt(ByteUtil.toXorBytes(Integer.MIN_VALUE), 0, 4));

    assertEquals(Long.MAX_VALUE,
        ByteUtil.toXorLong(ByteUtil.toXorBytes(Long.MAX_VALUE), 0, 8));
    assertEquals(Long.MAX_VALUE / 2,
        ByteUtil.toXorLong(ByteUtil.toXorBytes(Long.MAX_VALUE / 2), 0, 8));
    assertEquals(0L,
        ByteUtil.toXorLong(ByteUtil.toXorBytes(0L), 0, 8));
    assertEquals(Long.MIN_VALUE / 2,
        ByteUtil.toXorLong(ByteUtil.toXorBytes(Long.MIN_VALUE / 2), 0, 8));
    assertEquals(Long.MIN_VALUE,
        ByteUtil.toXorLong(ByteUtil.toXorBytes(Long.MIN_VALUE), 0, 8));

    assertEquals(Double.MAX_VALUE,
        ByteUtil.toXorDouble(ByteUtil.toXorBytes(Double.MAX_VALUE), 0, 8));
    assertEquals(Double.MAX_VALUE / 2,
        ByteUtil.toXorDouble(ByteUtil.toXorBytes(Double.MAX_VALUE / 2), 0, 8));
    assertEquals((double) 0,
        ByteUtil.toXorDouble(ByteUtil.toXorBytes((double) 0), 0, 8));
    assertEquals(Double.MIN_VALUE / 2,
        ByteUtil.toXorDouble(ByteUtil.toXorBytes(Double.MIN_VALUE / 2), 0, 8));
    assertEquals(Double.MIN_VALUE,
        ByteUtil.toXorDouble(ByteUtil.toXorBytes(Double.MIN_VALUE), 0, 8));
  }

}
