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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;

/**
 * This is the utility class for No Dictionary changes.
 */
public class NonDictionaryUtil {

  /**
   * This method will form one single byte [] for all the high card dims.
   * For example if you need to pack 2 columns c1 and c2 , it stores in following way
   *  <total_len(short)><offsetLen(short)><offsetLen+c1_len(short)><c1(byte[])><c2(byte[])>
   * @param byteBufferArr
   * @return
   */
  public static byte[] packByteBufferIntoSingleByteArray(byte[][] byteBufferArr) {
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
      int noOfBytes = byteBufferArr[index].length;

      buffer.putShort((short) (offsetLen + noOfBytes));
      offsetLen += noOfBytes;
    }

    // put actual data.
    for (int index = 0; index < byteBufferArr.length; index++) {
      buffer.put(byteBufferArr[index]);
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
  private static int calculateTotalBytes(byte[][] byteBufferArr) {
    int total = 0;
    for (int index = 0; index < byteBufferArr.length; index++) {
      total += byteBufferArr[index].length;
    }
    return total;
  }

  /**
   * Method to get the required dictionary Dimension from obj []
   *
   * @param index
   * @param row
   * @return
   */
  public static int getDictDimension(int index, Object[] row) {
    int[] dimensions = (int[]) row[WriteStepRowUtil.DICTIONARY_DIMENSION];
    return dimensions[index];
  }

  /**
   * Method to get the required non-dictionary & complex from 3-parted row
   * @param index
   * @param row
   * @return
   */
  public static byte[] getNoDictOrComplex(int index, Object[] row) {
    byte[][] nonDictArray = (byte[][]) row[WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX];
    return nonDictArray[index];
  }

  /**
   * Method to get the required measure from obj []
   *
   * @param index
   * @param row
   * @return
   */
  public static Object getMeasure(int index, Object[] row) {
    Object[] measures = (Object[]) row[WriteStepRowUtil.MEASURE];
    return measures[index];
  }

  public static void prepareOutObj(Object[] out, int[] dimArray, byte[][] byteBufferArr,
      Object[] measureArray) {
    out[WriteStepRowUtil.DICTIONARY_DIMENSION] = dimArray;
    out[WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX] = byteBufferArr;
    out[WriteStepRowUtil.MEASURE] = measureArray;

  }
}
