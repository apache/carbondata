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

/**
 *
 */
package org.apache.carbondata.processing.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.IgnoreDictionary;

/**
 * This is the utility class for No Dictionary changes.
 */
public class NonDictionaryUtil {
  /**
   * Here we are dividing one single object [] into 3 arrays. one for
   * dimensions , one for high card, one for measures.
   *
   * @param out
   * @param byteBufferArr
   */
  public static void prepareOut(Object[] newOutArr, ByteBuffer[] byteBufferArr, Object[] out,
      int dimCount) {

    byte[] nonDictionaryCols =
        NonDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArr);
    Integer[] dimArray = new Integer[dimCount];
    for (int i = 0; i < dimCount; i++) {
      dimArray[i] = (Integer) out[i];
    }

    Object[] measureArray = new Object[out.length - dimCount];
    int index = 0;
    for (int j = dimCount; j < out.length; j++) {
      measureArray[index++] = out[j];
    }

    newOutArr[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()] = dimArray;
    newOutArr[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()] = nonDictionaryCols;
    newOutArr[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()] = measureArray;

  }

  /**
   * This method will form one single byte [] for all the high card dims.
   *
   * @param byteBufferArr
   * @return
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
   * Method to check whether entire row is empty or not.
   *
   * @param row
   * @return
   */
  public static boolean checkAllValuesForNull(Object[] row) {
    if (checkNullForDims(row[0]) && checkNullForMeasures(row[2]) && null == row[1]) {
      return true;
    }
    return false;
  }

  /**
   * To check whether the measures are empty/null
   *
   * @param object
   * @return
   */
  private static boolean checkNullForMeasures(Object object) {
    Object[] measures = (Object[]) object;
    for (Object measure : measures) {
      if (null != measure) {
        return false;
      }
    }
    return true;
  }

  /**
   * To check whether the dimensions are empty/null
   *
   * @param object
   * @return
   */
  private static boolean checkNullForDims(Object object) {
    Integer[] dimensions = (Integer[]) object;
    for (Integer dimension : dimensions) {
      if (null != dimension) {
        return false;
      }
    }
    return true;
  }

  /**
   * Method to get the required Dimension from obj []
   *
   * @param index
   * @param row
   * @return
   */
  public static Integer getDimension(int index, Object[] row) {

    Integer[] dimensions = (Integer[]) row[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()];

    return dimensions[index];

  }

  /**
   * Method to get the required measure from obj []
   *
   * @param index
   * @param row
   * @return
   */
  public static Object getMeasure(int index, Object[] row) {
    Object[] measures = (Object[]) row[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()];
    return measures[index];
  }

  public static byte[] getByteArrayForNoDictionaryCols(Object[] row) {

    return (byte[]) row[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()];
  }

  public static void prepareOutObj(Object[] out, Integer[] dimArray, byte[] byteBufferArr,
      Object[] measureArray) {

    out[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()] = dimArray;
    out[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()] = byteBufferArr;
    out[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()] = measureArray;

  }

  public static void prepareOutObj(Object[] out, int[] dimArray, byte[][] byteBufferArr,
      Object[] measureArray) {

    out[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()] = dimArray;
    out[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()] = byteBufferArr;
    out[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()] = measureArray;

  }

  /**
   * This will extract the high cardinality count from the string.
   */
  public static int extractNoDictionaryCount(String noDictionaryDim) {
    return extractNoDictionaryDimsArr(noDictionaryDim).length;
  }

  /**
   * This method will split one single byte array of high card dims into array
   * of byte arrays.
   *
   * @param NoDictionaryArr
   * @param NoDictionaryCount
   * @return
   */
  public static byte[][] splitNoDictionaryKey(byte[] NoDictionaryArr, int NoDictionaryCount) {
    byte[][] split = new byte[NoDictionaryCount][];

    ByteBuffer buff = ByteBuffer.wrap(NoDictionaryArr, 2, NoDictionaryCount * 2);

    int remainingCol = NoDictionaryCount;
    short secIndex = 0;
    short firstIndex = 0;
    for (int i = 0; i < NoDictionaryCount; i++) {

      if (remainingCol == 1) {
        firstIndex = buff.getShort();
        int length = NoDictionaryArr.length - firstIndex;

        // add 2 bytes (short) as length required to determine size of
        // each column value.

        split[i] = new byte[length + 2];
        ByteBuffer splittedCol = ByteBuffer.wrap(split[i]);
        splittedCol.putShort((short) length);

        System.arraycopy(NoDictionaryArr, firstIndex, split[i], 2, length);

      } else {

        firstIndex = buff.getShort();
        secIndex = buff.getShort();
        int length = secIndex - firstIndex;

        // add 2 bytes (short) as length required to determine size of
        // each column value.

        split[i] = new byte[length + 2];
        ByteBuffer splittedCol = ByteBuffer.wrap(split[i]);
        splittedCol.putShort((short) length);

        System.arraycopy(NoDictionaryArr, firstIndex, split[i], 2, length);
        buff.position(buff.position() - 2);

      }
      remainingCol--;
    }

    return split;
  }

  /**
   * @param index
   * @param val
   */
  public static void setDimension(int index, int val, Object[] objArr) {
    Integer[] dimensions = (Integer[]) objArr[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()];

    dimensions[index] = val;

  }

  /**
   * This will extract the high cardinality count from the string.
   */
  public static String[] extractNoDictionaryDimsArr(String noDictionaryDim) {

    if (null == noDictionaryDim || noDictionaryDim.isEmpty()) {
      return new String[0];
    }

    String[] NoDictionary = noDictionaryDim.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    List<String> list1 = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 0; i < NoDictionary.length; i++) {
      String[] dim = NoDictionary[i].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      list1.add(dim[0]);
    }

    return list1.toArray(new String[list1.size()]);
  }
  /**
   * This will extract the high cardinality count from the string.
   */
  public static Map<String, String> extractDimColsDataTypeValues(String colDataTypes) {
    Map<String, String> mapOfColNameDataType =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    if (null == colDataTypes || colDataTypes.isEmpty()) {
      return mapOfColNameDataType;
    }
    String[] colArray = colDataTypes.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    String[] colValueArray = null;
    for (String colArrayVal : colArray) {
      colValueArray = colArrayVal.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
      mapOfColNameDataType.put(colValueArray[0].toLowerCase(), colValueArray[1]);

    }
    return mapOfColNameDataType;
  }

  public static byte[] convertListByteArrToSingleArr(List<byte[]> noDictionaryValKeyList) {
    ByteBuffer[] buffArr = new ByteBuffer[noDictionaryValKeyList.size()];
    int index = 0;
    for (byte[] singleColVal : noDictionaryValKeyList) {
      buffArr[index] = ByteBuffer.allocate(singleColVal.length);
      buffArr[index].put(singleColVal);
      buffArr[index++].rewind();
    }

    return NonDictionaryUtil.packByteBufferIntoSingleByteArray(buffArr);

  }

  /**
   * This method will convert boolean [] to String with comma separated.
   * This needs to be done as sort step meta only supports string types.
   *
   * @param noDictionaryDimsMapping boolean arr to represent which dims is no dictionary
   * @return
   */
  public static String convertBooleanArrToString(Boolean[] noDictionaryDimsMapping) {
    StringBuilder  builder = new StringBuilder();
    int index = 0;
    for (; index < noDictionaryDimsMapping.length ; index++) {
      builder.append(noDictionaryDimsMapping[index]);
      builder.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
    }
    int lastIndex = builder.lastIndexOf(CarbonCommonConstants.COMA_SPC_CHARACTER);
    String str = -1 != lastIndex ? builder.substring(0, lastIndex) : builder.toString();
    return str;
  }

  /**
   * This will convert string to boolean[].
   *
   * @param noDictionaryColMapping String representation of the boolean [].
   * @return
   */
  public static boolean[] convertStringToBooleanArr(String noDictionaryColMapping) {

    String[] splittedValue = null != noDictionaryColMapping ?
        noDictionaryColMapping.split(CarbonCommonConstants.COMA_SPC_CHARACTER) :
        new String[0];

    // convert string[] to boolean []

    boolean[] noDictionaryMapping = new boolean[splittedValue.length];
    int index = 0;
    for (String str : splittedValue) {
      noDictionaryMapping[index++] = Boolean.parseBoolean(str);
    }

    return noDictionaryMapping;
  }

  /**
   * This method will extract the single dimension from the complete high card dims byte[].+     *
   * The format of the byte [] will be,  Totallength,CompleteStartOffsets,Dat
   *
   * @param highCardArr
   * @param index
   * @param highCardinalityCount
   * @param outBuffer
   */
  public static void extractSingleHighCardDims(byte[] highCardArr, int index,
      int highCardinalityCount, ByteBuffer outBuffer) {
    ByteBuffer buff = null;
    short secIndex = 0;
    short firstIndex = 0;
    int length;
    // if the requested index is a last one then we need to calculate length
    // based on byte[] length.
    if (index == highCardinalityCount - 1) {
      // need to read 2 bytes(1 short) to determine starting offset and
      // length can be calculated by array length.
      buff = ByteBuffer.wrap(highCardArr, (index * 2) + 2, 2);
    } else {
      // need to read 4 bytes(2 short) to determine starting offset and
      // length.
      buff = ByteBuffer.wrap(highCardArr, (index * 2) + 2, 4);
    }

    firstIndex = buff.getShort();
    // if it is a last dimension in high card then this will be last
    // offset.so calculate length from total length
    if (index == highCardinalityCount - 1) {
      secIndex = (short) highCardArr.length;
    } else {
      secIndex = buff.getShort();
    }

    length = secIndex - firstIndex;

    outBuffer.position(firstIndex);
    outBuffer.limit(outBuffer.position() + length);

  }
}
