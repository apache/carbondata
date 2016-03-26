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

/**
 *
 */
package org.carbondata.processing.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.IgnoreDictionary;
import org.carbondata.core.constants.MolapCommonConstants;

/**
 * This is the utility class for No Dictionary changes.
 */
public class RemoveDictionaryUtil {
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
                RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArr);
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
     * Method to check whether entire row is empty or not.
     *
     * @param row
     * @return
     */
    public static boolean checkAllValuesForNull(Object[] row) {
        if (checkNullForDims(row[0]) && checkNullForDouble(row[2]) && null == row[1]) {
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
    private static boolean checkNullForDouble(Object object) {
        Double[] measures = (Double[]) object;
        for (Double measure : measures) {
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

    /**
     * @param row
     * @return
     */
    public static Integer[] getCompleteDimensions(Object[] row) {

        return (Integer[]) row[0];
        }

    /**
     * This will extract the high cardinality count from the string.
     */
    public static int extractHighCardCount(String highCardinalityDim) {
        return extractHighCardDimsArr(highCardinalityDim).length;
    }

    /**
     * This method will split one single byte array of high card dims into array
     * of byte arrays.
     *
     * @param highCardArr
     * @param highCardCount
     * @return
     */
    public static byte[][] splitHighCardKey(byte[] highCardArr, int highCardCount) {
        byte[][] split = new byte[highCardCount][];

        ByteBuffer buff = ByteBuffer.wrap(highCardArr, 2, highCardCount * 2);

        int remainingCol = highCardCount;
        short secIndex = 0;
        short firstIndex = 0;
        for (int i = 0; i < highCardCount; i++) {

            if (remainingCol == 1) {
                firstIndex = buff.getShort();
                int length = highCardArr.length - firstIndex;

                // add 2 bytes (short) as length required to determine size of
                // each column value.

                split[i] = new byte[length + 2];
                ByteBuffer splittedCol = ByteBuffer.wrap(split[i]);
                splittedCol.putShort((short) length);

                System.arraycopy(highCardArr, firstIndex, split[i], 2, length);

            } else {

                firstIndex = buff.getShort();
                secIndex = buff.getShort();
                int length = secIndex - firstIndex;

                // add 2 bytes (short) as length required to determine size of
                // each column value.

                split[i] = new byte[length + 2];
                ByteBuffer splittedCol = ByteBuffer.wrap(split[i]);
                splittedCol.putShort((short) length);

                System.arraycopy(highCardArr, firstIndex, split[i], 2, length);
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
        Integer[] dimensions =
                (Integer[]) objArr[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()];

        dimensions[index] = val;

    }

    /**
     * This will extract the high cardinality count from the string.
     */
    public static String[] extractHighCardDimsArr(String highCardinalityDim) {

        if (null == highCardinalityDim || highCardinalityDim.isEmpty()) {
            return new String[0];
        }

        String[] highCard = highCardinalityDim.split(MolapCommonConstants.COMA_SPC_CHARACTER);
        List<String> list1 = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (int i = 0; i < highCard.length; i++) {
            String[] dim = highCard[i].split(MolapCommonConstants.COLON_SPC_CHARACTER);
            list1.add(dim[0]);
        }

        return list1.toArray(new String[list1.size()]);
    }

    public static byte[] convertListByteArrToSingleArr(List<byte[]> directSurrogateKeyList) {
        ByteBuffer[] buffArr = new ByteBuffer[directSurrogateKeyList.size()];
        int index = 0;
        for (byte[] singleColVal : directSurrogateKeyList) {
            buffArr[index] = ByteBuffer.allocate(singleColVal.length);
            buffArr[index].put(singleColVal);
            buffArr[index++].rewind();
        }

        return RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(buffArr);

    }
}
