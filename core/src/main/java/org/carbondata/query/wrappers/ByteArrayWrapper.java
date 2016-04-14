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

package org.carbondata.query.wrappers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.jpountz.xxhash.XXHash32;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.ByteUtil.UnsafeComparer;

/**
 * Class Description :This class will be used as a key in the result map, it
 * will contain maksed key and actual data
 * Version 1.0
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper>, Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 2203622486612960809L;
    /**
     * masked keys
     */
    protected byte[] maskedKey;
    protected List<byte[]> complexTypesData;
    List<byte[]> listOfNoDictionaryValVal;
    private XXHash32 xxHash32;
    private byte[] noDictionaryValVal;

    public ByteArrayWrapper() {
        this.complexTypesData = new ArrayList<byte[]>();
    }

    public ByteArrayWrapper(XXHash32 xxHash32) {
        this.xxHash32 = xxHash32;
        this.complexTypesData = new ArrayList<byte[]>();
    }

    public byte[] getComplexTypeData(int index) {
        return complexTypesData.get(index);
    }

    public List<byte[]> getCompleteComplexTypeData() {
        return complexTypesData;
    }

    public void addComplexTypeData(byte[] data) {
        complexTypesData.add(data);
    }

    /**
     * @param data           keys
     * @param offset         key offset
     * @param maxKey         max key
     * @param maskByteRanges mask byte range
     * @param byteCount      total byte count
     */
    public void setData(byte[] data, int offset, byte[] maxKey, int[] maskByteRanges,
            int byteCount) {
        // check masked key is null or not
        if (maskedKey == null) {
            this.maskedKey = new byte[byteCount];
        }
        int counter = 0;
        int byteRange = 0;
        for (int i = 0; i < maskByteRanges.length; i++) {
            byteRange = maskByteRanges[i];
            maskedKey[counter++] = (byte) (data[byteRange + offset] & maxKey[byteRange]);
        }

    }

    /**
     * @param data      byte array
     * @param offset    key offset
     * @param length    key length
     * @param maxKey    max key
     * @param byteCount total byte count
     */
    public void setData(byte[] data, int offset, int length, byte[] maxKey, int byteCount) {
        if (maskedKey == null) {
            this.maskedKey = new byte[length];
        }

        for (int j = 0; j < length; j++) {
            maskedKey[j] = (byte) (data[offset] & maxKey[j]);
            offset++;
        }
    }

    /**
     * This method is used to calculate the hash code
     *
     * @param maskKey mask key
     * @return hashcode
     */
    protected int getHashCode(byte[] maskKey) {
        int len = maskKey.length;
        if (xxHash32 != null) {
            return xxHash32.hash(maskKey, 0, len, 0);
        }

        int result = 1;
        for (int j = 0; j < len; j++) {
            result = 31 * result + maskKey[j];
        }
        return result;
    }

    /**
     * This method will be used to get the hascode, this will be used to the
     * index for inserting ArrayWrapper object as a key in Map
     *
     * @return int hascode
     */
    public int hashCode() {
        int len = maskedKey.length;
        if (xxHash32 != null) {
            return xxHash32.hash(maskedKey, 0, len, 0);
        }

        int result = 1;
        for (int j = 0; j < len; j++) {
            result = 31 * result + maskedKey[j];
        }
        if (null != listOfNoDictionaryValVal) {
            int index = 0;
            for (byte[] noDictionaryValValue : listOfNoDictionaryValVal) {
                for (int i = 0; i < noDictionaryValValue.length; i++) {
                    result = 31 * result + noDictionaryValValue[i];
                }
            }
        }
        return result;
    }

    /**
     * This method will be used check to ByteArrayWrapper object is equal or not
     *
     * @param object ArrayWrapper object
     * @return boolean
     * equal or not
     */
    public boolean equals(Object other) {

        if (null == other || !(other instanceof ByteArrayWrapper)) {
            return false;
        }
        boolean result = false;
        // High cardinality dimension rows has been added in list of
        // ByteArrayWrapper, so
        // the same has to be compared to know whether the byte array wrappers
        // are equals or not.
        List<byte[]> otherList = ((ByteArrayWrapper) other).getNoDictionaryValKeyList();
        if (null != listOfNoDictionaryValVal) {
            if (listOfNoDictionaryValVal.size() != otherList.size()) {
                return false;
            } else {
                for (int i = 0; i < listOfNoDictionaryValVal.size(); i++) {
                    result = UnsafeComparer.INSTANCE
                            .equals(listOfNoDictionaryValVal.get(i), otherList.get(i));
                    if (!result) {
                        return false;
                    }
                }
            }

        }

        List<byte[]> otherComplexTypesData =
                ((ByteArrayWrapper) other).getCompleteComplexTypeData();
        if (null != complexTypesData) {
            if (complexTypesData.size() != otherComplexTypesData.size()) {
                return false;
            } else {
                for (int i = 0; i < complexTypesData.size(); i++) {
                    result = UnsafeComparer.INSTANCE
                            .equals(complexTypesData.get(i), otherComplexTypesData.get(i));
                    if (!result) {
                        return false;
                    }
                }
            }

        }
        return UnsafeComparer.INSTANCE.equals(maskedKey, ((ByteArrayWrapper) other).maskedKey);
    }

    /**
     * Compare method for ByteArrayWrapper class this will used to compare Two
     * ByteArrayWrapper data object, basically it will compare two byte
     * array
     *
     * @param other ArrayWrapper Object
     */
    @Override
    public int compareTo(ByteArrayWrapper other) {
        int compareTo = UnsafeComparer.INSTANCE.compareTo(maskedKey, other.maskedKey);
        if (compareTo == 0) {
            if (null != listOfNoDictionaryValVal) {
                for (int i = 0; i < listOfNoDictionaryValVal.size(); i++) {
                    compareTo = UnsafeComparer.INSTANCE.compareTo(listOfNoDictionaryValVal.get(i),
                            other.listOfNoDictionaryValVal.get(i));
                    if (compareTo != 0) {
                        return compareTo;
                    }
                }
            }
        }
        return compareTo;
    }

    public byte[] getMaskedKey() {
        return maskedKey;
    }

    public void setMaskedKey(byte[] maskedKey) {
        this.maskedKey = maskedKey;
    }

    /**
     * addToNoDictionaryValKeyList
     *
     * @param noDictionaryValKeyData
     */
    public void addToNoDictionaryValKeyList(byte[] noDictionaryValKeyData) {
        if (null == listOfNoDictionaryValVal) {
            listOfNoDictionaryValVal =
                    new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
            listOfNoDictionaryValVal.add(noDictionaryValKeyData);
        } else {
            listOfNoDictionaryValVal.add(noDictionaryValKeyData);
        }

    }

    /**
     * addToNoDictionaryValKeyList
     *
     * @param noDictionaryValKeyData
     */
    public void addToNoDictionaryValKeyList(List<byte[]> noDictionaryValKeyData) {
        //Add if any direct surrogates are really present.
        if (null != noDictionaryValKeyData && !noDictionaryValKeyData.isEmpty()) {
            if (null == listOfNoDictionaryValVal) {
                listOfNoDictionaryValVal =
                        new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                listOfNoDictionaryValVal.addAll(noDictionaryValKeyData);
            } else {
                listOfNoDictionaryValVal.addAll(noDictionaryValKeyData);
            }
        }

    }

    /**
     * @return
     */
    public List<byte[]> getNoDictionaryValKeyList() {
        return listOfNoDictionaryValVal;
    }
}
