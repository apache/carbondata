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

package org.carbondata.query.datastorage.storeInterfaces;

import java.util.Arrays;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.carbon.SqlStatement;

/**
 * This Class will hold key and value where key will be mdkey and value will be
 * measure value
 */
public class KeyValue {
    /**
     *
     */
    public int keyOffset;
    /**
     *
     */
    public int keyLength;
    /**
     *
     */
    public byte[] backKeyArray;
    /**
     *
     */
    public boolean isReset;
    /**
     *
     */
    private byte[] key;
    /**
     *
     */
    private double[] val;
    /**
     *
     */
    private int row;
    /**
     *
     */
    private int valueOffset;
    /**
     *
     */
    private short valueLength;
    /**
     *
     */
    //    private MeasureDataWrapper msrData;
    private CarbonReadDataHolder[] msrData;
    /**
     *
     */
    private int[] msrCols;
    /**
     *
     */
    private int nKeys;

    public KeyValue() {

    }

    /**
     * @return Returns the key.
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * @param key The key to set.
     */
    public void setKey(byte[] key) {
        this.key = key;
    }

    /**
     * @return Returns the backKeyArray.
     */
    public byte[] getBackKeyArray() {
        return backKeyArray;
    }

    /**
     * @param backKeyArray
     */
    public void setBackKeyArray(byte[] backKeyArray) {
        this.backKeyArray = backKeyArray;
    }

    /**
     * @return Returns the isReset.
     */
    public boolean isReset() {
        return isReset;
    }

    /**
     * @param isReset The isReset to set.
     */
    public void setReset(boolean isReset) {
        this.isReset = isReset;
    }

    /**
     * This will be used to set the block of data store with measure value
     *
     * @param block
     */
    public void setBlock(DataStoreBlock block, FileHolder fileHolder) {
        MeasureDataWrapper nodeMsrDataWrapper = block.getNodeMsrDataWrapper(msrCols, fileHolder);
        if (nodeMsrDataWrapper != null) {
            msrData = nodeMsrDataWrapper.getValues();
        }
        nKeys = block.getnKeys();
        this.backKeyArray = block.getBackKeyArray(fileHolder);
    }

    /**
     * This will be used to set the block of data store with measure value
     *
     * @param block
     * @param data  array
     *              it will be leaf node data
     */
    public void setBlock(DataStoreBlock block, byte[] backKeyArray, FileHolder fileHolder) {
        msrData = block.getNodeMsrDataWrapper(msrCols, fileHolder).getValues();
        nKeys = block.getnKeys();
        this.backKeyArray = backKeyArray;
    }

    public void increment() {
        keyOffset += keyLength;
        valueOffset += valueLength;
        row++;
    }

    public void reset() {
        keyOffset = 0;
        valueOffset = 0;
        row = 0;
        isReset = true;
    }

    public void resetOffsets() {
        keyOffset = 0;
        valueOffset = 0;
        row = 0;
    }

    public byte[] getArray() {
        return backKeyArray;
    }

    public Object getValue(int col, SqlStatement.Type dataType) {
        switch (dataType)   //get measure type and distinguish the get methods of MolapReadDataHolder
        {
        case LONG:
            return msrData[col].getReadableLongValueByIndex(row);
        case DECIMAL:
            return msrData[col].getReadableBigDecimalValueByIndex(row);
        default:
            return msrData[col].getReadableDoubleValueByIndex(row);
        }
    }

    /**
     * get the MolapReadDataHolder[] for agg(MolapReadDataHolder newVal,int index)
     *
     * @return
     */
    public CarbonReadDataHolder getMsrData(int col) {
        return msrData[col];
    }

    public byte[] getByteArrayValue(int col) {
        return msrData[col].getReadableByteArrayValueByIndex(row);//[row];//.get(row, col);
    }

    public int searchInternal(byte[] key, KeyGenerator keyGenerator) {

        // Do a binary search in the leaf node
        int lo = row;
        int hi = nKeys - 1;
        int mid = 0;
        int k = 0;
        int offset/* = keyOffset*/;
        while (lo <= hi) {
            //
            mid = (lo + hi) >>> 1;
            offset = keyLength * (mid);
            k = keyGenerator.compare(key, 0, keyLength, backKeyArray, offset, keyLength);
            //
            if (k < 0) {
                hi = mid - 1;
            }
            //
            else if (k > 0) {
                lo = mid + 1;
            } else {
                int currentPos = mid;
                while (currentPos - 1 >= 0 && keyGenerator
                        .compare(backKeyArray, ((currentPos - 1) * keyLength), keyLength,
                                backKeyArray, ((currentPos) * keyLength), keyLength) == 0) {
                    currentPos--;
                }
                mid = currentPos;
                //
                setRow(mid);
                return mid;
            }
        }
        if (k > 0) {
            // The entry at mid is less than the input key. Advance it by one
            mid++;
        }
        if (mid < nKeys) {
            //
            setRow(mid);
            return mid;
        }
        return -1;
    }

    /**
     * @return the keyOffset
     */
    public int getKeyOffset() {
        return keyOffset;
    }

    /**
     * @param keyOffset the keyOffset to set
     */
    public void setKeyOffset(int keyOffset) {
        this.keyOffset = keyOffset;
    }

    /**
     * @return the keyLength
     */
    public int getKeyLength() {
        return keyLength;
    }

    /**
     * @param keyLength the keyLength to set
     */
    public void setKeyLength(int keyLength) {
        this.keyLength = keyLength;
    }

    /**
     * @return the row
     */
    public int getRow() {
        return row;
    }

    /**
     * @param row the row to set
     */
    public void setRow(int row) {
        this.row = row;
        keyOffset = keyLength * row;
        valueOffset = valueLength * row;
    }

    /**
     * @return the valueOffset
     */
    public int getValueOffset() {
        return valueOffset;
    }

    /**
     * @param valueOffset the valueOffset to set
     */
    public void setValueOffset(int valueOffset) {
        this.valueOffset = valueOffset;
    }

    /**
     * @return the valueLength
     */
    public short getValueLength() {
        return valueLength;
    }

    /**
     * @param valueLength the valueLength to set
     */
    public void setValueLength(short valueLength) {
        this.valueLength = valueLength;
    }

    /**
     * @return the msrCols
     */
    public int[] getMsrCols() {
        return msrCols;
    }

    /**
     * @param msrCols the msrCols to set
     */
    public void setMsrCols(int[] msrCols) {
        this.msrCols = msrCols;
    }

    public void setOrigainalValue(double[] val) {
        this.val = val;
    }

    public byte[] getOriginalKey() {
        if (key == null) {
            byte[] k = new byte[keyLength];
            System.arraycopy(backKeyArray, keyOffset, k, 0, keyLength);
            return k;
        }
        return key;
    }

    public void setOriginalKey(byte[] key) {
        this.key = key;
    }

    public double[] getOriginalValue() {
        if (Arrays.equals(val, null)) {
            double[] cp = new double[msrData.length];
            for (int i = 0; i < cp.length; i++) {
                cp[i] = msrData[i].getReadableDoubleValueByIndex(
                        row);//[row];//.getValue(index, decimal[i], maxValue[i]);
            }
            return cp;

            //            return msrData[].get(row);
        }
        return val;
    }
}
