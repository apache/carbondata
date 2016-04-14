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

package org.carbondata.query.columnar.keyvalue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public abstract class AbstractColumnarScanResult {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(AbstractColumnarScanResult.class.getName());
    protected int currentRow = -1;
    protected int keySize;
    protected int sourcePosition = -1;
    protected int[] rowMapping;
    protected CarbonReadDataHolder[] measureBlocks;
    protected ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder;
    private int totalNumberOfRows;
    private int rowCounter;
    private int[] selectedDimensionIndex;

    public AbstractColumnarScanResult(int keySize, int[] selectedDimensionIndex) {
        this.keySize = keySize;
        this.selectedDimensionIndex = selectedDimensionIndex;
    }

    public void setKeyBlock(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder) {
        this.columnarKeyStoreDataHolder = columnarKeyStoreDataHolder;
    }

    public int getKeyBlockLength() {
        return columnarKeyStoreDataHolder.length;
    }

    public void setMeasureBlock(CarbonReadDataHolder[] measureBlocks) {
        this.measureBlocks = measureBlocks;
    }

    public void setNumberOfRows(int totalNumberOfRows) {
        this.totalNumberOfRows = totalNumberOfRows;
    }

    public void setIndexes(int[] indexes) {
        this.rowMapping = indexes;
    }

    public int numberOfOutputRows() {
        return this.totalNumberOfRows;
    }

    public boolean hasNext() {
        return rowCounter < this.totalNumberOfRows;
    }

    public void reset() {
        sourcePosition = -1;
        rowCounter = 0;
        currentRow = -1;
    }

    protected byte[] getKeyArray(int columnIndex, ByteArrayWrapper keyVal) {
        byte[] completeKeyArray = new byte[keySize];
        int destinationPosition = 0;
        for (int i = 0; i < selectedDimensionIndex.length; i++) {
            if (columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getColumnarKeyStoreMetadata()
                    .isNoDictionaryValColumn()) {
                //Incase of high cardinality system has to update the byte array with high
                //cardinality dimension values.
                updateByteArrayWithNoDictionaryValKeyVal(keyVal, columnIndex,
                        columnarKeyStoreDataHolder[selectedDimensionIndex[i]]);
                continue;
            }
            if (!columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getColumnarKeyStoreMetadata()
                    .isSorted()) {
                System.arraycopy(
                        columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData(),
                        columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getColumnReverseIndex()[columnIndex]
                                * columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize(), completeKeyArray,
                        destinationPosition, columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize());
            } else if (!columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                    .getColumnarKeyStoreMetadata().isSorted()) {
                System.arraycopy(
                        columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData(),
                        ((columnIndex) * columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize()), completeKeyArray,
                        destinationPosition, columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize());

            } else {

                System.arraycopy(
                        columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData(),
                        columnIndex * columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize(), completeKeyArray,
                        destinationPosition, columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize());
            }
            destinationPosition += columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                    .getColumnarKeyStoreMetadata().getEachRowSize();
        }
        rowCounter++;
        return completeKeyArray;
    }

    /**
     * Incase of high cardinality system has to update the byte array with high cardinality
     * dimension values separately since its not part of Key generator. Based on column reverse
     * index value the high cardinality data has been get from the
     * mapOfColumnarKeyBlockDataForNoDictionaryVals.
     *
     * @param key
     * @param colIndex
     * @param columnarKeyStoreDataHolder
     */
    private void updateByteArrayWithNoDictionaryValKeyVal(ByteArrayWrapper key, int colIndex,
            ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder) {

        List<byte[]> listOfColumnarKeyBlockDataForNoDictionaryVals =
                columnarKeyStoreDataHolder.getNoDictionaryValBasedKeyBlockData();
        int[] columnReverseIndexArray =
                columnarKeyStoreDataHolder.getColumnarKeyStoreMetadata().getColumnReverseIndex();

        if (null != listOfColumnarKeyBlockDataForNoDictionaryVals) {
            if (null != columnReverseIndexArray) {

                key.addToNoDictionaryValKeyList(listOfColumnarKeyBlockDataForNoDictionaryVals
                        .get(columnReverseIndexArray[colIndex]));
            } else {
                key.addToNoDictionaryValKeyList(
                  listOfColumnarKeyBlockDataForNoDictionaryVals.get(colIndex));
            }

        }

    }

    public byte[] getKeyDataStore() {
        byte[] completeKeyArray = new byte[keySize];
        for (int i = 0; i < selectedDimensionIndex.length; i++) {
            if (columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getColumnarKeyStoreMetadata()
                    .isNoDictionaryValColumn()) {
                return columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData();
            }

        }

        return completeKeyArray;
    }

    protected List<byte[]> getKeyArrayWithComplexTypes(int columnIndex,
            Map<Integer, GenericQueryType> complexQueryDims, ByteArrayWrapper keyVal) {
        int keyArrayLength = 0;
        List<byte[]> completeComplexKey = new ArrayList<byte[]>();
        List<byte[]> completePrimitiveKey = new ArrayList<byte[]>();
        for (int i = 0; i < selectedDimensionIndex.length; i++) {
            GenericQueryType complexType = complexQueryDims.get(selectedDimensionIndex[i]);
            if (complexType == null) {
                byte[] currentColBytes =
                        new byte[columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                .getColumnarKeyStoreMetadata().getEachRowSize()];
                if (columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                        .getColumnarKeyStoreMetadata().isNoDictionaryValColumn()) {
                    //Incase of high cardinality system has to update the byte array with high
                    //cardinality dimension values.
                    updateByteArrayWithNoDictionaryValKeyVal(keyVal, columnIndex,
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]]);
                    continue;
                }
                if (!columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                        .getColumnarKeyStoreMetadata().isSorted()) {
                    System.arraycopy(
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData(),
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                    .getColumnarKeyStoreMetadata()
                                    .getColumnReverseIndex()[columnIndex]
                                    * columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                    .getColumnarKeyStoreMetadata().getEachRowSize(),
                            currentColBytes, 0,
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                    .getColumnarKeyStoreMetadata().getEachRowSize());
                } else {

                    System.arraycopy(
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]].getKeyBlockData(),
                            columnIndex * columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                    .getColumnarKeyStoreMetadata().getEachRowSize(),
                            currentColBytes, 0,
                            columnarKeyStoreDataHolder[selectedDimensionIndex[i]]
                                    .getColumnarKeyStoreMetadata().getEachRowSize());
                }
                completePrimitiveKey.add(currentColBytes);
                keyArrayLength += currentColBytes.length;
            } else {
                try {
                    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                    DataOutputStream dataOutput = new DataOutputStream(byteStream);
                    complexType
                            .parseBlocksAndReturnComplexColumnByteArray(columnarKeyStoreDataHolder,
                                    columnIndex, dataOutput);
                    completeComplexKey.add(byteStream.toByteArray());
                    //                    keyArrayLength += byteStream.toByteArray().length;
                    byteStream.close();
                } catch (IOException e) {
                    LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
                }
                i += (complexType.getColsCount() - 1);
            }
        }
        byte[] completeKeyArray = new byte[keyArrayLength];
        int l = 0;
        for (byte[] key : completePrimitiveKey) {
            for (int i = 0; i < key.length; i++) {
                completeKeyArray[l++] = key[i];
            }
        }
        completeComplexKey.add(completeKeyArray);
        rowCounter++;
        return completeComplexKey;
    }

    protected int getSurrogateKey(int index, int dimOrdinal) {
        return columnarKeyStoreDataHolder[dimOrdinal].getSurrogateKey(index);
    }

    protected void getComplexSurrogateKey(int index, GenericQueryType complexType,
            DataOutputStream dataOutputStream) throws IOException {
        complexType.parseBlocksAndReturnComplexColumnByteArray(columnarKeyStoreDataHolder, index,
                dataOutputStream);
    }

    public int getNotNullCount(byte[] notNullByteArray, Dimension dim) {
        int start = ByteUtil.UnsafeComparer.INSTANCE
                .compareTo(columnarKeyStoreDataHolder[dim.getOrdinal()].getKeyBlockData(),
                        0 * notNullByteArray.length, notNullByteArray.length, notNullByteArray, 0,
                        notNullByteArray.length);
        if (start == -1) {
            return totalNumberOfRows;
        }
        int count = 1;
        for (int j = start + 1; j < totalNumberOfRows; j++) {
            if (ByteUtil.UnsafeComparer.INSTANCE
                    .compareTo(columnarKeyStoreDataHolder[dim.getOrdinal()].getKeyBlockData(),
                            j * notNullByteArray.length, notNullByteArray.length, notNullByteArray,
                            0, notNullByteArray.length) == 0) {
                count++;
            }
        }
        return totalNumberOfRows - count;
    }

    public abstract double getDoubleValue(int measureOrdinal);

    public abstract BigDecimal getBigDecimalValue(int measureOrdinal);

    public abstract long getLongValue(int measureOrdinal);

    public abstract byte[] getByteArrayValue(int measureOrdinal);

    public abstract byte[] getKeyArray(ByteArrayWrapper key);

    public abstract byte[] getKeyArray();

    public abstract List<byte[]> getKeyArrayWithComplexTypes(
            Map<Integer, GenericQueryType> complexQueryDims, ByteArrayWrapper keyVal);

    public abstract int getDimDataForAgg(int dimOrdinal);

    public abstract byte[] getNo_DictionayDimDataForAgg(int dimOrdinal);

    public abstract void getComplexDimDataForAgg(GenericQueryType complexType,
            DataOutputStream dataOutputStream) throws IOException;

}
