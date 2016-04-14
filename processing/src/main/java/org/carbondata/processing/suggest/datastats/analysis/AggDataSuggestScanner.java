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

package org.carbondata.processing.suggest.datastats.analysis;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.wrappers.ByteArrayWrapper;

/**
 * This is store scanner, it returns given no of rows of records
 *
 */
public class AggDataSuggestScanner extends AbstractColumnarScanResult {
    protected int[] dataBlockSize;
    private byte[][] dataBlock;

    public AggDataSuggestScanner(int keySize, int[] selectedDimensionIndex) {
        super(keySize, selectedDimensionIndex);
    }

    public void setKeyBlock(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder) {
        super.setKeyBlock(columnarKeyStoreDataHolder);
        dataBlock = new byte[columnarKeyStoreDataHolder.length][];
        dataBlockSize = new int[columnarKeyStoreDataHolder.length];
        for (int i = 0; i < columnarKeyStoreDataHolder.length; i++) {
            dataBlock[i] = columnarKeyStoreDataHolder[i].getKeyBlockData();
            dataBlockSize[i] =
                    columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getEachRowSize();
        }

    }

    public HashSet<Integer> getLimitedDataBlock(int noOfRows) {

        byte[] completeKeyArray = null;

        HashSet<Integer> uniqueData = new HashSet<Integer>(noOfRows);
        int maxRows = dataBlock[0].length / dataBlockSize[0];
        for (int j = 0; j < maxRows; j++) {
            completeKeyArray = new byte[dataBlockSize[0]];

            System.arraycopy(dataBlock[0], j * dataBlockSize[0], completeKeyArray, 0,
                    dataBlockSize[0]);

            byte[] actual = new byte[4];
            int destPos = 4 - dataBlockSize[0];
            System.arraycopy(completeKeyArray, 0, actual, destPos, dataBlockSize[0]);
            int valueInInt = ByteBuffer.wrap(actual).getInt();
            uniqueData.add(valueInInt);
            if (uniqueData.size() >= noOfRows) {
                return uniqueData;
            }
        }
        return uniqueData;

    }

    public double getDoubleValue(int measureOrdinal) {
        return 0.0;
    }

    public BigDecimal getBigDecimalValue(int measureOrdinal) {
        return new BigDecimal(0);
    }

    public long getLongValue(int measureOrdinal) {
        return (long) (0);
    }

    public byte[] getByteArrayValue(int measureOrdinal) {
        return null;
    }

    public List<byte[]> getKeyArrayWithComplexTypes(
            Map<Integer, GenericQueryType> complexQueryDims) {
        return null;
    }

    @Override
    public int getDimDataForAgg(int dimOrdinal) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void getComplexDimDataForAgg(GenericQueryType complexType,
            DataOutputStream dataOutputStream) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] getKeyArray(ByteArrayWrapper key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getKeyArray() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getNo_DictionayDimDataForAgg(int dimOrdinal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims,
            ByteArrayWrapper keyVal) {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * Read the columnar data blocks from columnar data holder instance.
     * @param noOfRows
     * @param mergedData
     * @param dataHolder
     */
    public void getLimitedDataBlockForNoDictionaryVals(int noOfRows,Set<byte[]> mergedData,ColumnarKeyStoreDataHolder dataHolder)
    {
    	if(null!=dataHolder)
    	{
    		ColumnarKeyStoreMetadata keyStoreMetadata=dataHolder.getColumnarKeyStoreMetadata();
			if (null != dataHolder.getNoDictionaryValBasedKeyBlockData()) {
				List<byte[]> listOfNoDictionaryVals = dataHolder
						.getNoDictionaryValBasedKeyBlockData();
				for(int i=0;i<noOfRows;i++)
				{
					if(null==keyStoreMetadata.getColumnReverseIndex())
					{
						mergedData.add(listOfNoDictionaryVals.get(i));
					}
					else
					{
						mergedData.add(listOfNoDictionaryVals.get(keyStoreMetadata.getColumnReverseIndex()[i]));	
					}
				}
			}
    	}
    }

}
