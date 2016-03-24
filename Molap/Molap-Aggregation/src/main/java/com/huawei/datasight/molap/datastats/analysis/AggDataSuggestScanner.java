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

package com.huawei.datasight.molap.datastats.analysis;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

/**
 * This is store scanner, it returns given no of rows of records
 * 
 * @author A00902717
 *
 */
public class AggDataSuggestScanner extends AbstractColumnarScanResult
{
	private byte[][] dataBlock;

	protected int[] dataBlockSize;

	public AggDataSuggestScanner(int keySize, int[] selectedDimensionIndex)
	{
		super(keySize, selectedDimensionIndex);
		// TODO Auto-generated constructor stub
	}

	public void setKeyBlock(
			ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder)
	{
		super.setKeyBlock(columnarKeyStoreDataHolder);
		dataBlock = new byte[columnarKeyStoreDataHolder.length][];
		dataBlockSize = new int[columnarKeyStoreDataHolder.length];
		for (int i = 0; i < columnarKeyStoreDataHolder.length; i++)
		{
			dataBlock[i] = columnarKeyStoreDataHolder[i].getKeyBlockData();
			dataBlockSize[i] = columnarKeyStoreDataHolder[i]
					.getColumnarKeyStoreMetadata().getEachRowSize();
		}

	}

	public HashSet<Integer> getLimitedDataBlock(int noOfRows)
	{

		// int[] surrogates = new int[numberOfOutputRows()];
		byte[] completeKeyArray = null;

		HashSet<Integer> uniqueData = new HashSet<Integer>(noOfRows);
		int maxRows = dataBlock[0].length / dataBlockSize[0];
		for (int j = 0; j < maxRows; j++)
		{
			completeKeyArray = new byte[dataBlockSize[0]];

			System.arraycopy(dataBlock[0], j * dataBlockSize[0],
					completeKeyArray, 0, dataBlockSize[0]);

			byte[] actual = new byte[4];
			int destPos = 4 - dataBlockSize[0];
			System.arraycopy(completeKeyArray, 0, actual, destPos,
					dataBlockSize[0]);
			int valueInInt = ByteBuffer.wrap(actual).getInt();
			uniqueData.add(valueInInt);
			if (uniqueData.size() >= noOfRows)
			{
				return uniqueData;
			}
		}
		return uniqueData;

	}

	/**
	 * it returns complete data from given column
	 * 
	 * @param column
	 * @param noOfRows
	 * @return
	 */
	/*
	 * public byte[][] getDataBlock(int column) { byte[][] columnsData = new
	 * byte[numberOfOutputRows()][]; // int[] surrogates = new
	 * int[numberOfOutputRows()]; byte[] completeKeyArray = null;
	 * 
	 * for (int j = 0; j < numberOfOutputRows(); j++) { completeKeyArray = new
	 * byte[dataBlockSize[column]];
	 * 
	 * System.arraycopy(dataBlock[column], j * dataBlockSize[column],
	 * completeKeyArray, 0, dataBlockSize[column]);
	 * 
	 * byte[] actual = new byte[4]; int destPos = 4 - dataBlockSize[column];
	 * System.arraycopy(completeKeyArray, 0, actual, destPos,
	 * dataBlockSize[column]);
	 * 
	 * columnsData[j] = completeKeyArray;
	 * 
	 * } return columnsData; }
	 */

	public double getNormalMeasureValue(int measureOrdinal)
	{
		return 0.0;
	}

	public byte[] getCustomMeasureValue(int measureOrdinal)
	{
		return null;
	}

	public byte[] getKeyArray()
	{
		return null;
	}
	public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims)
	{
		return null;
	}

	@Override
	public int getDimDataForAgg(int dimOrdinal)
	{
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
    public byte[] getHighCardinalityDimDataForAgg(Dimension dimension)
    {
        // TODO Auto-generated method stub
        return null;
    }

	@Override
	public List<byte[]> getKeyArrayWithComplexTypes(
			Map<Integer, GenericQueryType> complexQueryDims,
			ByteArrayWrapper keyVal) {
		// TODO Auto-generated method stub
		return null;
	}

}
