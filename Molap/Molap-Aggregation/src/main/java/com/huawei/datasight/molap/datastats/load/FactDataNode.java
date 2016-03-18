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

package com.huawei.datasight.molap.datastats.load;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * This class contains compressed Columnkey block of store and Measure value
 * 
 * @author A00902717
 *
 */
public class FactDataNode
{

	/**
	 * Compressed keyblocks
	 */
	private ColumnarKeyStore keyStore;


	private int maxKeys;


	public FactDataNode(int maxKeys, int[] eachBlockSize, boolean isFileStore,
			FileHolder fileHolder, LeafNodeInfoColumnar leafNodeInfo,
			ValueCompressionModel compressionModel)
	{

		this.maxKeys = maxKeys;
		
		ColumnarKeyStoreInfo columnarStoreInfo = MolapUtil
				.getColumnarKeyStoreInfo(leafNodeInfo, eachBlockSize,null);
		keyStore = StoreFactory.createColumnarKeyStore(columnarStoreInfo,
				fileHolder, isFileStore);

	}

	public ColumnarKeyStoreDataHolder[] getColumnData(FileHolder fileHolder,
			int[] dimensions, boolean[] needCompression)
	{
		
	   	ColumnarKeyStoreDataHolder[] keyDataHolderUncompressed = keyStore
				.getUnCompressedKeyArray(fileHolder, dimensions,
						needCompression);

		return keyDataHolderUncompressed;

	}

	public int getMaxKeys()
	{
		return maxKeys;
	}

}
