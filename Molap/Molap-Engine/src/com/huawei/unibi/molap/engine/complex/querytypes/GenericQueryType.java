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

package com.huawei.unibi.molap.engine.complex.querytypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.spark.sql.types.DataType;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public interface GenericQueryType {
	
	void setName(String name);
	
	String getName();

	void setParentname(String parentname);
	
	String getParentname();
	
	void setBlockIndex(int blockIndex);
	
	int getBlockIndex();
	
	void addChildren(GenericQueryType children);
	
	void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild);
	
	int getSurrogateIndex();
	
	void setSurrogateIndex(int surrIndex);
	
	int getColsCount();
	
	void setKeySize(int[] keyBlockSize);
	
	void setKeyOrdinalForQuery(int keyOrdinalForQuery);
	
	int getKeyOrdinalForQuery();
	
	void parseBlocksAndReturnComplexColumnByteArray(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder, int rowNumber, DataOutputStream dataOutputStream) throws IOException;
	
	DataType getSchemaType();
	
	Object getDataBasedOnDataTypeFromSurrogates(List<InMemoryCube> slices, ByteBuffer surrogateData, Dimension[] dimensions);
	
	void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput) throws IOException;
	
	void fillRequiredBlockData(BlockDataHolder blockDataHolder);
	
}
