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
