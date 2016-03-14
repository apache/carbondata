package com.huawei.datasight.molap.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;

import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedDimSurrogateKeyGen;

public interface GenericDataType {
	
	void setName(String name);
	
	String getName();

	void setParentname(String parentname);
	
	String getParentname();
	
	void addChildren(GenericDataType children);
	
	void getAllPrimitiveChildren(List<GenericDataType> primitiveChild);
	
	void parseStringAndWriteByteArray(String tableName, String inputString, String[] delimiter, 
			int delimiterIndex,	DataOutputStream dataOutputStream, 
			MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException;
	
	int getSurrogateIndex();
	
	void setSurrogateIndex(int surrIndex);
	
	void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream, KeyGenerator[] generator) throws IOException, KeyGenException;
	
	int getColsCount();
	
	void setOutputArrayIndex(int outputArrayIndex);
	
	int getMaxOutputArrayIndex();
	
	void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray, ByteBuffer inputArray);
	
	int getDataCounter();
	
	void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock);
	
	void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize);
	
	void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex, int[] maxSurrogateKeyArray);
}
