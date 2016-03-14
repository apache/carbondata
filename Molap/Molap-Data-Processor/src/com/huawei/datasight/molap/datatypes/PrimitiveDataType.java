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

public class PrimitiveDataType implements GenericDataType {

	private int index;
	
	private String name;
	private String parentname;
	
	private int keySize;
	
	private int outputArrayIndex;
	
	private int dataCounter;
	
	public PrimitiveDataType(String name, String parentname)
	{
		this.name = name;
		this.parentname = parentname;
	}
	
	@Override
	public void addChildren(GenericDataType children) {

	}

	@Override
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setParentname(String parentname) {
		this.parentname = parentname;
		
	}

	@Override
	public String getParentname() {
		return parentname;
	}
	
	@Override
	public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {

	}

	@Override
	public int getSurrogateIndex() {
		return index;
	}

	@Override
	public void setSurrogateIndex(int surrIndex) {
		index = surrIndex;
	}

	@Override
	public void parseStringAndWriteByteArray(String tableName, String inputString,
			String[] delimiter, int delimiterIndex,
			DataOutputStream dataOutputStream,
			MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException {
		dataOutputStream.writeInt(surrogateKeyGen.generateSurrogateKeys(inputString,
				tableName+"_"+name, index, new Object[0]));
	}
	
	@Override
	public void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream, KeyGenerator[] generator) throws IOException, KeyGenException
	{
		int data = byteArrayInput.getInt();
		dataOutputStream.write(generator[index].generateKey(new int[]{data}));
	}

	@Override
	public int getColsCount() {
		return 1;
	}

	@Override
	public void setOutputArrayIndex(int outputArrayIndex) {
		this.outputArrayIndex = outputArrayIndex;
	}
	
	@Override
	public int getMaxOutputArrayIndex()
	{
		return outputArrayIndex;
	}
	

	@Override
	public void getColumnarDataForComplexType(
			List<ArrayList<byte[]>> columnsArray, ByteBuffer inputArray) {
		byte[] key = new byte[keySize];
		inputArray.get(key);
		columnsArray.get(outputArrayIndex).add(key);
		dataCounter++;
	}
	
	@Override
	public int getDataCounter()
	{
		return this.dataCounter;
	}
	
	public void setKeySize(int keySize)
	{
		this.keySize = keySize;
	}
	
	@Override
	public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock)
	{
		aggKeyBlockWithComplex.add(aggKeyBlock[index]);
	}

	@Override
	public void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize)
	{
		blockKeySizeWithComplex.add(primitiveBlockKeySize[index]);
	}
	
	@Override
	public void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex, int[] maxSurrogateKeyArray)
	{
		dimCardWithComplex.add(maxSurrogateKeyArray[index]);
	}
}
