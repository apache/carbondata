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

package com.huawei.datasight.molap.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedDimSurrogateKeyGen;
import org.pentaho.di.core.exception.KettleException;

public class PrimitiveDataType implements GenericDataType {

    private int index;

    private String name;
    private String parentname;

    private int keySize;

    private int outputArrayIndex;

    private int dataCounter;

    public PrimitiveDataType(String name, String parentname) {
        this.name = name;
        this.parentname = parentname;
    }

    @Override public void addChildren(GenericDataType children) {

    }

    @Override public String getName() {
        return name;
    }

    @Override public void setName(String name) {
        this.name = name;
    }

    @Override public String getParentname() {
        return parentname;
    }

    @Override public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {

    }

    @Override public int getSurrogateIndex() {
        return index;
    }

    @Override public void setSurrogateIndex(int surrIndex) {
        index = surrIndex;
    }

    @Override public void parseStringAndWriteByteArray(String tableName, String inputString,
            String[] delimiter, int delimiterIndex, DataOutputStream dataOutputStream,
            MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException {
        dataOutputStream.writeInt(surrogateKeyGen
                .generateSurrogateKeys(inputString, tableName + "_" + name, index, new Object[0]));
    }

    @Override
    public void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
            KeyGenerator[] generator) throws IOException, KeyGenException {
        int data = byteArrayInput.getInt();
        dataOutputStream.write(generator[index].generateKey(new int[] { data }));
    }

    @Override public int getColsCount() {
        return 1;
    }

    @Override public void setOutputArrayIndex(int outputArrayIndex) {
        this.outputArrayIndex = outputArrayIndex;
    }

    @Override public int getMaxOutputArrayIndex() {
        return outputArrayIndex;
    }

    @Override public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
            ByteBuffer inputArray) {
        byte[] key = new byte[keySize];
        inputArray.get(key);
        columnsArray.get(outputArrayIndex).add(key);
        dataCounter++;
    }

    @Override public int getDataCounter() {
        return this.dataCounter;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    @Override
    public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock) {
        aggKeyBlockWithComplex.add(aggKeyBlock[index]);
    }

    @Override public void fillBlockKeySize(List<Integer> blockKeySizeWithComplex,
            int[] primitiveBlockKeySize) {
        blockKeySizeWithComplex.add(primitiveBlockKeySize[index]);
    }

    @Override public void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex,
            int[] maxSurrogateKeyArray) {
        dimCardWithComplex.add(maxSurrogateKeyArray[index]);
    }
}
