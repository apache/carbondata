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

package org.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedDimSurrogateKeyGen;

import org.pentaho.di.core.exception.KettleException;

public interface GenericDataType {

  String getName();

  void setName(String name);

  String getParentname();

  void addChildren(GenericDataType children);

  void getAllPrimitiveChildren(List<GenericDataType> primitiveChild);

  void parseStringAndWriteByteArray(String tableName, String inputString, String[] delimiter,
      int delimiterIndex, DataOutputStream dataOutputStream,
      CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException;

  int getSurrogateIndex();

  void setSurrogateIndex(int surrIndex);

  void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator) throws IOException, KeyGenException;

  int getColsCount();

  void setOutputArrayIndex(int outputArrayIndex);

  int getMaxOutputArrayIndex();

  void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray, ByteBuffer inputArray);

  int getDataCounter();

  void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock);

  void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize);

  void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex, int[] maxSurrogateKeyArray);
}
