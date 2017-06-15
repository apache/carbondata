/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.GenericDataType;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.processing.newflow.complexobjects.StructObject;

/**
 * Struct DataType stateless object used in data loading
 */
public class StructDataType implements GenericDataType<StructObject> {

  /**
   * children columns
   */
  private List<GenericDataType> children = new ArrayList<GenericDataType>();
  /**
   * name of the column
   */
  private String name;
  /**
   * parent column name
   */
  private String parentname;
  /**
   * column unique id
   */
  private String columnId;
  /**
   * output array index
   */
  private int outputArrayIndex;
  /**
   * data counter
   */
  private int dataCounter;

  /**
   * constructor
   * @param name
   * @param parentname
   * @param columnId
   */
  public StructDataType(String name, String parentname, String columnId) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
  }

  /*
   * add child dimensions
   */
  @Override
  public void addChildren(GenericDataType newChild) {
    if (this.getName().equals(newChild.getParentname())) {
      this.children.add(newChild);
    } else {
      for (GenericDataType child : this.children) {
        child.addChildren(newChild);
      }
    }

  }

  /*
   * get column name
   */
  @Override
  public String getName() {
    return name;
  }

  /*
   * get parent column name
   */
  @Override
  public String getParentname() {
    return parentname;
  }

  /*
   * get column unique id
   */
  @Override
  public String getColumnId() {
    return columnId;
  }

  /*
   * get all primitive columns from complex column
   */
  @Override
  public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {
    for (int i = 0; i < children.size(); i++) {
      GenericDataType child = children.get(i);
      if (child instanceof PrimitiveDataType) {
        primitiveChild.add(child);
      } else {
        child.getAllPrimitiveChildren(primitiveChild);
      }
    }
  }

  /*
   * get surrogate index
   */
  @Override
  public int getSurrogateIndex() {
    return 0;
  }

  /*
   * set surrogate index
   */
  @Override
  public void setSurrogateIndex(int surrIndex) {

  }

  @Override public void writeByteArray(StructObject input, DataOutputStream dataOutputStream)
      throws IOException, DictionaryGenerationException {
    dataOutputStream.writeInt(children.size());
    if (input == null) {
      dataOutputStream.writeInt(children.size());
      for (int i = 0; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream);
      }
    } else {
      Object[] data = input.getData();
      for (int i = 0; i < data.length && i < children.size(); i++) {
        children.get(i).writeByteArray(data[i], dataOutputStream);
      }

      // For other children elements which dont have data, write empty
      for (int i = data.length; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream);
      }
    }
  }

  @Override
  public void fillCardinality(List<Integer> dimCardWithComplex) {
    dimCardWithComplex.add(0);
    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillCardinality(dimCardWithComplex);
    }
  }

  /*
       * parse bytearray and bit pack
       */
  @Override
  public void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator) throws IOException, KeyGenException {
    int childElement = byteArrayInput.getInt();
    dataOutputStream.writeInt(childElement);
    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
        dataOutputStream.writeInt(generator[children.get(i).getSurrogateIndex()]
            .getKeySizeInBytes());
      }
      children.get(i).parseAndBitPack(byteArrayInput, dataOutputStream, generator);
    }
  }

  /*
   * return all columns count
   */
  @Override
  public int getColsCount() {
    int colsCount = 1;
    for (int i = 0; i < children.size(); i++) {
      colsCount += children.get(i).getColsCount();
    }
    return colsCount;
  }

  /*
   * set output array index
   */
  @Override
  public void setOutputArrayIndex(int outputArrayIndex) {
    this.outputArrayIndex = outputArrayIndex++;
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
        children.get(i).setOutputArrayIndex(outputArrayIndex++);
      } else {
        children.get(i).setOutputArrayIndex(outputArrayIndex++);
        outputArrayIndex = getMaxOutputArrayIndex() + 1;
      }
    }
  }

  /*
   * get max array index
   */
  @Override
  public int getMaxOutputArrayIndex() {
    int currentMax = outputArrayIndex;
    for (int i = 0; i < children.size(); i++) {
      int childMax = children.get(i).getMaxOutputArrayIndex();
      if (childMax > currentMax) {
        currentMax = childMax;
      }
    }
    return currentMax;
  }

  /*
   * split byte array and return metadata and primitive columns
   */
  @Override
  public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {

    ByteBuffer b = ByteBuffer.allocate(8);
    int childElement = inputArray.getInt();
    b.putInt(childElement);
    if (childElement == 0) {
      b.putInt(0);
    } else {
      b.putInt(children.get(0).getDataCounter());
    }
    columnsArray.get(this.outputArrayIndex).add(b.array());

    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
        ((PrimitiveDataType) children.get(i)).setKeySize(inputArray.getInt());
      }
      children.get(i).getColumnarDataForComplexType(columnsArray, inputArray);
    }
    this.dataCounter++;
  }

  /*
   * return data counter
   */
  @Override
  public int getDataCounter() {
    return this.dataCounter;
  }

  /*
   * fill agg block
   */
  @Override
  public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock) {
    aggKeyBlockWithComplex.add(false);
    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillAggKeyBlock(aggKeyBlockWithComplex, aggKeyBlock);
    }
  }

  /*
   * fill keysize
   */
  @Override
  public void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize) {
    blockKeySizeWithComplex.add(8);
    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillBlockKeySize(blockKeySizeWithComplex, primitiveBlockKeySize);
    }
  }

  /*
   * fill cardinality
   */
  @Override
  public void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex,
      int[] maxSurrogateKeyArray) {
    dimCardWithComplex.add(0);
    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillCardinalityAfterDataLoad(dimCardWithComplex, maxSurrogateKeyArray);
    }
  }
}
