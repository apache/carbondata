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

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;

/**
 * Array DataType stateless object used in data loading
 */
public class ArrayDataType implements GenericDataType<Object> {

  /**
   * child columns
   */
  private GenericDataType children;

  /**
   * name of the column
   */
  private String name;

  /**
   * column unique id
   */
  private String columnId;

  /**
   * parent column name
   */
  private String parentName;

  /**
   * output array index
   */
  private int outputArrayIndex;

  /**
   * True if this is for dictionary column
   */
  private boolean isDictionary;

  /**
   * current data counter
   */
  private int dataCounter;

  /* flat complex datatype length, including the children*/
  private int depth;

  private ArrayDataType(int outputArrayIndex, int dataCounter, GenericDataType children,
      String name) {
    this.outputArrayIndex = outputArrayIndex;
    this.dataCounter = dataCounter;
    this.children = children;
    this.name = name;
  }

  /**
   * constructor
   * @param name
   * @param parentName
   * @param columnId
   */
  public ArrayDataType(String name, String parentName, String columnId) {
    this.name = name;
    this.parentName = parentName;
    this.columnId = columnId;
  }

  /**
   * constructor
   * @param name
   * @param parentName
   * @param columnId
   * @param isDictionary
   */
  public ArrayDataType(String name, String parentName, String columnId,
      Boolean isDictionary) {
    this.name = name;
    this.parentName = parentName;
    this.columnId = columnId;
    this.isDictionary = isDictionary;
  }

  /*
   * to add child dimensions
   */
  @Override
  public void addChildren(GenericDataType children) {
    if (this.getName().equals(children.getParentName())) {
      this.children = children;
    } else {
      this.children.addChildren(children);
    }
  }

  /*
   * return column name
   */
  @Override
  public String getName() {
    return name;
  }

  /*
   * return column unique id
   */
  @Override
  public String getColumnNames() {
    return columnId;
  }

  /*
   * set parent name
   */
  @Override
  public String getParentName() {
    return parentName;
  }

  /*
   * returns all primitive type child columns
   */
  @Override
  public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {
    if (children instanceof PrimitiveDataType) {
      primitiveChild.add(children);
    } else {
      children.getAllPrimitiveChildren(primitiveChild);
    }
  }

  /*
   * set surrogate index
   */
  @Override
  public void setSurrogateIndex(int surrIndex) {

  }

  @Override
  public boolean getIsColumnDictionary() {
    return isDictionary;
  }

  @Override
  public void writeByteArray(Object input, DataOutputStream dataOutputStream,
      BadRecordLogHolder logHolder, Boolean isWithoutConverter, boolean isEmptyBadRecord)
      throws IOException {
    if (input == null) {
      dataOutputStream.writeInt(1);
      children.writeByteArray(null, dataOutputStream, logHolder, isWithoutConverter,
          isEmptyBadRecord);
    } else {
      Object[] data = ((ArrayObject) input).getData();
      if (data.length == 1 && data[0] != null
          && data[0].equals("") && !(children instanceof PrimitiveDataType)) {
        // If child complex column is empty, no need to iterate. Fill empty byte array and return.
        CarbonBadRecordUtil.updateEmptyValue(dataOutputStream, isEmptyBadRecord, logHolder,
            parentName, DataTypeUtil.valueOf("array"));
        return;
      } else {
        dataOutputStream.writeInt(data.length);
      }
      for (Object eachInput : data) {
        children.writeByteArray(eachInput, dataOutputStream, logHolder, isWithoutConverter,
            isEmptyBadRecord);
      }
    }
  }

  @Override
  public void parseComplexValue(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream)
      throws IOException, KeyGenException {
    int dataLength = byteArrayInput.getInt();

    dataOutputStream.writeInt(dataLength);
    if (children instanceof PrimitiveDataType) {
      if (children.getIsColumnDictionary()) {
        dataOutputStream.writeInt(ByteUtil.dateBytesSize());
      }
    }
    for (int i = 0; i < dataLength; i++) {
      children.parseComplexValue(byteArrayInput, dataOutputStream);
    }
  }

  /*
   * get children column count
   */
  @Override
  public int getColsCount() {
    return children.getColsCount() + 1;
  }

  /*
   * set array index
   */
  @Override
  public void setOutputArrayIndex(int outputArrayIndex) {
    this.outputArrayIndex = outputArrayIndex;
    children.setOutputArrayIndex(outputArrayIndex + 1);
  }

  /*
   * get current max array index
   */
  @Override
  public int getMaxOutputArrayIndex() {
    int currentMax = outputArrayIndex;
    int childMax = children.getMaxOutputArrayIndex();
    if (childMax > currentMax) {
      currentMax = childMax;
    }
    return currentMax;
  }

  /*
   * split byte array and return metadata and primitive column data
   */
  @Override
  public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {
    ByteBuffer b = ByteBuffer.allocate(8);
    int dataLength = inputArray.getInt();
    b.putInt(dataLength);
    if (dataLength == 0) {
      b.putInt(0);
    } else {
      b.putInt(children.getDataCounter());
    }
    columnsArray.get(this.outputArrayIndex).add(b.array());

    if (children instanceof PrimitiveDataType) {
      PrimitiveDataType child = ((PrimitiveDataType) children);
      if (child.getIsColumnDictionary()) {
        child.setKeySize(inputArray.getInt());
      }
    }
    for (int i = 0; i < dataLength; i++) {
      children.getColumnarDataForComplexType(columnsArray, inputArray);
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

  @Override
  public GenericDataType<Object> deepCopy() {
    return new ArrayDataType(this.outputArrayIndex, this.dataCounter, this.children.deepCopy(),
        this.name);
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_ARRAY, DataTypeUtil.valueOf("array"),
            name, false));
    children.getComplexColumnInfo(columnInfoList);
  }

  @Override
  public int getDepth() {
    if (depth == 0) {
      // calculate only one time
      List<ComplexColumnInfo> complexColumnInfoList = new ArrayList<>();
      getComplexColumnInfo(complexColumnInfoList);
      depth = complexColumnInfoList.size();
    }
    return depth;
  }

}