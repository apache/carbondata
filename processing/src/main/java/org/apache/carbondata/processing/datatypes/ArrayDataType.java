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

/**
 * Array DataType stateless object used in data loading
 */
public class ArrayDataType implements GenericDataType<ArrayObject> {

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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2587
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2588
      String name) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    return isDictionary;
  }

  @Override
  public void writeByteArray(ArrayObject input, DataOutputStream dataOutputStream,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3761
      BadRecordLogHolder logHolder, Boolean isWithoutConverter) throws IOException {
    if (input == null) {
      dataOutputStream.writeInt(1);
      children.writeByteArray(null, dataOutputStream, logHolder, isWithoutConverter);
    } else {
      Object[] data = input.getData();
      dataOutputStream.writeInt(data.length);
      for (Object eachInput : data) {
        children.writeByteArray(eachInput, dataOutputStream, logHolder, isWithoutConverter);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2437
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
  public GenericDataType<ArrayObject> deepCopy() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2587
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2588
    return new ArrayDataType(this.outputArrayIndex, this.dataCounter, this.children.deepCopy(),
        this.name);
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_ARRAY, DataTypeUtil.valueOf("array"),
            name, false));
    children.getComplexColumnInfo(columnInfoList);
  }

  @Override
  public int getDepth() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
    if (depth == 0) {
      // calculate only one time
      List<ComplexColumnInfo> complexColumnInfoList = new ArrayList<>();
      getComplexColumnInfo(complexColumnInfoList);
      depth = complexColumnInfoList.size();
    }
    return depth;
  }

}
