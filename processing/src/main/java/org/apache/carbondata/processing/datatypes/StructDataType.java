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
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;

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
  private String parentName;
  /**
   * column unique id
   */
  private String columnId;
  /**
   * output array index
   */
  private int outputArrayIndex;

  /**
   * True if this is for dictionary column
   */
  private boolean isDictionary;

  /**
   * data counter
   */
  private int dataCounter;

  /* flat complex datatype length, including the children*/
  private int depth;

  private StructDataType(List<GenericDataType> children, int outputArrayIndex, int dataCounter,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2587
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2588
      String name) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
    this.children = children;
    this.outputArrayIndex = outputArrayIndex;
    this.dataCounter = dataCounter;
    this.name = name;
  }

  /**
   * constructor
   * @param name
   * @param parentName
   * @param columnId
   */
  public StructDataType(String name, String parentName, String columnId) {
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
  public StructDataType(String name, String parentName, String columnId,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      Boolean isDictionary) {
    this.name = name;
    this.parentName = parentName;
    this.columnId = columnId;
    this.isDictionary = isDictionary;
  }

  /*
   * add child dimensions
   */
  @Override
  public void addChildren(GenericDataType newChild) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    if (this.getName().equals(newChild.getParentName())) {
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
  public String getParentName() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    return parentName;
  }

  /*
   * get column unique id
   */
  @Override
  public String getColumnNames() {
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
  public void writeByteArray(StructObject input, DataOutputStream dataOutputStream,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3761
      BadRecordLogHolder logHolder, Boolean isWithoutConverter) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    dataOutputStream.writeShort(children.size());
    if (input == null) {
      for (int i = 0; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream, logHolder, isWithoutConverter);
      }
    } else {
      Object[] data = input.getData();
      for (int i = 0; i < data.length && i < children.size(); i++) {
        children.get(i).writeByteArray(data[i], dataOutputStream, logHolder, isWithoutConverter);
      }

      // For other children elements which don't have data, write empty
      for (int i = data.length; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream, logHolder, isWithoutConverter);
      }
    }
  }

  /**
   *
   * @param byteArrayInput
   * @param dataOutputStream
   * @return
   * @throws IOException
   * @throws KeyGenException
   */
  @Override
  public void parseComplexValue(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream)
      throws IOException, KeyGenException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    short childElement = byteArrayInput.getShort();
    dataOutputStream.writeShort(childElement);
    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
        if (children.get(i).getIsColumnDictionary()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
          dataOutputStream.writeInt(ByteUtil.dateBytesSize());
        }
      }
      children.get(i).parseComplexValue(byteArrayInput, dataOutputStream);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    ByteBuffer b = ByteBuffer.allocate(2);
    int childElement = inputArray.getShort();
    b.putShort((short)childElement);
    columnsArray.get(this.outputArrayIndex).add(b.array());
    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2437
        PrimitiveDataType child = ((PrimitiveDataType) children.get(i));
        if (child.getIsColumnDictionary()) {
          child.setKeySize(inputArray.getInt());
        }
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

  @Override
  public GenericDataType<StructObject> deepCopy() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
    List<GenericDataType> childrenClone = new ArrayList<>();
    for (GenericDataType child : children) {
      childrenClone.add(child.deepCopy());
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2587
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2588
    return new StructDataType(childrenClone, this.outputArrayIndex, this.dataCounter, this.name);
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_STRUCT, DataTypeUtil.valueOf("struct"),
            name, false));
    for (int i = 0; i < children.size(); i++) {
      children.get(i).getComplexColumnInfo(columnInfoList);
    }
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
