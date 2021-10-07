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
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;

/**
 * Struct DataType stateless object used in data loading
 */
public class StructDataType implements GenericDataType<Object> {

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
      String name) {
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
    return isDictionary;
  }

  @Override
  public void writeByteArray(Object input, DataOutputStream dataOutputStream,
      BadRecordLogHolder logHolder, Boolean isWithoutConverter, boolean isEmptyBadRecord)
      throws IOException {
    dataOutputStream.writeShort(children.size());
    if (input == null || input.equals("")) {
      for (int i = 0; i < children.size(); i++) {
        if (input != null && input.equals("") && (children.get(i) instanceof ArrayDataType)) {
          // If child column is of array type and is empty, no need to iterate.
          // Fill empty byte array and return.
          CarbonBadRecordUtil.updateEmptyValue(dataOutputStream, isEmptyBadRecord, logHolder,
              children.get(i).getParentName(), DataTypeUtil.valueOf("array"));
        } else {
          children.get(i).writeByteArray(input, dataOutputStream, logHolder, isWithoutConverter,
              isEmptyBadRecord);
        }
      }
    } else {
      Object[] data = ((StructObject) input).getData();
      for (int i = 0; i < data.length && i < children.size(); i++) {
        children.get(i).writeByteArray(data[i], dataOutputStream, logHolder, isWithoutConverter,
            isEmptyBadRecord);
      }

      // For other children elements which don't have data, write empty
      for (int i = data.length; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream, logHolder, isWithoutConverter,
            isEmptyBadRecord);
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
    short childElement = byteArrayInput.getShort();
    dataOutputStream.writeShort(childElement);
    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
        if (children.get(i).getIsColumnDictionary()) {
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
    ByteBuffer b = ByteBuffer.allocate(2);
    int childElement = inputArray.getShort();
    b.putShort((short)childElement);
    columnsArray.get(this.outputArrayIndex).add(b.array());
    for (int i = 0; i < childElement; i++) {
      if (children.get(i) instanceof PrimitiveDataType) {
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
  public GenericDataType<Object> deepCopy() {
    List<GenericDataType> childrenClone = new ArrayList<>();
    for (GenericDataType child : children) {
      childrenClone.add(child.deepCopy());
    }
    return new StructDataType(childrenClone, this.outputArrayIndex, this.dataCounter, this.name);
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_STRUCT, DataTypeUtil.valueOf("struct"),
            name, false));
    for (int i = 0; i < children.size(); i++) {
      children.get(i).getComplexColumnInfo(columnInfoList);
    }
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