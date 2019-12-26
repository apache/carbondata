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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;

/**
 * Generic DataType interface which will be used while data loading for complex types like Array &
 * Struct
 */
public interface GenericDataType<T> extends Serializable {

  /**
   * @return name of the column
   */
  String getName();

  /**
   * @return - columns parent name
   */
  String getParentName();

  /**
   * @param children - To add children dimension for parent complex type
   */
  void addChildren(GenericDataType children);

  /**
   * @param primitiveChild - Returns all primitive type columns in complex type
   */
  void getAllPrimitiveChildren(List<GenericDataType> primitiveChild);

  /**
   * writes to byte stream
   * @param dataOutputStream
   * @throws IOException
   */
  void writeByteArray(T input, DataOutputStream dataOutputStream, BadRecordLogHolder logHolder)
      throws IOException;

  /**
   * @param surrIndex - surrogate index of primitive column in complex type
   */
  void setSurrogateIndex(int surrIndex);

  /**
   * Returns true in case the column has Dictionary Encoding.
   * @return
   */
  boolean getIsColumnDictionary();

  /**
   * Parse the Complex Datatype from the ByteBuffer.
   * @param byteArrayInput
   * @param dataOutputStream
   * @return
   * @throws IOException
   * @throws KeyGenException
   */
  void parseComplexValue(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream)
      throws IOException, KeyGenException;

  /**
   * @return columns count of each complex type
   */
  int getColsCount();

  /**
   * @return column uuid string
   */
  String getColumnNames();

  /**
   * set array index to be referred while creating metadata column
   * @param outputArrayIndex
   */
  void setOutputArrayIndex(int outputArrayIndex);

  /**
   * @return array index count of metadata column
   */
  int getMaxOutputArrayIndex();

  /**
   * Split byte array into complex metadata column and primitive column
   * @param columnsArray
   * @param inputArray
   */
  void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray, ByteBuffer inputArray);

  /**
   * @return current read row count
   */
  int getDataCounter();

  /**
   * clone self for multithread access (for complex type processing in table page)
   */
  GenericDataType<T> deepCopy();

  void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList);

  /**
   * @return depth of the complex columns , this is the length of flattened complex data.
   */
  int getDepth();
}
