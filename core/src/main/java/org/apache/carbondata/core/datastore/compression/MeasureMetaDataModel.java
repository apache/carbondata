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

package org.apache.carbondata.core.datastore.compression;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class MeasureMetaDataModel {
  /**
   * maxValue
   */
  private Object[] maxValue;

  /**
   * minValue
   */
  private Object[] minValue;

  /**
   * mantissa
   */
  private int[] mantissa;

  /**
   * measureCount
   */
  private int measureCount;

  /**
   * uniqueValue
   */
  private Object[] uniqueValue;

  /**
   * type
   */
  private DataType[] type;

  /**
   * dataTypeSelected
   */
  private byte[] dataTypeSelected;

  public MeasureMetaDataModel(Object[] minValue, Object[] maxValue, int[] mantissa,
      int measureCount, Object[] uniqueValue, DataType[] type, byte[] dataTypeSelected) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.mantissa = mantissa;
    this.measureCount = measureCount;
    this.uniqueValue = uniqueValue;
    this.type = type;
    this.dataTypeSelected = dataTypeSelected;
  }

  /**
   * get Max value
   *
   * @return
   */
  public Object[] getMaxValue() {
    return maxValue;
  }

  /**
   * getMinValue
   *
   * @return
   */
  public Object[] getMinValue() {
    return minValue;
  }

  /**
   * getMantissa
   *
   * @return
   */
  public int[] getMantissa() {
    return mantissa;
  }

  /**
   * getMeasureCount
   *
   * @return
   */
  public int getMeasureCount() {
    return measureCount;
  }

  /**
   * nonExistValue
   *
   * @return
   */
  public Object[] getUniqueValue() {
    return uniqueValue;
  }

  /**
   * @return the type
   */
  public DataType[] getType() {
    return type;
  }

  /**
   * @return the dataTypeSelected
   */
  public byte[] getDataTypeSelected() {
    return dataTypeSelected;
  }

}