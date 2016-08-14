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

package org.apache.carbondata.core.datastorage.store.compression;

import org.apache.carbondata.core.util.ValueCompressionUtil;

public class ValueCompressionModel {
  /**
   * COMPRESSION_TYPE[] variable.
   */
  private ValueCompressionUtil.COMPRESSION_TYPE[] compType;

  /**
   * DataType[]  variable.
   */
  private ValueCompressionUtil.DataType[] changedDataType;
  /**
   * DataType[]  variable.
   */
  private ValueCompressionUtil.DataType[] actualDataType;

  /**
   * maxValue
   */
  private Object[] maxValue;
  /**
   * minValue.
   */
  private Object[] minValue;

  private Object[] minValueFactForAgg;

  /**
   * uniqueValue
   */
  private Object[] uniqueValue;
  /**
   * decimal.
   */
  private int[] decimal;

  /**
   * aggType
   */
  private char[] type;

  /**
   * dataTypeSelected
   */
  private byte[] dataTypeSelected;
  /**
   * unCompressValues.
   */
  private ValueCompressonHolder.UnCompressValue[] unCompressValues;

  /**
   * @return the compType
   */
  public ValueCompressionUtil.COMPRESSION_TYPE[] getCompType() {
    return compType;
  }

  /**
   * @param compType the compType to set
   */
  public void setCompType(ValueCompressionUtil.COMPRESSION_TYPE[] compType) {
    this.compType = compType;
  }

  /**
   * @return the changedDataType
   */
  public ValueCompressionUtil.DataType[] getChangedDataType() {
    return changedDataType;
  }

  /**
   * @param changedDataType the changedDataType to set
   */
  public void setChangedDataType(ValueCompressionUtil.DataType[] changedDataType) {
    this.changedDataType = changedDataType;
  }

  /**
   * @return the actualDataType
   */
  public ValueCompressionUtil.DataType[] getActualDataType() {
    return actualDataType;
  }

  /**
   * @param actualDataType
   */
  public void setActualDataType(ValueCompressionUtil.DataType[] actualDataType) {
    this.actualDataType = actualDataType;
  }

  /**
   * @return the maxValue
   */
  public Object[] getMaxValue() {
    return maxValue;
  }

  /**
   * @param maxValue the maxValue to set
   */
  public void setMaxValue(Object[] maxValue) {
    this.maxValue = maxValue;
  }

  /**
   * @return the decimal
   */
  public int[] getDecimal() {
    return decimal;
  }

  /**
   * @param decimal the decimal to set
   */
  public void setDecimal(int[] decimal) {
    this.decimal = decimal;
  }

  /**
   * getUnCompressValues().
   *
   * @return the unCompressValues
   */
  public ValueCompressonHolder.UnCompressValue[] getUnCompressValues() {
    return unCompressValues;
  }

  /**
   * @param unCompressValues the unCompressValues to set
   */
  public void setUnCompressValues(ValueCompressonHolder.UnCompressValue[] unCompressValues) {
    this.unCompressValues = unCompressValues;
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
   * setMinValue.
   *
   * @param minValue
   */
  public void setMinValue(Object[] minValue) {
    this.minValue = minValue;
  }

  /**
   * @return the aggType
   */
  public char[] getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(char[] type) {
    this.type = type;
  }

  /**
   * @return the dataTypeSelected
   */
  public byte[] getDataTypeSelected() {
    return dataTypeSelected;
  }

  /**
   * @param dataTypeSelected the dataTypeSelected to set
   */
  public void setDataTypeSelected(byte[] dataTypeSelected) {
    this.dataTypeSelected = dataTypeSelected;
  }

  /**
   * getUniqueValue
   *
   * @return
   */
  public Object[] getUniqueValue() {
    return uniqueValue;
  }

  /**
   * setUniqueValue
   *
   * @param uniqueValue
   */
  public void setUniqueValue(Object[] uniqueValue) {
    this.uniqueValue = uniqueValue;
  }

  /**
   * @return the minValueFactForAgg
   */
  public Object[] getMinValueFactForAgg() {
    return minValueFactForAgg;
  }

  /**
   * @param minValueFactForAgg the minValueFactForAgg to set
   */
  public void setMinValueFactForAgg(Object[] minValueFactForAgg) {
    this.minValueFactForAgg = minValueFactForAgg;
  }
}
