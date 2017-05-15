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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;

public class WriterCompressModel {

  /**
   * DataType[]  variable.
   */
  private DataType[] convertedDataType;
  /**
   * DataType[]  variable.
   */
  private DataType[] actualDataType;

  /**
   * maxValue
   */
  private Object[] maxValue;
  /**
   * minValue.
   */
  private Object[] minValue;

  /**
   * uniqueValue
   */
  private Object[] uniqueValue;
  /**
   * mantissa.
   */
  private int[] mantissa;

  /**
   * aggType
   */
  private DataType[] type;

  /**
   * dataTypeSelected
   */
  private byte[] dataTypeSelected;
  /**
   * unCompressValues.
   */
  private ValueCompressionHolder[] valueHolder;

  private CompressionFinder[] compressionFinders;

  /**
   * @return the convertedDataType
   */
  public DataType[] getConvertedDataType() {
    return convertedDataType;
  }

  /**
   * @param convertedDataType the convertedDataType to set
   */
  public void setConvertedDataType(DataType[] convertedDataType) {
    this.convertedDataType = convertedDataType;
  }

  /**
   * @return the actualDataType
   */
  public DataType[] getActualDataType() {
    return actualDataType;
  }

  /**
   * @param actualDataType
   */
  public void setActualDataType(DataType[] actualDataType) {
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
   * @return the mantissa
   */
  public int[] getMantissa() {
    return mantissa;
  }

  /**
   * @param mantissa the mantissa to set
   */
  public void setMantissa(int[] mantissa) {
    this.mantissa = mantissa;
  }

  /**
   * getUnCompressValues().
   *
   * @return the unCompressValues
   */
  public ValueCompressionHolder[] getValueCompressionHolder() {
    return valueHolder;
  }

  /**
   * @param valueHolder set the ValueCompressionHolder
   */
  public void setValueCompressionHolder(ValueCompressionHolder[] valueHolder) {
    this.valueHolder = valueHolder;
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
    char[] ret = new char[type.length];
    for (int i = 0; i < ret.length; i++) {
      switch (type[i]) {
        case SHORT:
        case INT:
        case LONG:
          ret[i] = CarbonCommonConstants.BIG_INT_MEASURE;
          break;
        case DOUBLE:
          ret[i] = CarbonCommonConstants.DOUBLE_MEASURE;
          break;
        case DECIMAL:
          ret[i] = CarbonCommonConstants.BIG_DECIMAL_MEASURE;
          break;
      }
    }
    return ret;
  }

  public DataType[] getDataType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(DataType[] type) {
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

  public void setCompressionFinders(CompressionFinder[] compressionFinders) {
    this.compressionFinders = compressionFinders;
  }

  public CompressionFinder[] getCompressionFinders() {
    return this.compressionFinders;
  }

  /**
   * @return the compType
   */
  public ValueCompressionUtil.COMPRESSION_TYPE getCompType(int index) {
    return this.compressionFinders[index].getCompType();
  }
}
