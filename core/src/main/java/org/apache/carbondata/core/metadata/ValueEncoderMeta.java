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

package org.apache.carbondata.core.metadata;

import java.io.Serializable;

/**
 * DO NOT MODIFY THIS CLASS AND PACKAGE NAME, BECAUSE
 * IT IS SERIALIZE TO STORE
 * It holds Value compression metadata for one data column
 */
public class ValueEncoderMeta implements Serializable {

  /**
   * maxValue
   */
  private Object maxValue;
  /**
   * minValue.
   */
  private Object minValue;

  /**
   * uniqueValue
   */
  private Object uniqueValue;

  private int decimal;

  private char type;

  private byte dataTypeSelected;

  public Object getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Object maxValue) {
    this.maxValue = maxValue;
  }

  public Object getMinValue() {
    return minValue;
  }

  public void setMinValue(Object minValue) {
    this.minValue = minValue;
  }

  public Object getUniqueValue() {
    return uniqueValue;
  }

  public void setUniqueValue(Object uniqueValue) {
    this.uniqueValue = uniqueValue;
  }

  public int getDecimal() {
    return decimal;
  }

  public void setDecimal(int decimal) {
    this.decimal = decimal;
  }

  public char getType() {
    return type;
  }

  public void setType(char type) {
    this.type = type;
  }

  public byte getDataTypeSelected() {
    return dataTypeSelected;
  }

  public void setDataTypeSelected(byte dataTypeSelected) {
    this.dataTypeSelected = dataTypeSelected;
  }
}
