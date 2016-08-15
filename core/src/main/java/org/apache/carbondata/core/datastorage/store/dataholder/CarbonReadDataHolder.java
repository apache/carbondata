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

package org.apache.carbondata.core.datastorage.store.dataholder;

import java.math.BigDecimal;

public class CarbonReadDataHolder {

  /**
   * doubleValues
   */
  private double[] doubleValues;

  /**
   * longValues
   */
  private long[] longValues;

  /**
   * bigDecimalValues
   */
  private BigDecimal[] bigDecimalValues;

  /**
   * byteValues
   */
  private byte[][] byteValues;

  /**
   * @return the doubleValues
   */
  public double[] getReadableDoubleValues() {
    return doubleValues;
  }

  /**
   * @param doubleValues the doubleValues to set
   */
  public void setReadableDoubleValues(double[] doubleValues) {
    this.doubleValues = doubleValues;
  }

  /**
   * @return the byteValues
   */
  public byte[][] getReadableByteArrayValues() {
    return byteValues;
  }

  /**
   * @param longValues the longValues to set
   */
  public void setReadableLongValues(long[] longValues) {
    this.longValues = longValues;
  }

  /**
   * @param longValues the bigDecimalValues to set
   */
  public void setReadableBigDecimalValues(BigDecimal[] bigDecimalValues) {
    this.bigDecimalValues = bigDecimalValues;
  }

  /**
   * @param byteValues the byteValues to set
   */
  public void setReadableByteValues(byte[][] byteValues) {
    this.byteValues = byteValues;
  }

  /**
   * below method will be used to get the double value by index
   *
   * @param index
   * @return double values
   */
  public double getReadableDoubleValueByIndex(int index) {
    return this.doubleValues[index];
  }

  public long getReadableLongValueByIndex(int index) {
    return this.longValues[index];
  }

  public BigDecimal getReadableBigDecimalValueByIndex(int index) {
    return this.bigDecimalValues[index];
  }

  /**
   * below method will be used to get the readable byte array value by index
   *
   * @param index
   * @return byte array value
   */
  public byte[] getReadableByteArrayValueByIndex(int index) {
    return this.byteValues[index];
  }
}
