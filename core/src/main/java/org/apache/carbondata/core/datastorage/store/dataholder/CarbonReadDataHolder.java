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

// This class is used with Uncompressor to hold the decompressed column chunk in memory
public class CarbonReadDataHolder {

  private byte[][] byteValues;

  private short[] shortValues;

  private int[] intValues;

  private long[] longValues;

  private BigDecimal[] bigDecimalValues;

  private double[] doubleValues;

  public void setReadableByteValues(byte[][] byteValues) {
    this.byteValues = byteValues;
  }

  public void setReadableShortValues(short[] shortValues) {
    this.shortValues = shortValues;
  }

  public void setReadableIntValues(int[] intValues) {
    this.intValues = intValues;
  }

  public void setReadableLongValues(long[] longValues) {
    this.longValues = longValues;
  }

  public void setReadableBigDecimalValues(BigDecimal[] bigDecimalValues) {
    this.bigDecimalValues = bigDecimalValues;
  }

  public void setReadableDoubleValues(double[] doubleValues) {
    this.doubleValues = doubleValues;
  }

  public byte[] getReadableByteArrayValueByIndex(int index) {
    return this.byteValues[index];
  }

  public long getReadableShortValueByIndex(int index) {
    return this.shortValues[index];
  }

  public long getReadableIntValueByIndex(int index) {
    return this.intValues[index];
  }

  public long getReadableLongValueByIndex(int index) {
    return this.longValues[index];
  }

  public BigDecimal getReadableBigDecimalValueByIndex(int index) {
    return this.bigDecimalValues[index];
  }

  public double getReadableDoubleValueByIndex(int index) {
    return this.doubleValues[index];
  }

}
