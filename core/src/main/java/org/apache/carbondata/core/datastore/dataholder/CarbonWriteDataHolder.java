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

package org.apache.carbondata.core.datastore.dataholder;

public class CarbonWriteDataHolder {
  /**
   * doubleValues
   */
  private double[] doubleValues;

  /**
   * longValues
   */
  private long[] longValues;

  /**
   * byteValues
   */
  private byte[][] byteValues;

  /**
   * size
   */
  private int size;

  /**
   * total size of the data in bytes added
   */
  private int totalSize;

  public void reset() {
    size = 0;
    totalSize = 0;
  }

  /**
   * set long data type columnar data
   * @param values
   */
  public void setWritableLongPage(long[] values) {
    if (values != null) {
      longValues = values;
      size += values.length;
      totalSize += values.length;
    }
  }

  /**
   * set double data type columnar data
   * @param values
   */
  public void setWritableDoublePage(double[] values) {
    if (values != null) {
      doubleValues = values;
      size += values.length;
      totalSize += values.length;
    }
  }

  /**
   * set decimal data type columnar data
   * @param values
   */
  public void setWritableDecimalPage(byte[][] values) {
    if (values != null) {
      byteValues = values;
      size += values.length;
      for (int i = 0; i < values.length; i++) {
        if (values[i] != null) {
          totalSize += values[i].length;
        }
      }
    }
  }

  /**
   * Get Writable Double Values
   */
  public double[] getWritableDoubleValues() {
    if (size < doubleValues.length) {
      double[] temp = new double[size];
      System.arraycopy(doubleValues, 0, temp, 0, size);
      doubleValues = temp;
    }
    return doubleValues;
  }

  /**
   * Get writable byte array values
   */
  public byte[] getWritableByteArrayValues() {
    byte[] temp = new byte[totalSize];
    int startIndexToCopy = 0;
    for (int i = 0; i < size; i++) {
      if (byteValues[i] != null) {
        System.arraycopy(byteValues[i], 0, temp, startIndexToCopy, byteValues[i].length);
        startIndexToCopy += byteValues[i].length;
      }
    }
    return temp;
  }

  /**
   * Get Writable Double Values
   *
   * @return
   */
  public long[] getWritableLongValues() {
    if (size < longValues.length) {
      long[] temp = new long[size];
      System.arraycopy(longValues, 0, temp, 0, size);
      longValues = temp;
    }
    return longValues;
  }
}
