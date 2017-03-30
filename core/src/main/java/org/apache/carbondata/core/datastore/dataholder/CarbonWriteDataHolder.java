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
   * byteValues for no dictionary.
   */
  private byte[][][] byteValuesForNonDictionary;

  /**
   * byteValues
   */
  private byte[][][] columnByteValues;

  /**
   * size
   */
  private int size;

  /**
   * totalSize
   */
  private int totalSize;

  /**
   * Method to initialise double array
   *
   * @param size
   */
  public void initialiseDoubleValues(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Invalid array size");
    }
    doubleValues = new double[size];
  }

  public void reset() {
    size = 0;
    totalSize = 0;
  }

  /**
   * Method to initialise double array
   *
   * @param size
   */
  public void initialiseByteArrayValues(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Invalid array size");
    }

    byteValues = new byte[size][];
    columnByteValues = new byte[size][][];
  }

  /**
   * Method to initialise byte array
   *
   * @param size
   */
  public void initialiseByteArrayValuesForKey(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Invalid array size");
    }

    byteValues = new byte[size][];
  }

  public void initialiseByteArrayValuesForNonDictionary(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Invalid array size");
    }

    byteValuesForNonDictionary = new byte[size][][];
  }

  /**
   * Method to initialise long array
   *
   * @param size
   */
  public void initialiseLongValues(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Invalid array size");
    }
    longValues = new long[size];
  }

  /**
   * set double value by index
   *
   * @param index
   * @param value
   */
  public void setWritableDoubleValueByIndex(int index, Object value) {
    doubleValues[index] = (Double) value;
    size++;
  }

  /**
   * set double value by index
   *
   * @param index
   * @param value
   */
  public void setWritableLongValueByIndex(int index, Object value) {
    longValues[index] = (Long) value;
    size++;
  }

  /**
   * set byte array value by index
   *
   * @param index
   * @param value
   */
  public void setWritableByteArrayValueByIndex(int index, byte[] value) {
    byteValues[index] = value;
    size++;
    if (null != value) totalSize += value.length;
  }

  public void setWritableNonDictByteArrayValueByIndex(int index, byte[][] value) {
    byteValuesForNonDictionary[index] = value;
    size++;
    if (null != value) totalSize += value.length;
  }

  /**
   * set byte array value by index
   */
  public void setWritableByteArrayValueByIndex(int index, int mdKeyIndex, Object[] columnData) {
    int l = 0;
    columnByteValues[index] = new byte[columnData.length - (mdKeyIndex + 1)][];
    for (int i = mdKeyIndex + 1; i < columnData.length; i++) {
      columnByteValues[index][l++] = (byte[]) columnData[i];
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
      System.arraycopy(byteValues[i], 0, temp, startIndexToCopy, byteValues[i].length);
      startIndexToCopy += byteValues[i].length;
    }
    return temp;
  }

  public byte[][] getByteArrayValues() {
    if (size < byteValues.length) {
      byte[][] temp = new byte[size][];
      System.arraycopy(byteValues, 0, temp, 0, size);
      byteValues = temp;
    }
    return byteValues;
  }

  public byte[][][] getNonDictByteArrayValues() {
    if (size < byteValuesForNonDictionary.length) {
      byte[][][] temp = new byte[size][][];
      System.arraycopy(byteValuesForNonDictionary, 0, temp, 0, size);
      byteValuesForNonDictionary = temp;
    }
    return byteValuesForNonDictionary;
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
