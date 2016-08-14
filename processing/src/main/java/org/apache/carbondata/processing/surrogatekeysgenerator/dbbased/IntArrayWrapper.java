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

package org.apache.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.io.Serializable;
import java.util.Arrays;

public class IntArrayWrapper implements Serializable, Comparable<IntArrayWrapper> {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 1L;

  private int[] data;

  private int primaryKey;

  public IntArrayWrapper(int[] data, int primaryKey) {
    initialize(data);
    this.primaryKey = primaryKey;
  }

  /**
   * This method is used to initialize data array
   *
   * @param data
   */
  public void initialize(int[] data) {
    if (data == null) {
      throw new IllegalArgumentException(" Data Array is NUll");
    }
    this.data = data;
  }

  /**
   * This method will be used check to ArrayWrapper object is equal or not
   *
   * @return boolean
   * equal or not
   */
  @Override public boolean equals(Object other) {
    if (other instanceof IntArrayWrapper) {
      return Arrays.equals(data, ((IntArrayWrapper) other).data);
    }
    return false;
  }

  /**
   * This method will be used to get the hascode, this will be used to the
   * index for inserting ArrayWrapper object as a key in Map
   *
   * @return hascode
   */
  @Override public int hashCode() {
    return Arrays.hashCode(data);
  }

  /**
   * This method will be used to get the long array surrogate keys
   *
   * @return data
   */
  public int[] getData() {
    return data;
  }

  /**
   * Compare method for ArrayWrapper class this will used to compare Two
   * ArrayWrapper data object, basically it will compare two surrogate keys
   * array to check which one is greater
   *
   * @param other ArrayWrapper Object
   */
  @Override public int compareTo(IntArrayWrapper other) {
    for (int i = 0; i < data.length; i++) {
      if (data[i] > other.data[i]) {
        return 1;
      } else if (data[i] < other.data[i]) {
        return -1;
      }
    }
    return 0;
  }

  public int getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(int primaryKey) {
    this.primaryKey = primaryKey;
  }
}
