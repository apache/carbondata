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

package org.apache.carbondata.core.cache.dictionary;

import java.util.Arrays;

import org.apache.carbondata.core.util.ByteUtil;

import net.jpountz.xxhash.XXHash32;

/**
 * class that holds the byte array and overrides equals and hashcode method which
 * will be useful for object comparison
 */
public class DictionaryByteArrayWrapper {

  /**
   * dictionary value as byte array
   */
  private byte[] data;

  /**
   * hashing algorithm to calculate hash code
   */
  private XXHash32 xxHash32;

  /**
   * @param data
   */
  public DictionaryByteArrayWrapper(byte[] data) {
    this.data = data;
  }

  /**
   * @param data
   * @param xxHash32
   */
  public DictionaryByteArrayWrapper(byte[] data, XXHash32 xxHash32) {
    this(data);
    this.xxHash32 = xxHash32;
  }

  /**
   * This method will compare 2 DictionaryByteArrayWrapper objects
   *
   * @param other
   * @return
   */
  @Override public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    DictionaryByteArrayWrapper otherObjectToCompare = (DictionaryByteArrayWrapper) other;
    if (data.length != otherObjectToCompare.data.length) {
      return false;
    }
    return ByteUtil.UnsafeComparer.INSTANCE.equals(data, otherObjectToCompare.data);

  }

  /**
   * This method will calculate the hash code for given data
   *
   * @return
   */
  @Override public int hashCode() {
    if (null != xxHash32) {
      return xxHash32.hash(data, 0, data.length, 0);
    }
    int result = Arrays.hashCode(data);
    result = 31 * result;
    return result;
  }

  public byte[] getData() {
    return data;
  }
}
