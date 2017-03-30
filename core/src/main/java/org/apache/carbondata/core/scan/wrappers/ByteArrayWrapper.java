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
package org.apache.carbondata.core.scan.wrappers;

import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;

/**
 * This class will store the dimension column data when query is executed
 * This can be used as a key for aggregation
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {

  /**
   * to store key which is generated using
   * key generator
   */
  protected byte[] dictionaryKey;

  /**
   * to store no dictionary column data
   */
  protected byte[][] complexTypesKeys;

  /**
   * to store no dictionary column data
   */
  protected byte[][] noDictionaryKeys;

  /**
   * contains value of implicit columns in byte array format
   */
  protected byte[] implicitColumnByteArray;

  public ByteArrayWrapper() {
  }

  /**
   * @return the dictionaryKey
   */
  public byte[] getDictionaryKey() {
    return dictionaryKey;
  }

  /**
   * @param dictionaryKey the dictionaryKey to set
   */
  public void setDictionaryKey(byte[] dictionaryKey) {
    this.dictionaryKey = dictionaryKey;
  }

  /**
   * @param noDictionaryKeys the noDictionaryKeys to set
   */
  public void setNoDictionaryKeys(byte[][] noDictionaryKeys) {
    this.noDictionaryKeys = noDictionaryKeys;
  }

  /**
   * to get the no dictionary column data
   *
   * @param index of the no dictionary key
   * @return no dictionary key for the index
   */
  public byte[] getNoDictionaryKeyByIndex(int index) {
    return this.noDictionaryKeys[index];
  }

  /**
   * to get the no dictionary column data
   *
   * @return no dictionary keys
   */
  public byte[][] getNoDictionaryKeys() {
    return this.noDictionaryKeys;
  }

  /**
   * to get the no dictionary column data
   *
   * @param index of the no dictionary key
   * @return no dictionary key for the index
   */
  public byte[] getComplexTypeByIndex(int index) {
    return this.complexTypesKeys[index];
  }

  /**
   * to generate the hash code
   */
  @Override public int hashCode() {
    // first generate the has code of the dictionary column
    int len = dictionaryKey.length;
    int result = 1;
    for (int j = 0; j < len; j++) {
      result = 31 * result + dictionaryKey[j];
    }
    // then no dictionary column
    for (byte[] directSurrogateValue : noDictionaryKeys) {
      for (int i = 0; i < directSurrogateValue.length; i++) {
        result = 31 * result + directSurrogateValue[i];
      }
    }
    // then for complex type
    for (byte[] complexTypeKey : complexTypesKeys) {
      for (int i = 0; i < complexTypeKey.length; i++) {
        result = 31 * result + complexTypeKey[i];
      }
    }
    return result;
  }

  /**
   * to validate the two
   *
   * @param other object
   */
  @Override public boolean equals(Object other) {
    if (null == other || !(other instanceof ByteArrayWrapper)) {
      return false;
    }
    boolean result = false;
    // Comparison will be as follows
    // first compare the no dictionary column
    // if it is not equal then return false
    // if it is equal then compare the complex column
    // if it is also equal then compare dictionary column
    byte[][] noDictionaryKeysOther = ((ByteArrayWrapper) other).noDictionaryKeys;
    if (noDictionaryKeysOther.length != noDictionaryKeys.length) {
      return false;
    } else {
      for (int i = 0; i < noDictionaryKeys.length; i++) {
        result = UnsafeComparer.INSTANCE.equals(noDictionaryKeys[i], noDictionaryKeysOther[i]);
        if (!result) {
          return false;
        }
      }
    }

    byte[][] complexTypesKeysOther = ((ByteArrayWrapper) other).complexTypesKeys;
    if (complexTypesKeysOther.length != complexTypesKeys.length) {
      return false;
    } else {
      for (int i = 0; i < complexTypesKeys.length; i++) {
        result = UnsafeComparer.INSTANCE.equals(complexTypesKeys[i], complexTypesKeysOther[i]);
        if (!result) {
          return false;
        }
      }
    }

    return UnsafeComparer.INSTANCE.equals(dictionaryKey, ((ByteArrayWrapper) other).dictionaryKey);
  }

  /**
   * Compare method for ByteArrayWrapper class this will used to compare Two
   * ByteArrayWrapper data object, basically it will compare two byte array
   *
   * @param other ArrayWrapper Object
   */
  @Override public int compareTo(ByteArrayWrapper other) {
    // compare will be as follows
    //compare dictionary column
    // then no dictionary column
    // then complex type column data
    int compareTo = UnsafeComparer.INSTANCE.compareTo(dictionaryKey, other.dictionaryKey);
    if (compareTo == 0) {
      for (int i = 0; i < noDictionaryKeys.length; i++) {
        compareTo =
            UnsafeComparer.INSTANCE.compareTo(noDictionaryKeys[i], other.noDictionaryKeys[i]);
        if (compareTo != 0) {
          return compareTo;
        }
      }
    }
    if (compareTo == 0) {
      for (int i = 0; i < complexTypesKeys.length; i++) {
        compareTo =
            UnsafeComparer.INSTANCE.compareTo(complexTypesKeys[i], other.complexTypesKeys[i]);
        if (compareTo != 0) {
          return compareTo;
        }
      }
    }
    return compareTo;
  }

  /**
   * @return the complexTypesKeys
   */
  public byte[][] getComplexTypesKeys() {
    return complexTypesKeys;
  }

  /**
   * @param complexTypesKeys the complexTypesKeys to set
   */
  public void setComplexTypesKeys(byte[][] complexTypesKeys) {
    this.complexTypesKeys = complexTypesKeys;
  }

  /**
   * @return
   */
  public byte[] getImplicitColumnByteArray() {
    return implicitColumnByteArray;
  }

  /**
   * @param implicitColumnByteArray
   */
  public void setImplicitColumnByteArray(byte[] implicitColumnByteArray) {
    this.implicitColumnByteArray = implicitColumnByteArray;
  }
}
