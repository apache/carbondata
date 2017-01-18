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
package org.apache.carbondata.core.datastore;

/**
 * Index class to store the index of the segment blocklet infos
 */
public class IndexKey {

  /**
   * key which is generated from key generator
   */
  private byte[] dictionaryKeys;

  /**
   * key which was no generated using key generator
   * <Index of FirstKey (2 bytes)><Index of SecondKey (2 bytes)><Index of NKey (2 bytes)>
   * <First Key ByteArray><2nd Key ByteArray><N Key ByteArray>
   */
  private byte[] noDictionaryKeys;

  public IndexKey(byte[] dictionaryKeys, byte[] noDictionaryKeys) {
    this.dictionaryKeys = dictionaryKeys;
    this.noDictionaryKeys = noDictionaryKeys;
    if (null == dictionaryKeys) {
      this.dictionaryKeys = new byte[0];
    }
    if (null == noDictionaryKeys) {
      this.noDictionaryKeys = new byte[0];
    }
  }

  /**
   * @return the dictionaryKeys
   */
  public byte[] getDictionaryKeys() {
    return dictionaryKeys;
  }

  /**
   * @return the noDictionaryKeys
   */
  public byte[] getNoDictionaryKeys() {
    return noDictionaryKeys;
  }
}
