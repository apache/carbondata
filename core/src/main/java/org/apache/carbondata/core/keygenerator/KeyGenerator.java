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

package org.apache.carbondata.core.keygenerator;

import java.io.Serializable;
import java.util.Comparator;

/**
 * It generates the key by using multiple keys(typically multiple dimension keys
 * are combined to form a single key). And it can return the individual
 * key(dimensional key) out of combined key.
 */
public interface KeyGenerator extends Serializable, Comparator<byte[]> {
  /**
   * It generates the single key aka byte array from multiple keys.
   *
   * @param keys
   * @return byte array
   * @throws KeyGenException
   */
  byte[] generateKey(long[] keys) throws KeyGenException;

  /**
   * It generates the single key aka byte array from multiple keys.
   *
   * @param keys
   * @return
   * @throws KeyGenException
   */
  byte[] generateKey(int[] keys) throws KeyGenException;

  /**
   * It gets array of keys out of single key aka byte array
   *
   * @param key
   * @return array of keys.
   */
  long[] getKeyArray(byte[] key);

  /**
   * It gets array of keys out of single key aka byte array
   *
   * @param key
   * @param offset
   * @return array of keys.
   */
  long[] getKeyArray(byte[] key, int offset);

  /**
   * It gets array of keys out of single key aka byte array
   *
   * @param key
   * @param maskedByteRanges
   * @return array of keys
   */
  long[] getKeyArray(byte[] key, int[] maskedByteRanges);

  /**
   * It gets the key in the specified index from the single key aka byte array
   *
   * @param key
   * @param index of key.
   * @return key
   */
  long getKey(byte[] key, int index);

  /**
   * Gives the key size in number of bytes.
   */
  int getKeySizeInBytes();


  /**
   * returns key bytes offset
   *
   * @param index
   * @return
   */
  int[] getKeyByteOffsets(int index);

  /**
   * returns the dimension count
   *
   * @return
   */
  int getDimCount();

}