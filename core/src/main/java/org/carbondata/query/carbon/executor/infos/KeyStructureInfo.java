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
package org.carbondata.query.carbon.executor.infos;

import org.carbondata.core.keygenerator.KeyGenerator;

/**
 * Below class will store the structure of the key
 * used during query execution
 */
public class KeyStructureInfo {

  /**
   * it's actually a latest key generator
   * last table block as this key generator will be used to
   * to update the mdkey of the older slice with the new slice
   */
  private KeyGenerator keyGenerator;

  /**
   * mask bytes ranges for the query
   */
  private int[] maskByteRanges;

  /**
   * masked bytes of the query
   */
  private int[] maskedBytes;

  /**
   * max key for query execution
   */
  private byte[] maxKey;

  /**
   * mdkey start index of block
   */
  private int blockMdKeyStartOffset;

  /**
   * @return the keyGenerator
   */
  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  /**
   * @param keyGenerator the keyGenerator to set
   */
  public void setKeyGenerator(KeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;
  }

  /**
   * @return the maskByteRanges
   */
  public int[] getMaskByteRanges() {
    return maskByteRanges;
  }

  /**
   * @param maskByteRanges the maskByteRanges to set
   */
  public void setMaskByteRanges(int[] maskByteRanges) {
    this.maskByteRanges = maskByteRanges;
  }

  /**
   * @return the maskedBytes
   */
  public int[] getMaskedBytes() {
    return maskedBytes;
  }

  /**
   * @param maskedBytes the maskedBytes to set
   */
  public void setMaskedBytes(int[] maskedBytes) {
    this.maskedBytes = maskedBytes;
  }

  /**
   * @return the maxKey
   */
  public byte[] getMaxKey() {
    return maxKey;
  }

  /**
   * @param maxKey the maxKey to set
   */
  public void setMaxKey(byte[] maxKey) {
    this.maxKey = maxKey;
  }

  /**
   * @param startOffset
   */
  public void setBlockMdKeyStartOffset(int startOffset) {
    this.blockMdKeyStartOffset = startOffset;
  }

  /**
   * @return
   */
  public int getBlockMdKeyStartOffset() {
    return this.blockMdKeyStartOffset;
  }
}
