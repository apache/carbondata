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

package org.apache.carbondata.core.keygenerator.columnar;

import org.apache.carbondata.core.keygenerator.KeyGenException;

/**
 * Splits the odometer key to columns.Further these columns can be stored in a columnar storage.
 */
public interface ColumnarSplitter {
  /**
   * Splits generated MDKey to multiple columns.
   *
   * @param key MDKey
   * @return Multiple columns in 2 dimensional byte array
   */
  byte[][] splitKey(byte[] key);

  /**
   * It generates and splits key to multiple columns
   *
   * @param keys
   * @return
   * @throws KeyGenException
   */
  byte[][] generateAndSplitKey(long[] keys) throws KeyGenException;

  /**
   * It generates and splits key to multiple columns
   *
   * @param keys
   * @return
   * @throws KeyGenException
   */
  byte[][] generateAndSplitKey(int[] keys) throws KeyGenException;

  /**
   * Takes the split keys and generates the surrogate key array
   *
   * @param key
   * @return
   */
  long[] getKeyArray(byte[][] key);

  /**
   * Takes the split keys and generates the surrogate key array in bytes
   *
   * @param key
   * @return
   */
  byte[] getKeyByteArray(byte[][] key);

  /**
   * Below method will be used to get the block size
   *
   * @return
   */
  int[] getBlockKeySize();

  /**
   * Below method will be used to get the total key Size of the particular block
   *
   * @param blockIndexes
   * @return
   */
  int getKeySizeByBlock(int[] blockIndexes);

}
