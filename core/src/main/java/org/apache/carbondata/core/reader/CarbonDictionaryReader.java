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

package org.apache.carbondata.core.reader;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 * dictionary reader interface which declares methods for
 * reading carbon dictionary files
 */
public interface CarbonDictionaryReader extends Closeable {
  /**
   * This method should be used when complete dictionary data needs to be read.
   * Applicable scenarios :
   * 1. Global dictionary generation in case of incremental load
   * 2. Reading dictionary file on first time query
   * 3. Loading a dictionary column in memory based on query requirement.
   * This is a case where carbon column cache feature is enabled in which a
   * column dictionary is read if it is present in the query.
   *
   * @return list of byte array. Each byte array is unique dictionary value
   */
  List<byte[]> read();

  /**
   * This method should be used when data has to be read from a given offset.
   * Applicable scenarios :
   * 1. Incremental data load. If column dictionary is already loaded in memory
   * and incremental load is done, then for the new query only new dictionary data
   * has to be read form memory.
   *
   * @param startOffset start offset of dictionary file
   * @return list of byte array. Each byte array is unique dictionary value
   */
  List<byte[]> read(long startOffset);

  /**
   * This method will be used to read data between given start and end offset.
   * Applicable scenarios:
   * 1. Truncate operation. If there is any inconsistency while writing the dictionary file
   * then we can give the start and end offset till where the data has to be retained.
   *
   * @param startOffset start offset of dictionary file
   * @param endOffset   end offset of dictionary file
   * @return iterator over byte array. Each byte array is unique dictionary value
   */
  Iterator<byte[]> read(long startOffset, long endOffset);
}
