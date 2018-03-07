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

import java.io.IOException;

public interface DictionaryCacheLoader {

  /**
   * This method will load the dictionary data for a given columnIdentifier
   *
   * @param dictionaryInfo             dictionary info object which will hold the required data
   *                                   for a given column
   * @param dictionaryChunkStartOffset start offset from where dictionary file has to
   *                                   be read
   * @param dictionaryChunkEndOffset   end offset till where dictionary file has to
   *                                   be read
   * @param loadSortIndex              flag to indicate whether the sort index file has to be
   *                                   read in memory after dictionary loading
   * @throws IOException
   */
  void load(DictionaryInfo dictionaryInfo, long dictionaryChunkStartOffset,
      long dictionaryChunkEndOffset, boolean loadSortIndex)
      throws IOException;
}
