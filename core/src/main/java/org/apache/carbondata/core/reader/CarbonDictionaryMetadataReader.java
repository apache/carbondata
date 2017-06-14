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
import java.io.IOException;
import java.util.List;

/**
 * dictionary metadata reader interface which declares methods to read dictionary metadata
 */
public interface CarbonDictionaryMetadataReader extends Closeable {

  /**
   * This method will be used to read complete metadata file.
   * Applicable scenarios:
   * 1. Query execution. Whenever a query is executed then to read the dictionary file
   * and define the query scope first dictionary metadata has to be read first.
   * 2. If dictionary file is read using start and end offset then using this meta list
   * we can count the total number of dictionary chunks present between the 2 offsets
   *
   * @return list of all dictionary meta chunks which contains information for each segment
   * @throws IOException if an I/O error occurs
   */
  List<CarbonDictionaryColumnMetaChunk> read() throws IOException;

  /**
   * This method will be used to read only the last entry of dictionary meta chunk.
   * Applicable scenarios :
   * 1. Global dictionary generation for incremental load. In this case only the
   * last dictionary chunk meta entry has to be read to calculate min, max surrogate
   * key and start and end offset for the new dictionary chunk.
   * 2. Truncate operation. While writing dictionary file in case of incremental load
   * dictionary file needs to be validated for any inconsistency. Here end offset of last
   * dictionary chunk meta is validated with file size.
   *
   * @return last segment entry for dictionary chunk
   * @throws IOException if an I/O error occurs
   */
  CarbonDictionaryColumnMetaChunk readLastEntryOfDictionaryMetaChunk() throws IOException;

  /**
   * This method will be used to read the last dictionary meta chunk ending at end_Offset.
   * Applicable scenarios :
   * 1. When loading into LRU cache, we need to calculate the size of Object in memory,for
   * this we need the number of records already loaded into LRU cache, so that we can calculate
   * the memory required for incremental load
   *
   * @return last segment entry for dictionary chunk
   * @throws IOException if an I/O error occurs
   */
  CarbonDictionaryColumnMetaChunk readEntryOfDictionaryMetaChunk(long end_Offset)
          throws IOException;
}
