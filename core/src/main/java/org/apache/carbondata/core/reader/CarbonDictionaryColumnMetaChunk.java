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

/**
 * A wrapper class for thrift class ColumnDictionaryChunkMeta which will
 * contain data like min and max surrogate key, start and end offset, chunk count
 */
public class CarbonDictionaryColumnMetaChunk {

  /**
   * Minimum value surrogate key for a segment
   */
  private int min_surrogate_key;

  /**
   * Max value of surrogate key for a segment
   */
  private int max_surrogate_key;

  /**
   * start offset of dictionary chunk in dictionary file for a segment
   */
  private long start_offset;

  /**
   * end offset of dictionary chunk in dictionary file for a segment
   */
  private long end_offset;

  /**
   * count of dictionary chunks for a segment
   */
  private int chunk_count;

  /**
   * constructor
   *
   * @param min_surrogate_key Minimum value surrogate key for a segment
   * @param max_surrogate_key Maximum value surrogate key for a segment
   * @param start_offset      start offset of dictionary chunk in dictionary file for a segment
   * @param end_offset        end offset of dictionary chunk in dictionary file for a segment
   * @param chunk_count       count of dictionary chunks for a segment
   */
  public CarbonDictionaryColumnMetaChunk(int min_surrogate_key, int max_surrogate_key,
      long start_offset, long end_offset, int chunk_count) {
    this.min_surrogate_key = min_surrogate_key;
    this.max_surrogate_key = max_surrogate_key;
    this.start_offset = start_offset;
    this.end_offset = end_offset;
    this.chunk_count = chunk_count;
  }

  /**
   * @return min surrogate key
   */
  public int getMin_surrogate_key() {
    return min_surrogate_key;
  }

  /**
   * @return max surrogate key
   */
  public int getMax_surrogate_key() {
    return max_surrogate_key;
  }

  /**
   * @return start offset
   */
  public long getStart_offset() {
    return start_offset;
  }

  /**
   * @return end offset
   */
  public long getEnd_offset() {
    return end_offset;
  }

  /**
   * @return chunk count
   */
  public int getChunk_count() {
    return chunk_count;
  }
}


