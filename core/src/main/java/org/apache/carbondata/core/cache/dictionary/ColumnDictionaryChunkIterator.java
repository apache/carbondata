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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.format.ColumnDictionaryChunk;

/**
 * This class is a wrapper over column dictionary chunk thrift object.
 * The wrapper class wraps the list<ColumnDictionaryChunk> and provides an API
 * to fill the byte array into list
 */
public class ColumnDictionaryChunkIterator extends CarbonIterator {

  /**
   * list of dictionaryChunks
   */
  private List<ColumnDictionaryChunk> columnDictionaryChunks;

  /**
   * size of the list
   */
  private int size;

  /**
   * Current index of the list
   */
  private int currentSize;

  /**
   * variable holds the count of elements already iterated
   */
  private int iteratorIndex;

  /**
   * variable holds the current index of List<List<byte[]>> being traversed
   */
  private int outerIndex;

  /**
   * Constructor of ColumnDictionaryChunkIterator
   *
   * @param columnDictionaryChunks
   */
  public ColumnDictionaryChunkIterator(List<ColumnDictionaryChunk> columnDictionaryChunks) {
    this.columnDictionaryChunks = columnDictionaryChunks;
    for (ColumnDictionaryChunk dictionaryChunk : columnDictionaryChunks) {
      this.size += dictionaryChunk.getValues().size();
    }
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   * (In other words, returns {@code true} if {@link #next} would
   * return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return (currentSize < size);
  }

  /**
   * Returns the next element in the iteration.
   * The method pics the next elements from the first inner list till first is not finished, pics
   * the second inner list ...
   *
   * @return the next element in the iteration
   */
  @Override public byte[] next() {
    if (iteratorIndex >= columnDictionaryChunks.get(outerIndex).getValues().size()) {
      iteratorIndex = 0;
      outerIndex++;
    }
    ByteBuffer buffer = columnDictionaryChunks.get(outerIndex).getValues().get(iteratorIndex);
    byte[] value = buffer.array();
    currentSize++;
    iteratorIndex++;
    return value;
  }
}
