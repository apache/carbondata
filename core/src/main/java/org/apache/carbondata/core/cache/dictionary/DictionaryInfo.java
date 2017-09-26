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

import java.util.List;

import org.apache.carbondata.core.cache.Cacheable;

/**
 * An interface which holds dictionary information like end offset,
 * file timestamp for one column
 */
public interface DictionaryInfo extends Cacheable, Dictionary {

  /**
   * This method will increment the access count for a column by 1
   * whenever a column is getting used in query or incremental data load
   */
  void incrementAccessCount();

  /**
   * This method will update the end offset of file everytime a file is read
   *
   * @param offsetTillFileIsRead
   */
  void setOffsetTillFileIsRead(long offsetTillFileIsRead);

  /**
   * offset till the file is read
   * @return
   */
  long getOffsetTillFileIsRead();

  /**
   * the memory size of this object after loaded into memory
   * @param memorySize
   */
  void setMemorySize(long memorySize);

  /**
   * This method will update the timestamp of a file if a file is modified
   * like in case of incremental load
   *
   * @param fileTimeStamp
   */
  void setFileTimeStamp(long fileTimeStamp);

  /**
   * This method will add a new dictionary chunk to existing list of dictionary chunks
   *
   * @param dictionaryChunk
   */
  void addDictionaryChunk(List<byte[]> dictionaryChunk);

  /**
   * This method will return the size of of last dictionary chunk so that only that many
   * values are read from the dictionary reader
   *
   * @return size of last dictionary chunk
   */
  int getSizeOfLastDictionaryChunk();

  /**
   * This method will set the sort order index of a dictionary column.
   * Sort order index if the index of dictionary values after they are sorted.
   *
   * @param sortOrderIndex
   */
  void setSortOrderIndex(List<Integer> sortOrderIndex);

  /**
   * This method will set the sort reverse index of a dictionary column.
   * Sort reverse index is the index of dictionary values before they are sorted.
   *
   * @param sortReverseOrderIndex
   */
  void setSortReverseOrderIndex(List<Integer> sortReverseOrderIndex);

  /**
   * dictionary metadata file length which will be set whenever we reload dictionary
   * data from disk
   *
   * @param dictionaryMetaFileLength length of dictionary metadata file
   */
  void setDictionaryMetaFileLength(long dictionaryMetaFileLength);

  /**
   * Dictionary meta file offset which will be read to check whether length of dictionary
   * meta file has been modified
   *
   * @return
   */
  long getDictionaryMetaFileLength();
}
