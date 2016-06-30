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

package org.carbondata.core.cache.dictionary;

import java.io.IOException;
import java.util.List;

import org.carbondata.common.factory.CarbonCommonFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.reader.CarbonDictionaryReader;
import org.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.carbondata.core.service.DictionaryService;

/**
 * This class is responsible for loading the dictionary data for given columns
 */
public class DictionaryCacheLoaderImpl implements DictionaryCacheLoader {

  /**
   * carbon table identifier
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  /**
   * carbon store path
   */
  private String carbonStorePath;

  /**
   * @param carbonTableIdentifier fully qualified table name
   * @param carbonStorePath       hdfs store path
   */
  public DictionaryCacheLoaderImpl(CarbonTableIdentifier carbonTableIdentifier,
      String carbonStorePath) {
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.carbonStorePath = carbonStorePath;
  }

  /**
   * This method will load the dictionary data for a given columnIdentifier
   *
   * @param dictionaryInfo             dictionary info object which will hold the required data
   *                                   for a given column
   * @param columnIdentifier           column unique identifier
   * @param dictionaryChunkStartOffset start offset from where dictionary file has to
   *                                   be read
   * @param dictionaryChunkEndOffset   end offset till where dictionary file has to
   *                                   be read
   * @param loadSortIndex              flag to indicate whether the sort index file has to be
   *                                   read in memory after dictionary loading
   * @throws IOException
   */
  @Override public void load(DictionaryInfo dictionaryInfo, ColumnIdentifier columnIdentifier,
      long dictionaryChunkStartOffset, long dictionaryChunkEndOffset, boolean loadSortIndex)
      throws IOException {
    List<byte[]> dictionaryChunk =
        load(columnIdentifier, dictionaryChunkStartOffset, dictionaryChunkEndOffset);
    if (loadSortIndex) {
      readSortIndexFile(dictionaryInfo, columnIdentifier);
    }
    dictionaryInfo.addDictionaryChunk(dictionaryChunk);
  }

  /**
   * This method will load the dictionary data between a given start and end offset
   *
   * @param columnIdentifier column unique identifier
   * @param startOffset      start offset of dictionary file
   * @param endOffset        end offset of dictionary file
   * @return list of dictionary value
   * @throws IOException
   */
  private List<byte[]> load(ColumnIdentifier columnIdentifier, long startOffset, long endOffset)
      throws IOException {
    CarbonDictionaryReader dictionaryReader = getDictionaryReader(columnIdentifier);
    List<byte[]> dictionaryValue = null;
    try {
      dictionaryValue = dictionaryReader.read(startOffset, endOffset);
    } finally {
      dictionaryReader.close();
    }
    return dictionaryValue;
  }

  /**
   * This method will read the sort index file and load into memory
   *
   * @param dictionaryInfo
   * @param columnIdentifier
   * @throws IOException
   */
  private void readSortIndexFile(DictionaryInfo dictionaryInfo, ColumnIdentifier columnIdentifier)
      throws IOException {
    CarbonDictionarySortIndexReader sortIndexReader = getSortIndexReader(columnIdentifier);
    try {
      dictionaryInfo.setSortOrderIndex(sortIndexReader.readSortIndex());
      dictionaryInfo.setSortReverseOrderIndex(sortIndexReader.readInvertedSortIndex());
    } finally {
      sortIndexReader.close();
    }
  }

  /**
   * This method will create a dictionary reader instance to read the dictionary file
   *
   * @param columnIdentifier unique column identifier
   * @return carbon dictionary reader instance
   */
  private CarbonDictionaryReader getDictionaryReader(ColumnIdentifier columnIdentifier) {
    DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
    return dictService
        .getDictionaryReader(carbonTableIdentifier, columnIdentifier, carbonStorePath);
  }

  /**
   * @param columnIdentifier unique column identifier
   * @return sort index reader instance
   */
  private CarbonDictionarySortIndexReader getSortIndexReader(ColumnIdentifier columnIdentifier) {
    DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
    return dictService
        .getDictionarySortIndexReader(carbonTableIdentifier, columnIdentifier, carbonStorePath);
  }
}
