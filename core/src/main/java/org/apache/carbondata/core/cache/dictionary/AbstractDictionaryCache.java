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
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.apache.carbondata.core.service.CarbonCommonFactory;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSizeCalculator;

/**
 * Abstract class which implements methods common to reverse and forward dictionary cache
 */
public abstract class AbstractDictionaryCache<K extends DictionaryColumnUniqueIdentifier,
    V extends Dictionary>
    implements Cache<DictionaryColumnUniqueIdentifier, Dictionary> {

  /**
   * thread pool size to be used for dictionary data reading
   */
  protected int thread_pool_size;

  /**
   * LRU cache variable
   */
  protected CarbonLRUCache carbonLRUCache;


  /**
   * @param carbonLRUCache
   */
  public AbstractDictionaryCache(CarbonLRUCache carbonLRUCache) {
    this.carbonLRUCache = carbonLRUCache;
    initThreadPoolSize();
  }

  /**
   * This method will initialize the thread pool size to be used for creating the
   * max number of threads for a job
   */
  private void initThreadPoolSize() {
    thread_pool_size = CarbonProperties.getInstance().getNumberOfCores();
  }

  /**
   * This method will read dictionary metadata file and return the dictionary meta chunks
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return list of dictionary metadata chunks
   * @throws IOException read and close method throws IO exception
   */
  protected CarbonDictionaryColumnMetaChunk readLastChunkFromDictionaryMetadataFile(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) throws IOException {
    DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
    CarbonDictionaryMetadataReader columnMetadataReaderImpl = dictService
        .getDictionaryMetadataReader(dictionaryColumnUniqueIdentifier);

    CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk = null;
    // read metadata file
    try {
      carbonDictionaryColumnMetaChunk =
          columnMetadataReaderImpl.readLastEntryOfDictionaryMetaChunk();
    } finally {
      // close the metadata reader
      columnMetadataReaderImpl.close();
    }
    return carbonDictionaryColumnMetaChunk;
  }

  /**
   * get the dictionary column meta chunk for object already read and stored in LRU cache
   * @param dictionaryColumnUniqueIdentifier
   * @param offsetRead
   * @return
   * @throws IOException
   */
  protected long getNumRecordsInCarbonDictionaryColumnMetaChunk(
          DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, long offsetRead)
          throws IOException {
    DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
    CarbonDictionaryMetadataReader columnMetadataReaderImpl = dictService
            .getDictionaryMetadataReader(dictionaryColumnUniqueIdentifier);

    CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk = null;
    // read metadata file
    try {
      carbonDictionaryColumnMetaChunk =
              columnMetadataReaderImpl.readEntryOfDictionaryMetaChunk(offsetRead);
    } finally {
      // close the metadata reader
      columnMetadataReaderImpl.close();
    }
    return carbonDictionaryColumnMetaChunk.getMax_surrogate_key();
  }

  /**
   * This method will validate dictionary metadata file for any modification
   *
   * @param carbonFile
   * @param fileTimeStamp
   * @param endOffset
   * @return
   */
  private boolean isDictionaryMetaFileModified(CarbonFile carbonFile, long fileTimeStamp,
      long endOffset) {
    return carbonFile.isFileModified(fileTimeStamp, endOffset);
  }

  /**
   * This method will return the carbon file objetc based on its type (local, HDFS)
   *
   * @param dictionaryColumnUniqueIdentifier
   * @return
   */
  private CarbonFile getDictionaryMetaCarbonFile(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) throws IOException {
    String dictionaryFilePath = dictionaryColumnUniqueIdentifier.getDictionaryFilePath();
    FileFactory.FileType fileType = FileFactory.getFileType(dictionaryFilePath);
    CarbonFile dictFile = FileFactory.getCarbonFile(dictionaryFilePath, fileType);
    // When rename table triggered parallely with select query, dictionary files may not exist
    if (!dictFile.exists()) {
      throw new IOException("Dictionary file does not exist: " + dictionaryFilePath);
    }
    return dictFile;
  }

  protected long getSortIndexSize(long numOfRecords) {
    // sort index has sort index and reverse sort index,each is 4 byte integer.
    // 32 byte is the array header of both the integer arrays
    return numOfRecords * ObjectSizeCalculator.estimate(0, 16) * 2 + 32;
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @param dictionaryInfo
   * @param lruCacheKey
   * @throws IOException                    in case memory is not sufficient to load dictionary
   *                                        into memory
   */
  protected void checkAndLoadDictionaryData(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier,
      DictionaryInfo dictionaryInfo, String lruCacheKey, boolean loadSortIndex)
      throws IOException {
    // read last segm
    // ent dictionary meta chunk entry to get the end offset of file
    CarbonFile carbonFile = getDictionaryMetaCarbonFile(dictionaryColumnUniqueIdentifier);
    boolean dictionaryMetaFileModified =
        isDictionaryMetaFileModified(carbonFile, dictionaryInfo.getFileTimeStamp(),
            dictionaryInfo.getDictionaryMetaFileLength());
    // if dictionary metadata file is modified then only read the last entry from dictionary
    // meta file
    if (dictionaryMetaFileModified) {
      synchronized (dictionaryInfo) {
        carbonFile = getDictionaryMetaCarbonFile(dictionaryColumnUniqueIdentifier);
        dictionaryMetaFileModified =
            isDictionaryMetaFileModified(carbonFile, dictionaryInfo.getFileTimeStamp(),
                dictionaryInfo.getDictionaryMetaFileLength());
        // Double Check :
        // if dictionary metadata file is modified then only read the last entry from dictionary
        // meta file
        if (dictionaryMetaFileModified) {
          CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk =
              readLastChunkFromDictionaryMetadataFile(dictionaryColumnUniqueIdentifier);

          long requiredSize = getEstimatedDictionarySize(dictionaryInfo,
              carbonDictionaryColumnMetaChunk,
              dictionaryColumnUniqueIdentifier, loadSortIndex);

          if (requiredSize > 0) {
            dictionaryInfo.setMemorySize(requiredSize);
            boolean colCanBeAddedToLRUCache =
                    carbonLRUCache.tryPut(lruCacheKey, requiredSize);
            // if column can be added to lru cache then only load the
            // dictionary data
            if (colCanBeAddedToLRUCache) {
              // load dictionary data
              loadDictionaryData(dictionaryInfo, dictionaryColumnUniqueIdentifier,
                      dictionaryInfo.getOffsetTillFileIsRead(),
                      carbonDictionaryColumnMetaChunk.getEnd_offset(),
                      loadSortIndex);
              // set the end offset till where file is read
              dictionaryInfo
                      .setOffsetTillFileIsRead(carbonDictionaryColumnMetaChunk.getEnd_offset());
              long updateRequiredSize = ObjectSizeCalculator.estimate(dictionaryInfo, requiredSize);
              dictionaryInfo.setMemorySize(updateRequiredSize);
              if (!carbonLRUCache.put(lruCacheKey, dictionaryInfo, updateRequiredSize)) {
                throw new DictionaryBuilderException(
                        "Cannot load dictionary into memory. Not enough memory available");
              }
              dictionaryInfo.setFileTimeStamp(carbonFile.getLastModifiedTime());
              dictionaryInfo.setDictionaryMetaFileLength(carbonFile.getSize());
            } else {
              throw new DictionaryBuilderException(
                      "Cannot load dictionary into memory. Not enough memory available");
            }
          }
        }
      }
    }
    // increment the column access count
    incrementDictionaryAccessCount(dictionaryInfo);
  }

  /**
   * This method will prepare the lru cache key and return the same
   *
   * @param columnIdentifier
   * @return
   */
  protected String getLruCacheKey(String columnIdentifier, CacheType cacheType) {
    return columnIdentifier + CarbonCommonConstants.UNDERSCORE + cacheType.getCacheName();
  }

  /**
   * This method will check and load the dictionary file in memory for a given column
   *
   * @param dictionaryInfo                   holds dictionary information and data
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @param dictionaryChunkStartOffset       start offset from where dictionary file has to
   *                                         be read
   * @param dictionaryChunkEndOffset         end offset till where dictionary file has to
   *                                         be read
   * @param loadSortIndex
   * @throws IOException
   */
  private void loadDictionaryData(DictionaryInfo dictionaryInfo,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier,
      long dictionaryChunkStartOffset, long dictionaryChunkEndOffset, boolean loadSortIndex)
      throws IOException {
    DictionaryCacheLoader dictionaryCacheLoader =
        new DictionaryCacheLoaderImpl(dictionaryColumnUniqueIdentifier);
    dictionaryCacheLoader
        .load(dictionaryInfo, dictionaryChunkStartOffset, dictionaryChunkEndOffset, loadSortIndex);
  }

  /**
   * This method will increment the access count for a given dictionary column
   *
   * @param dictionaryInfo
   */
  protected void incrementDictionaryAccessCount(DictionaryInfo dictionaryInfo) {
    dictionaryInfo.incrementAccessCount();
  }

  /**
   * This method will update the dictionary acceess count which is required for its removal
   * from column LRU cache
   *
   * @param dictionaryList
   */
  protected void clearDictionary(List<Dictionary> dictionaryList) {
    for (Dictionary dictionary : dictionaryList) {
      dictionary.clear();
    }
  }

  /**
   * calculate the probable size of Dictionary in java heap
   * Use the value to check if can be added to lru cache
   * This helps to avoid unnecessary loading of dictionary files
   * if estimated size more than that can be fit into lru cache
   * Estimated size can be less or greater than the actual size
   * due to java optimizations
   * @param dictionaryInfo
   * @param carbonDictionaryColumnMetaChunk
   * @param dictionaryColumnUniqueIdentifier
   * @param readSortIndexSize
   * @return
   * @throws IOException
   */
  protected long getEstimatedDictionarySize(DictionaryInfo dictionaryInfo,
      CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, boolean
      readSortIndexSize) throws IOException {
    return 0;
  }
}
