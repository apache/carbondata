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
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.CarbonLRUCache;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.carbondata.core.service.DictionaryService;
import org.carbondata.core.service.PathService;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;

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
   * c store path
   */
  protected String carbonStorePath;

  /**
   * @param carbonStorePath
   * @param carbonLRUCache
   */
  public AbstractDictionaryCache(String carbonStorePath, CarbonLRUCache carbonLRUCache) {
    this.carbonStorePath = carbonStorePath;
    this.carbonLRUCache = carbonLRUCache;
    initThreadPoolSize();
  }

  /**
   * This method will initialize the thread pool size to be used for creating the
   * max number of threads for a job
   */
  private void initThreadPoolSize() {
    try {
      thread_pool_size = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      thread_pool_size = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
  }

  /**
   * This method will check if dictionary and its metadata file exists for a given column
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @return
   */
  protected boolean isFileExistsForGivenColumn(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService
        .getCarbonTablePath(dictionaryColumnUniqueIdentifier.getColumnIdentifier(), carbonStorePath,
            dictionaryColumnUniqueIdentifier.getCarbonTableIdentifier());

    String dictionaryFilePath =
        carbonTablePath.getDictionaryFilePath(dictionaryColumnUniqueIdentifier
            .getColumnIdentifier().getColumnId());
    String dictionaryMetadataFilePath =
        carbonTablePath.getDictionaryMetaFilePath(dictionaryColumnUniqueIdentifier
            .getColumnIdentifier().getColumnId());
    // check if both dictionary and its metadata file exists for a given column
    return CarbonUtil.isFileExists(dictionaryFilePath) && CarbonUtil
        .isFileExists(dictionaryMetadataFilePath);
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
        .getDictionaryMetadataReader(dictionaryColumnUniqueIdentifier.getCarbonTableIdentifier(),
            dictionaryColumnUniqueIdentifier.getColumnIdentifier(), carbonStorePath);

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
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService
        .getCarbonTablePath(dictionaryColumnUniqueIdentifier.getColumnIdentifier(), carbonStorePath,
            dictionaryColumnUniqueIdentifier.getCarbonTableIdentifier());
    String dictionaryFilePath =
        carbonTablePath.getDictionaryMetaFilePath(dictionaryColumnUniqueIdentifier
            .getColumnIdentifier().getColumnId());
    FileFactory.FileType fileType = FileFactory.getFileType(dictionaryFilePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(dictionaryFilePath, fileType);
    return carbonFile;
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @param dictionaryInfo
   * @param lruCacheKey
   * @param loadSortIndex                    read and load sort index file in memory
   * @throws CarbonUtilException in case memory is not sufficient to load dictionary into memory
   */
  protected void checkAndLoadDictionaryData(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier,
      DictionaryInfo dictionaryInfo, String lruCacheKey, boolean loadSortIndex)
      throws CarbonUtilException {
    try {
      // read last segment dictionary meta chunk entry to get the end offset of file
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
            // required size will be size total size of file - offset till file is
            // already read
            long requiredSize =
                carbonDictionaryColumnMetaChunk.getEnd_offset() - dictionaryInfo.getMemorySize();
            if (requiredSize > 0) {
              boolean columnAddedToLRUCache =
                  carbonLRUCache.put(lruCacheKey, dictionaryInfo, requiredSize);
              // if column is successfully added to lru cache then only load the
              // dictionary data
              if (columnAddedToLRUCache) {
                // load dictionary data
                loadDictionaryData(dictionaryInfo, dictionaryColumnUniqueIdentifier,
                    dictionaryInfo.getMemorySize(), carbonDictionaryColumnMetaChunk.getEnd_offset(),
                    loadSortIndex);
                // set the end offset till where file is read
                dictionaryInfo
                    .setOffsetTillFileIsRead(carbonDictionaryColumnMetaChunk.getEnd_offset());
                dictionaryInfo.setFileTimeStamp(carbonFile.getLastModifiedTime());
                dictionaryInfo.setDictionaryMetaFileLength(carbonFile.getSize());
              } else {
                throw new CarbonUtilException(
                    "Cannot load dictionary into memory. Not enough memory available");
              }
            }
          }
        }
      }
      // increment the column access count
      incrementDictionaryAccessCount(dictionaryInfo);
    } catch (IOException e) {
      throw new CarbonUtilException(e.getMessage());
    }
  }

  /**
   * This method will prepare the lru cache key and return the same
   *
   * @param columnIdentifier
   * @return
   */
  protected String getLruCacheKey(String columnIdentifier, CacheType cacheType) {
    String lruCacheKey =
        columnIdentifier + CarbonCommonConstants.UNDERSCORE + cacheType.getCacheName();
    return lruCacheKey;
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
        new DictionaryCacheLoaderImpl(dictionaryColumnUniqueIdentifier.getCarbonTableIdentifier(),
            carbonStorePath);
    dictionaryCacheLoader
        .load(dictionaryInfo, dictionaryColumnUniqueIdentifier.getColumnIdentifier(),
            dictionaryChunkStartOffset, dictionaryChunkEndOffset, loadSortIndex);
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
}
