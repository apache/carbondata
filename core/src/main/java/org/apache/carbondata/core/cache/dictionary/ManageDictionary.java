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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class is aimed at managing dictionary files for any new addition and deletion
 * and calling of clear cache for the non existing dictionary files
 */
public class ManageDictionary {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ManageDictionary.class.getName());

  /**
   * This method will delete the dictionary files for the given column IDs and
   * clear the dictionary cache
   *
   * @param columnSchema
   * @param carbonTableIdentifier
   * @param storePath
   */
  public static void deleteDictionaryFileAndCache(final ColumnSchema columnSchema,
      CarbonTableIdentifier carbonTableIdentifier, String storePath) {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier);
    String metadataDirectoryPath = carbonTablePath.getMetadataDirectoryPath();
    CarbonFile metadataDir = FileFactory
        .getCarbonFile(metadataDirectoryPath, FileFactory.getFileType(metadataDirectoryPath));
    // sort index file is created with dictionary size appended to it. So all the files
    // with a given column ID need to be listed
    CarbonFile[] listFiles = metadataDir.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile path) {
        if (path.getName().startsWith(columnSchema.getColumnUniqueId())) {
          return true;
        }
        return false;
      }
    });
    for (CarbonFile file : listFiles) {
      // try catch is inside for loop because even if one deletion fails, other files
      // still need to be deleted
      try {
        FileFactory
            .deleteFile(file.getCanonicalPath(), FileFactory.getFileType(file.getCanonicalPath()));
      } catch (IOException e) {
        LOGGER.error("Failed to delete dictionary or sortIndex file for column " + columnSchema
            .getColumnName() + "with column ID " + columnSchema.getColumnUniqueId());
      }
    }
    // remove dictionary cache
    removeDictionaryColumnFromCache(carbonTableIdentifier, storePath,
        columnSchema.getColumnUniqueId());
  }

  /**
   * This method will remove dictionary cache from driver for both reverse and forward dictionary
   *
   * @param carbonTableIdentifier
   * @param storePath
   * @param columnId
   */
  public static void removeDictionaryColumnFromCache(CarbonTableIdentifier carbonTableIdentifier,
      String storePath, String columnId) {
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache =
        CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY, storePath);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            new ColumnIdentifier(columnId, null, null));
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
    dictCache = CacheProvider.getInstance().createCache(CacheType.FORWARD_DICTIONARY, storePath);
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
  }

}
