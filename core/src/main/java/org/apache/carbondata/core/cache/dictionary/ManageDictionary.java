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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
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
   * @param dictionaryColumns
   * @param carbonTable
   */
  public static void deleteDictionaryFileAndCache(List<CarbonColumn> dictionaryColumns,
      CarbonTable carbonTable) {
    if (!dictionaryColumns.isEmpty()) {
      CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
      CarbonTablePath carbonTablePath =
          CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath(), carbonTableIdentifier);
      String metadataDirectoryPath = carbonTablePath.getMetadataDirectoryPath();
      CarbonFile metadataDir = FileFactory
          .getCarbonFile(metadataDirectoryPath, FileFactory.getFileType(metadataDirectoryPath));
      for (final CarbonColumn column : dictionaryColumns) {
        // sort index file is created with dictionary size appended to it. So all the files
        // with a given column ID need to be listed
        CarbonFile[] listFiles = metadataDir.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile path) {
            if (path.getName().startsWith(column.getColumnId())) {
              return true;
            }
            return false;
          }
        });
        for (CarbonFile file : listFiles) {
          // try catch is inside for loop because even if one deletion fails, other files
          // still need to be deleted
          try {
            FileFactory.deleteFile(file.getCanonicalPath(),
                FileFactory.getFileType(file.getCanonicalPath()));
          } catch (IOException e) {
            LOGGER.error(
                "Failed to delete dictionary or sortIndex file for column " + column.getColName()
                    + "with column ID " + column.getColumnId());
          }
        }
        // remove dictionary cache
        removeDictionaryColumnFromCache(carbonTable, column.getColumnId());
      }
    }
  }

  /**
   * This method will remove dictionary cache from driver for both reverse and forward dictionary
   *
   * @param carbonTable
   * @param columnId
   */
  public static void removeDictionaryColumnFromCache(CarbonTable carbonTable, String columnId) {
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache = CacheProvider.getInstance()
        .createCache(CacheType.REVERSE_DICTIONARY, carbonTable.getStorePath());
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(carbonTable.getCarbonTableIdentifier(),
            new ColumnIdentifier(columnId, null, null));
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
    dictCache = CacheProvider.getInstance()
        .createCache(CacheType.FORWARD_DICTIONARY, carbonTable.getStorePath());
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
  }

}
