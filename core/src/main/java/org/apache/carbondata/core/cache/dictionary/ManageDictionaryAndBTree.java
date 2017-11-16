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
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class is aimed at managing dictionary files for any new addition and deletion
 * and calling of clear cache for BTree and dictionary instances from LRU cache
 */
public class ManageDictionaryAndBTree {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ManageDictionaryAndBTree.class.getName());

  /**
   * This method will delete the dictionary files for the given column IDs and
   * clear the dictionary cache
   *
   * @param columnSchema
   * @param carbonTableIdentifier
   */
  public static void deleteDictionaryFileAndCache(final ColumnSchema columnSchema,
      AbsoluteTableIdentifier carbonTableIdentifier) {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonTableIdentifier);
    String metadataDirectoryPath = carbonTablePath.getMetadataDirectoryPath();
    CarbonFile metadataDir = FileFactory
        .getCarbonFile(metadataDirectoryPath, FileFactory.getFileType(metadataDirectoryPath));
    if (metadataDir.exists()) {
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
          FileFactory.deleteFile(file.getCanonicalPath(),
              FileFactory.getFileType(file.getCanonicalPath()));
        } catch (IOException e) {
          LOGGER.error("Failed to delete dictionary or sortIndex file for column "
              + columnSchema.getColumnName() + "with column ID "
              + columnSchema.getColumnUniqueId());
        }
      }
    }
    // remove dictionary cache
    removeDictionaryColumnFromCache(carbonTableIdentifier, columnSchema.getColumnUniqueId());
  }

  /**
   * This mwthod will invalidate both BTree and dictionary instances from LRU cache
   *
   * @param carbonTable
   */
  public static void clearBTreeAndDictionaryLRUCache(CarbonTable carbonTable) {
    // clear Btree cache from LRU cache
    LoadMetadataDetails[] loadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetaDataFilepath());
    if (null != loadMetadataDetails) {
      String[] segments = new String[loadMetadataDetails.length];
      int i = 0;
      for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
        segments[i++] = loadMetadataDetail.getLoadName();
      }
      invalidateBTreeCache(carbonTable.getAbsoluteTableIdentifier(), segments);
    }
    // clear dictionary cache from LRU cache
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    for (CarbonDimension dimension : dimensions) {
      removeDictionaryColumnFromCache(carbonTable.getAbsoluteTableIdentifier(),
          dimension.getColumnId());
    }
  }

  /**
   * This method will remove dictionary cache from driver for both reverse and forward dictionary
   *
   * @param carbonTableIdentifier
   * @param columnId
   */
  public static void removeDictionaryColumnFromCache(AbsoluteTableIdentifier carbonTableIdentifier,
      String columnId) {
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache =
        CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            new ColumnIdentifier(columnId, null, null));
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
    dictCache = CacheProvider.getInstance().createCache(CacheType.FORWARD_DICTIONARY);
    dictCache.invalidate(dictionaryColumnUniqueIdentifier);
  }

  /**
   * This method will remove the BTree instances from LRU cache
   *
   * @param absoluteTableIdentifier
   * @param segments
   */
  public static void invalidateBTreeCache(AbsoluteTableIdentifier absoluteTableIdentifier,
      String[] segments) {
    Cache<Object, Object> driverBTreeCache =
        CacheProvider.getInstance().createCache(CacheType.DRIVER_BTREE);
    for (String segmentNo : segments) {
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
          new TableSegmentUniqueIdentifier(absoluteTableIdentifier, segmentNo);
      driverBTreeCache.invalidate(tableSegmentUniqueIdentifier);
    }
  }

}
