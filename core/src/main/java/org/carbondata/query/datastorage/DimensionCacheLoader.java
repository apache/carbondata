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

package org.carbondata.query.datastorage;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.query.datastorage.streams.DataInputStream;
import org.carbondata.query.util.CacheUtil;
import org.carbondata.query.util.CarbonDataInputStreamFactory;

public final class DimensionCacheLoader {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DimensionCacheLoader.class.getName());

  private DimensionCacheLoader() {

  }

  /**
   * @param hierC
   * @param fileStore
   * @throws IOException
   */
  public static void loadHierarichyFromFileStore(HierarchyStore hierC, String fileStore)
      throws IOException {
    loadBTreeHierarchyFileStore(hierC, fileStore);
  }

  /**
   * @param hierC
   * @param fileStore
   * @throws IOException
   */
  private static void loadBTreeHierarchyFileStore(HierarchyStore hierC, String fileStore)
      throws IOException {
    KeyGenerator keyGen = getKeyGenerator(hierC.getCarbonHierarchy());

    String fileName = hierC.getDimensionName().replaceAll(" ", "_") + '_' + hierC.getTableName();
    //
    CarbonFile file = FileFactory.getCarbonFile(fileStore, FileFactory.getFileType(fileStore));
    if (file.isDirectory()) {
      // Read from the base location.
      String baseLocation = fileStore + File.separator + fileName;
      if (FileFactory.isFileExist(baseLocation, FileFactory.getFileType(baseLocation))) {
        //
        DataInputStream factStream = CarbonDataInputStreamFactory
            .getDataInputStream(baseLocation, keyGen.getKeySizeInBytes(), 0, false, fileStore,
                fileName, FileFactory.getFileType(fileStore));
        factStream.initInput();
        hierC.build(keyGen, factStream);

        factStream.closeInput();
      }
    }
  }

  private static KeyGenerator getKeyGenerator(CarbonDef.Hierarchy carbonHierarchy) {
    CarbonDef.Level[] levels = carbonHierarchy.levels;
    int levelSize = levels.length;
    int[] lens = new int[levelSize];
    int i = 0;
    for (CarbonDef.Level level : levels) {
      lens[i++] = level.levelCardinality;
    }

    return KeyGeneratorFactory.getKeyGenerator(lens);
  }

  /**
   * Loads members from file store.
   *
   * @param levelCache
   * @param fileStore
   * @param factTablename
   */
  public static void loadMemberFromFileStore(MemberStore levelCache, String fileStore,
      String dataType, String factTablename, String tableName) {
    //
    Member[][] members = null;

    int[] globalSurrogateMapping = null;
    int minValue = 0;
    int minValueForLevelFile = 0;
    int maxValueFOrLevelFile = 0;
    List<int[][]> sortOrderAndReverseOrderIndex = null;

    if (tableName.contains(".")) {
      tableName = tableName.split("\\.")[1];
    }
    if (factTablename.equals(tableName)) {
      tableName = factTablename;
    }

    String fileName = tableName + '_' + levelCache.getColumnName();
    CarbonFile file = FileFactory.getCarbonFile(fileStore, FileFactory.getFileType(fileStore));
    if (file.isDirectory()) {
      // Read from the base location.
      String baseLocation =
          fileStore + File.separator + fileName + CarbonCommonConstants.LEVEL_FILE_EXTENSION;
      String baseLocationForGlobalKeys = fileStore + File.separator + fileName + ".globallevel";
      String baseLocationForsortIndex = fileStore + File.separator + fileName;
      try {
        if (FileFactory.isFileExist(baseLocation, FileFactory.getFileType(baseLocation))) {
          members = CacheUtil.getMembersList(baseLocation, (byte) -1, dataType);
          minValueForLevelFile = CacheUtil.getMinValueFromLevelFile(baseLocation);
          maxValueFOrLevelFile = CacheUtil.getMaxValueFromLevelFile(baseLocation);
          minValue = CacheUtil.getMinValue(baseLocationForGlobalKeys);

          globalSurrogateMapping = CacheUtil.getGlobalSurrogateMapping(baseLocationForGlobalKeys);
          if (null != members) {
            sortOrderAndReverseOrderIndex =
                CacheUtil.getLevelSortOrderAndReverseIndex(baseLocationForsortIndex);
          }
        }
      } catch (IOException e) {
        LOGGER.error(e);
      }
    }

    levelCache
        .addAll(members, minValueForLevelFile, maxValueFOrLevelFile, sortOrderAndReverseOrderIndex);
    levelCache.addGlobalKey(globalSurrogateMapping, minValue);
  }

}
