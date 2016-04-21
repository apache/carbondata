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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.CarbonSchemaReader;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class DimensionHierarichyStore {
  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DimensionHierarichyStore.class.getName());
  /**
   * Hierarchy name --> HierarchyCache
   * Maintains all the hierarchies
   */
  private Map<String, HierarchyStore> hiers =
      new HashMap<String, HierarchyStore>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  /**
   * Can hold members, each level
   * column name and the cache
   */
  private Map<String, MemberStore> membersCache;
  /**
   *
   */
  private String cubeName;

  public DimensionHierarichyStore(CarbonDef.CubeDimension dimension,
      Map<String, MemberStore> membersCache, String cubeName, String factTableName,
      CarbonDef.Schema schema) {
    this.membersCache = membersCache;
    this.cubeName = cubeName;

    CarbonDef.Hierarchy[] extractHierarchies =
        CarbonSchemaReader.extractHierarchies(schema, dimension);
    if (null != extractHierarchies) {
      for (CarbonDef.Hierarchy hierarchy : extractHierarchies) {
        String hName = hierarchy.name == null ? dimension.name : hierarchy.name;

        hiers.put(hName, new HierarchyStore(hierarchy, factTableName, dimension.name));

        for (CarbonDef.Level level : hierarchy.levels) {
          String tableName = hierarchy.relation == null ?
              factTableName :
              ((CarbonDef.Table) hierarchy.relation).name;
          MemberStore memberCache = new MemberStore(level, tableName);
          membersCache.put(memberCache.getTableForMember() + '_' + dimension.name + '_' + hName,
              memberCache);
        }
      }
    }
  }

  public String getCubeName() {
    return cubeName;
  }

  /**
   * This method will unload the level file from memory
   */
  public void unloadLevelFile(String tableName, String dimName, String heirName,
      String levelActualName) {
    MemberStore memberStore =
        membersCache.get(tableName + '_' + levelActualName + '_' + dimName + '_' + heirName);
    memberStore.clear();
  }

  /**
   * Access the dimension members through levelName
   */
  public MemberStore getMemberCache(String levelName) {
    return membersCache.get(levelName);
  }

  /**
   * Process all hierarchies and members of each level to load cache.
   *
   * @param fileStore
   * @return false if any problem during cache load
   */
  public boolean processCacheFromFileStore(final String fileStore,
      ExecutorService executorService) {
    try {
      // Process hierarchies cache
      for (final HierarchyStore hCache : hiers.values()) {
        CarbonDef.Level[] levels = hCache.getCarbonHierarchy().levels;
        final List<String> dimNames =
            new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        int depth = 0;
        final String tableName = hCache.getCarbonHierarchy().relation == null ?
            hCache.getFactTableName() :
            ((CarbonDef.Table) hCache.getCarbonHierarchy().relation).name;
        for (int i = 0; i < levels.length; i++) {
          final CarbonDef.Level tempLevel = levels[i];
          depth++;
          executorService.submit(new Callable<Void>() {

            @Override public Void call() throws Exception {
              loadDimensionLevels(fileStore, hCache, dimNames, tempLevel, hCache.getHierName(),
                  tableName, hCache.getDimensionName());
              return null;
            }

          });

        }

        if (depth > 1) {
          DimensionCacheLoader.loadHierarichyFromFileStore(hCache, fileStore);
        }
      }
    } catch (IOException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
      return false;
    }

    return true;
  }

  private void loadDimensionLevels(String fileStore, HierarchyStore hCache, List<String> dimNames,
      CarbonDef.Level level3, String hierarchyName, String tableName, String dimensionName) {

    // Process level cache
    if (hierarchyName.contains(".")) {
      hierarchyName =
          hierarchyName.substring(hierarchyName.indexOf(".") + 1, hierarchyName.length());
    }
    String memberKey = tableName + '_' + level3.column + '_' + dimensionName + '_' + hierarchyName;
    MemberStore membercache = membersCache.get(memberKey);
    if (null == membercache.getAllMembers()) {
      DimensionCacheLoader
          .loadMemberFromFileStore(membercache, fileStore, level3.type, hCache.getFactTableName(),
              tableName);
      dimNames.add(memberKey);
    }
  }

}
