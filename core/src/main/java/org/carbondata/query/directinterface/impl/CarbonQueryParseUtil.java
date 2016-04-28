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

package org.carbondata.query.directinterface.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.CarbonLRUCache;
import org.carbondata.core.carbon.CarbonDef.AggMeasure;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.datastorage.cache.LevelInfo;

/**
 * It is util class to parse the carbon query object
 */
public final class CarbonQueryParseUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonQueryParseUtil.class.getName());

  private CarbonQueryParseUtil() {

  }

  /**
   * @param carbonTable
   * @param dims
   * @param measures
   */
  public static String getSuitableTable(CarbonTable carbonTable, List<CarbonDimension> dims,
      List<CarbonMeasure> measures) throws IOException {
    List<String> aggtablesMsrs = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    List<String> aggtables = buildAggTablesList(carbonTable, dims);
    if (measures.size() == 0 && aggtables.size() > 0) {
      return aggtables.get(0);
    }

    //get matching aggregate table matching measure and aggregate function
    for (String tableName : aggtables) {
      List<CarbonMeasure> aggMsrs = carbonTable.getMeasureByTableName(tableName);
      boolean present = false;
      for (CarbonMeasure msr : measures) {
        boolean found = false;
        for (CarbonMeasure aggMsr : aggMsrs) {
          if (msr.getColName().equals(aggMsr.getColName()) && msr.getAggregateFunction()
              .equals(aggMsr.getAggregateFunction())) {
            found = true;
            break;
          }
        }
        if (found) {
          present = true;
        } else {
          present = false;
          break;
        }
      }
      if (present) {
        aggtablesMsrs.add(tableName);
      }
    }
    if (aggtablesMsrs.size() == 0) {
      return carbonTable.getFactTableName();
    }
    return getTabName(carbonTable, aggtablesMsrs);
  }

  /**
   * Find the dimension from metadata by using unique name. As of now we are
   * taking level name as unique name. But user needs to give one unique name
   * for each level,that level he needs to mention in query.
   *
   * @param dimensions
   * @param carbonDim
   * @return
   */
  public static CarbonDimension findDimension(List<CarbonDimension> dimensions, String carbonDim) {
    CarbonDimension findDim = null;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.getColName().equalsIgnoreCase(carbonDim)) {
        findDim = dimension;
        break;
      }
    }
    return findDim;
  }

  public static boolean isDimensionMeasureInAggTable(AggMeasure[] aggMeasures, CarbonDimension dim,
      String agg) {
    for (AggMeasure aggMsrObj : aggMeasures) {
      if (dim.getColumnId().equals(aggMsrObj.name) && agg.equals(aggMsrObj.aggregator)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param carbonTable
   * @param aggtablesMsrs
   * @return
   */
  private static String getTabName(CarbonTable carbonTable, List<String> aggtablesMsrs) {
    String selectedTabName = carbonTable.getFactTableName();
    long selectedTabCount = Long.MAX_VALUE;
    for (String tableName : aggtablesMsrs) {
      long count = carbonTable.getDimensionByTableName(tableName).size();
      if (count < selectedTabCount) {
        selectedTabCount = count;
        selectedTabName = tableName;
      }
    }
    return selectedTabName;
  }

  /**
   * @param carbonTable
   * @param dims
   */
  private static List<String> buildAggTablesList(CarbonTable carbonTable,
      List<CarbonDimension> dims) {
    List<String> aggTables = new ArrayList<String>();
    List<String> tablesList = carbonTable.getAggregateTablesName();

    for (String tableName : tablesList) {
      List<CarbonDimension> aggDims = carbonTable.getDimensionByTableName(tableName);
      boolean present = true;
      for (CarbonDimension dim : dims) {
        boolean found = false;
        for (CarbonDimension aggDim : aggDims) {
          if (dim.getColumnId().equals(aggDim.getColumnId())) {
            found = true;
            break;
          }
        }
        if (!found) {
          present = false;
        }
      }
      if (present) {
        aggTables.add(tableName);
      }
    }
    return aggTables;
  }

  /**
   * This method will remove all columns form cache which have been dropped
   * from the cube
   *
   * @param listOfLoadFolders
   * @param columns
   * @param schemaName
   * @param cubeName
   * @param partitionCount
   */
  public static void removeDroppedColumnsFromLevelLRUCache(List<String> listOfLoadFolders,
      List<String> columns, String schemaName, String cubeName, int partitionCount) {
    String schemaNameWithPartition = null;
    String cubeNameWithPartition = null;
    String levelCacheKey = null;
    String columnActualName = null;
    String cubeUniqueName = null;
    CarbonLRUCache levelCacheInstance = null;
    for (String columnName : columns) {
      for (String loadName : listOfLoadFolders) {
        for (int i = 0; i < partitionCount; i++) {
          schemaNameWithPartition = schemaName + '_' + i;
          cubeNameWithPartition = cubeName + '_' + i;
          cubeUniqueName = schemaNameWithPartition + '_' + cubeNameWithPartition;
          Cube cube = CarbonMetadata.getInstance().getCube(cubeUniqueName);
          if (null != cube) {
            Dimension dimension = cube.getDimension(columnName);
            columnActualName = null != dimension ? dimension.getColName() : columnName;
            levelCacheKey = cubeUniqueName + '_' + loadName + '_' + columnActualName;
            LevelInfo levelInfo = null;
            if (null != levelInfo) {
              if (levelInfo.isLoaded()) {
                InMemoryTableStore.getInstance().unloadLevelFile(cubeUniqueName, levelInfo);
              }
              levelCacheInstance.remove(levelCacheKey);
            }
          }
        }
      }
    }
  }

}
