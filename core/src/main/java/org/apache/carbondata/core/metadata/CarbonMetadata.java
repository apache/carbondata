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
package org.apache.carbondata.core.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;

/**
 * Class which persist the information about the tables present the carbon schemas
 */
public final class CarbonMetadata {

  /**
   * meta data instance
   */
  private static final CarbonMetadata CARBONMETADATAINSTANCE = new CarbonMetadata();

  /**
   * holds the list of tableInfo currently present
   */
  private Map<String, CarbonTable> tableInfoMap;

  private CarbonMetadata() {
    // creating a concurrent map as it will be updated by multiple thread
    tableInfoMap = new ConcurrentHashMap<String, CarbonTable>();
  }

  public static CarbonMetadata getInstance() {
    return CARBONMETADATAINSTANCE;
  }

  /**
   * removed the table information
   *
   * @param tableUniquName
   */
  public void removeTable(String tableUniquName) {
    tableInfoMap.remove(convertToLowerCase(tableUniquName));
  }

  /**
   * Below method will be used to set the carbon table
   * This method will be used in executor side as driver will always have
   * updated table so from driver during query execution and data loading
   * we just need to add the table
   *
   * @param carbonTable
   */
  public void addCarbonTable(CarbonTable carbonTable) {
    tableInfoMap.put(convertToLowerCase(carbonTable.getTableUniqueName()), carbonTable);
  }

  /**
   * method load the table
   *
   * @param tableInfo
   */
  public void loadTableMetadata(TableInfo tableInfo) {
    CarbonTable carbonTable = tableInfoMap.get(convertToLowerCase(tableInfo.getTableUniqueName()));
    if (null == carbonTable || carbonTable.getTableLastUpdatedTime() < tableInfo
        .getLastUpdatedTime()) {
      carbonTable = new CarbonTable();
      carbonTable.loadCarbonTable(tableInfo);
      tableInfoMap.put(convertToLowerCase(tableInfo.getTableUniqueName()), carbonTable);
    }
  }

  /**
   * Below method to get the loaded carbon table
   *
   * @param tableUniqueName
   * @return
   */
  public CarbonTable getCarbonTable(String tableUniqueName) {
    return tableInfoMap.get(convertToLowerCase(tableUniqueName));
  }

  /**
   * @return the number of tables present in the schema
   */
  public int getNumberOfTables() {
    return tableInfoMap.size();
  }

  /**
   * returns the given string in lowercase
   * @param table
   * @return
   */
  public String convertToLowerCase(String table) {
    return table.toLowerCase();
  }

  /**
   * method will return dimension instance based on the column identifier
   * and table instance passed to it.
   *
   * @param carbonTable
   * @param columnIdentifier
   * @return CarbonDimension instance
   */
  public CarbonDimension getCarbonDimensionBasedOnColIdentifier(CarbonTable carbonTable,
      String columnIdentifier) {
    List<CarbonDimension> listOfCarbonDims =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    for (CarbonDimension dimension : listOfCarbonDims) {
      if (dimension.getColumnId().equals(columnIdentifier)) {
        return dimension;
      }
      if (dimension.getNumberOfChild() > 0) {
        CarbonDimension childDim =
            getCarbonChildDimsBasedOnColIdentifier(columnIdentifier, dimension);
        if (null != childDim) {
          return childDim;
        }
      }
    }
    return null;
  }

  /**
   * Below method will be used to get the dimension based on column identifier
   * for complex dimension children
   *
   * @param columnIdentifier column identifier
   * @param dimension        parent dimension
   * @return children dimension
   */
  private CarbonDimension getCarbonChildDimsBasedOnColIdentifier(String columnIdentifier,
      CarbonDimension dimension) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      if (dimension.getListOfChildDimensions().get(i).getColumnId().equals(columnIdentifier)) {
        return dimension.getListOfChildDimensions().get(i);
      } else if (dimension.getListOfChildDimensions().get(i).getNumberOfChild() > 0) {
        CarbonDimension childDim = getCarbonChildDimsBasedOnColIdentifier(columnIdentifier,
            dimension.getListOfChildDimensions().get(i));
        if (null != childDim) {
          return childDim;
        }
      }
    }
    return null;
  }
}
