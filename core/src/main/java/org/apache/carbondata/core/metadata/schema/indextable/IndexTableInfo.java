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

package org.apache.carbondata.core.metadata.schema.indextable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.status.IndexStatus;

import com.google.gson.Gson;

public class IndexTableInfo implements Serializable {

  private static final long serialVersionUID = 1106104914918491724L;

  private String databaseName;
  private String tableName;
  private List<String> indexCols;
  private Map<String, String> indexProperties;

  public IndexTableInfo(String databaseName, String tableName,
      Map<String, String> indexProperties) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexProperties = indexProperties;
    this.indexCols =
        Arrays.asList(indexProperties.get(CarbonCommonConstants.INDEX_COLUMNS).split(","));
  }

  /**
   * returns db name
   *
   * @return
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * returns table name
   *
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * returns all the index columns
   *
   * @return
   */
  public List<String> getIndexCols() {
    return indexCols;
  }

  public void setIndexCols(List<String> indexCols) {
    this.indexCols = indexCols;
  }

  /**
   * compares both the objects
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof IndexTableInfo)) {
      return false;
    }
    IndexTableInfo other = (IndexTableInfo) obj;
    if (indexCols == null) {
      if (other.indexCols != null) {
        return false;
      }
    } else if (!indexCols.equals(other.indexCols)) {
      return false;
    }
    return true;
  }

  /**
   * convert string to index table info object
   *
   * @param gsonData
   * @return
   */
  public static IndexTableInfo[] fromGson(String gsonData) {
    Gson gson = new Gson();
    return gson.fromJson(gsonData, IndexTableInfo[].class);
  }

  /**
   * converts index table info object to string
   *
   * @param gsonData
   * @return
   */
  public static String toGson(IndexTableInfo[] gsonData) {
    Gson gson = new Gson();
    return gson.toJson(gsonData);
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    for (String s : indexCols) {
      hashCode += s.hashCode();
    }
    return hashCode;
  }

  public static String updateIndexColumns(String oldGsonData, String columnToBeUpdated,
      String newColumnName) {
    IndexTableInfo[] indexTableInfos = fromGson(oldGsonData);
    if (null == indexTableInfos) {
      indexTableInfos = new IndexTableInfo[0];
    }
    for (int i = 0; i < indexTableInfos.length; i++) {
      List<String> newColumnList = new ArrayList<>();
      for (String indexColumn : indexTableInfos[i].getIndexCols()) {
        if (indexColumn.equalsIgnoreCase(columnToBeUpdated)) {
          newColumnList.add(newColumnName);
        } else {
          newColumnList.add(indexColumn);
        }
      }
      indexTableInfos[i].setIndexCols(newColumnList);
      Map<String, String> indexProperties = indexTableInfos[i].getIndexProperties();
      indexProperties
          .put(CarbonCommonConstants.INDEX_COLUMNS, String.join(",", newColumnList));
      indexTableInfos[i].setIndexProperties(indexProperties);
    }
    return toGson(indexTableInfos);
  }

  public static String enableIndex(String oldIndexIno, String indexName) {
    IndexTableInfo[] indexTableInfos = fromGson(oldIndexIno);
    for (IndexTableInfo indexTableInfo : indexTableInfos) {
      if (indexTableInfo.tableName.equalsIgnoreCase(indexName)) {
        Map<String, String> oldindexProperties = indexTableInfo.indexProperties;
        oldindexProperties.put(CarbonCommonConstants.INDEX_STATUS, IndexStatus.ENABLED.name());
        indexTableInfo.setIndexProperties(oldindexProperties);
      }
    }
    return toGson(indexTableInfos);
  }

  public Map<String, String> getIndexProperties() {
    return indexProperties;
  }

  public void setIndexProperties(Map<String, String> indexProperties) {
    this.indexProperties = indexProperties;
  }


}
