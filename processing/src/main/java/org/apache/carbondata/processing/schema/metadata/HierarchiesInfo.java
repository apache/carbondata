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

package org.apache.carbondata.processing.schema.metadata;

import java.util.Map;

public class HierarchiesInfo {

  /**
   * hierarichieName
   */
  private String hierarichieName;

  /**
   * columnIndex
   */
  private int[] columnIndex;

  /**
   * columnNames
   */
  private String[] columnNames;

  /**
   * columnPropMap
   */
  private Map<String, String[]> columnPropMap;

  /**
   * loadToHierarichiTable
   */
  private boolean loadToHierarichiTable;

  /**
   * query
   */
  private String query;

  /**
   * Is Time Dimension
   */
  private boolean isTimeDimension;

  /**
   * levelTypeColumnMap
   */
  private Map<String, String> levelTypeColumnMap;

  public boolean isLoadToHierarichiTable() {
    return loadToHierarichiTable;
  }

  public void setLoadToHierarichiTable(boolean loadToHierarichiTable) {
    this.loadToHierarichiTable = loadToHierarichiTable;
  }

  public String getHierarichieName() {
    return hierarichieName;
  }

  public void setHierarichieName(String hierarichieName) {
    this.hierarichieName = hierarichieName;
  }

  public int[] getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int[] columnIndex) {
    this.columnIndex = columnIndex;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
    this.columnNames = columnNames;
  }

  public Map<String, String[]> getColumnPropMap() {
    return columnPropMap;
  }

  public void setColumnPropMap(Map<String, String[]> columnPropMap) {
    this.columnPropMap = columnPropMap;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public boolean isTimeDimension() {
    return isTimeDimension;
  }

  public void setTimeDimension(boolean isTimeDimension) {
    this.isTimeDimension = isTimeDimension;
  }

  public Map<String, String> getLevelTypeColumnMap() {
    return levelTypeColumnMap;
  }

  public void setLevelTypeColumnMap(Map<String, String> levelTypeColumnMap) {
    this.levelTypeColumnMap = levelTypeColumnMap;
  }

}
