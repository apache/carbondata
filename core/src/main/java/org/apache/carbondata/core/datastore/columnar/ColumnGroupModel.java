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
package org.apache.carbondata.core.datastore.columnar;

public class ColumnGroupModel {

  /**
   * number of columns in columnar block
   */
  private int[] columnSplit;

  /**
   * total number of columns
   */
  private int noOfColumnsStore;

  /**
   * column groups
   * e.g
   * {{0,1,2},3,4,{5,6}}
   */
  private int[][] columnGroups;

  /**
   * return columnSplit
   *
   * @return
   */
  public int[] getColumnSplit() {
    return columnSplit;
  }

  /**
   * set columnSplit
   *
   * @param split
   */
  public void setColumnSplit(int[] split) {
    this.columnSplit = split;
  }

  /**
   * @return no of columnar block
   */
  public int getNoOfColumnStore() {
    return this.noOfColumnsStore;
  }

  /**
   * set no of columnar block
   *
   * @param noOfColumnsStore
   */
  public void setNoOfColumnStore(int noOfColumnsStore) {
    this.noOfColumnsStore = noOfColumnsStore;
  }

  /**
   * set column groups
   *
   * @param columnGroups
   */
  public void setColumnGroup(int[][] columnGroups) {
    this.columnGroups = columnGroups;
  }

  /**
   * @return columngroups
   */
  public int[][] getColumnGroup() {
    return this.columnGroups;
  }

}
