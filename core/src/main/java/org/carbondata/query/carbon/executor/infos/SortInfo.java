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
package org.carbondata.query.carbon.executor.infos;

import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;

/**
 * Below class holds the order by information about the query
 */
public class SortInfo {

  /**
   * sorting order of a dimension
   */
  private byte[] dimensionSortOrder;

  /**
   * byte range of each dimension present in the order by
   */
  private int[][] maskedByteRangeForSorting;

  /**
   * dimension indexes which is used in order bye
   */
  private byte[] sortDimensionIndex;

  /**
   * mask key of each dimension
   * this will be used to sort the dimension
   */
  private byte[][] dimensionMaskKeyForSorting;

  /**
   * sortDimension
   */
  private List<CarbonDimension> sortDimension;

  /**
   * @return the dimensionSortOrder
   */
  public byte[] getDimensionSortOrder() {
    return dimensionSortOrder;
  }

  /**
   * @param dimensionSortOrder the dimensionSortOrder to set
   */
  public void setDimensionSortOrder(byte[] dimensionSortOrder) {
    this.dimensionSortOrder = dimensionSortOrder;
  }

  /**
   * @return the maskedByteRangeForSorting
   */
  public int[][] getMaskedByteRangeForSorting() {
    return maskedByteRangeForSorting;
  }

  /**
   * @param maskedByteRangeForSorting the maskedByteRangeForSorting to set
   */
  public void setMaskedByteRangeForSorting(int[][] maskedByteRangeForSorting) {
    this.maskedByteRangeForSorting = maskedByteRangeForSorting;
  }

  /**
   * @return the sortDimensionIndex
   */
  public byte[] getSortDimensionIndex() {
    return sortDimensionIndex;
  }

  /**
   * @param sortDimensionIndex the sortDimensionIndex to set
   */
  public void setSortDimensionIndex(byte[] sortDimensionIndex) {
    this.sortDimensionIndex = sortDimensionIndex;
  }

  /**
   * @return the dimensionMaskKeyForSorting
   */
  public byte[][] getDimensionMaskKeyForSorting() {
    return dimensionMaskKeyForSorting;
  }

  /**
   * @param dimensionMaskKeyForSorting the dimensionMaskKeyForSorting to set
   */
  public void setDimensionMaskKeyForSorting(byte[][] dimensionMaskKeyForSorting) {
    this.dimensionMaskKeyForSorting = dimensionMaskKeyForSorting;
  }

  /**
   * @return the sortDimension
   */
  public List<CarbonDimension> getSortDimension() {
    return sortDimension;
  }

  /**
   * @param sortDimension the sortDimension to set
   */
  public void setSortDimension(List<CarbonDimension> sortDimension) {
    this.sortDimension = sortDimension;
  }
}
