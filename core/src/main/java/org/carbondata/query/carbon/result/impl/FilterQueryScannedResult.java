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
package org.carbondata.query.carbon.result.impl;

import java.math.BigDecimal;

import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.AbstractScannedResult;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedResult extends AbstractScannedResult {

  public FilterQueryScannedResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
    ++currentRow;
    return getDictionaryKeyArray(rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(rowMapping[currentRow]);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrenrRowId() {
    return rowMapping[currentRow];
  }

  /**
   * Return the dimension data based on dimension ordinal
   *
   * @param dimensionOrdinal dimension ordinal
   * @return dimension data
   */
  @Override public byte[] getDimensionKey(int dimensionOrdinal) {
    return getDimensionData(dimensionOrdinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to to check whether measure value
   * is null or for a measure
   *
   * @param ordinal measure ordinal
   * @return is null or not
   */
  @Override public boolean isNullMeasureValue(int ordinal) {
    return isNullMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the measure value for measure
   * of long data type
   *
   * @param ordinal measure ordinal
   * @return long value of measure
   */
  @Override public long getLongMeasureValue(int ordinal) {
    return getLongMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the value of measure of double
   * type
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public double getDoubleMeasureValue(int ordinal) {
    return getDoubleMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the data of big decimal type
   * of a measure
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public BigDecimal getBigDecimalMeasureValue(int ordinal) {
    return getBigDecimalMeasureValue(ordinal, rowMapping[currentRow]);
  }

}
