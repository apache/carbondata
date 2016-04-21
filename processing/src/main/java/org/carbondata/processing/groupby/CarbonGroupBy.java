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

package org.carbondata.processing.groupby;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

public class CarbonGroupBy {
  /**
   * key array index
   */
  private int keyIndex;

  /**
   * column index mapping
   */
  private int[] columnIndexMapping;

  /**
   * aggregate type
   */
  private String[] aggType;

  /**
   * previous row
   */
  private Object[] prvRow;

  /**
   * previous row key
   */
  private byte[] prvKey;

  public CarbonGroupBy(String aggType, String rowMeasures, String actualMeasures, Object[] row) {
    this.aggType = aggType.split(";");
    this.keyIndex = row.length - 1;
    this.columnIndexMapping = new int[this.aggType.length];
    updateColumnIndexMapping(rowMeasures.split(";"), actualMeasures.split(";"));
    addNewRow(row);
  }

  private void addNewRow(Object[] row) {
    int index = 0;
    Object[] newRow = new Object[columnIndexMapping.length + 1];
    for (int i = 0; i < columnIndexMapping.length; i++) {
      if (this.aggType[i].equals(CarbonCommonConstants.COUNT)) {
        if (row[columnIndexMapping[i]] != null) {
          newRow[index++] = 1D;
        } else {
          newRow[index++] = 0D;
        }
      } else if (!this.aggType[i].equals(CarbonCommonConstants.MAX) && !this.aggType[i]
          .equals(CarbonCommonConstants.MIN)) {
        if (null != row[columnIndexMapping[i]]) newRow[index++] = row[columnIndexMapping[i]];
        else {
          newRow[index++] = 0D;
        }
      } else {
        newRow[index++] = row[columnIndexMapping[i]];
      }
    }
    prvKey = (byte[]) row[this.keyIndex];
    newRow[index] = prvKey;
    prvRow = newRow;
  }

  /**
   * This method will be used to update the column index mapping array which
   * will be used for mapping actual column with row column
   *
   * @param rowMeasureName row Measure Name
   * @param actualMeasures actual Measures
   */
  private void updateColumnIndexMapping(String[] rowMeasureName, String[] actualMeasures) {
    int index = 0;
    for (int i = 0; i < actualMeasures.length; i++) {
      for (int j = 0; j < rowMeasureName.length; j++) {
        if (actualMeasures[i].equals(rowMeasureName[j])) {
          this.columnIndexMapping[index++] = j;
          break;
        }
      }
    }
  }

  /**
   * This method will be used to add new row it will check if new row and
   * previous row key is same then it will merger the measure values, else it
   * return the previous row
   *
   * @param row new row
   * @return previous row
   */
  public Object[] add(Object[] row) {
    if (CarbonDataProcessorUtil.compare(prvKey, (byte[]) row[this.keyIndex]) == 0) {
      updateMeasureValue(row);

    } else {
      Object[] lastRow = prvRow;
      addNewRow(row);
      return lastRow;
    }
    return null;
  }

  /**
   * This method will be used to update the measure value based on aggregator
   * type
   *
   * @param row row
   */
  private void updateMeasureValue(Object[] row) {

    for (int i = 0; i < columnIndexMapping.length; i++) {
      aggregateValue(prvRow[i], row[columnIndexMapping[i]], aggType[i], i);
    }
  }

  /**
   * This method will be used to update the measure value based on aggregator
   * type
   *
   * @param object1 previous row measure value
   * @param object2 new row measure value
   * @param aggType
   * @param idx     measure index
   */
  private void aggregateValue(Object object1, Object object2, String aggType, int idx) {
    if (aggType.equals(CarbonCommonConstants.MAX)) {
      if (null == object1 && null != object2) {
        prvRow[idx] = object2;
      } else if (null != object1 && null == object2) {
        prvRow[idx] = object1;
      } else if (null != object1 && null != object2) {
        prvRow[idx] = (Double) object1 > (Double) object2 ? object1 : object2;
      }
    } else if (aggType.equals(CarbonCommonConstants.MIN)) {
      if (null == object1 && null != object2) {
        prvRow[idx] = object2;
      } else if (null != object1 && null == object2) {
        prvRow[idx] = object1;
      } else if (null != object1 && null != object2) {
        prvRow[idx] = (Double) object1 < (Double) object2 ? object1 : object2;
      }
    } else if (aggType.equals(CarbonCommonConstants.COUNT)) {
      if (null != object2) {
        double d = (Double) prvRow[idx];
        d++;
        prvRow[idx] = d;
      }
    } else {
      if (null == object1 && null != object2) {
        prvRow[idx] = object2;
      } else if (null != object1 && null == object2) {
        prvRow[idx] = object1;
      } else if (null != object1 && null != object2) {
        prvRow[idx] = (Double) object1 + (Double) object2;
      }
    }
  }

  /**
   * This method will be used to get the last row
   *
   * @return last row
   */
  public Object[] getLastRow() {
    return prvRow;
  }
}
