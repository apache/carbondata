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

package org.carbondata.processing.sortandgroupby.sortkey;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.wrappers.ByteArrayWrapper;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class CarbonSortKeyHashbasedAggregator {
  protected ByteArrayWrapper dimensionsRowWrapper;
  /**
   * keyIndex
   */
  private int keyIndex;
  /**
   * aggType
   */
  private String[] aggType;
  /**
   * aggClassName
   */
  private String[] aggClassName;
  /**
   * factKeyGenerator
   */
  private KeyGenerator factKeyGenerator;
  /**
   * max value for each measure
   */
  private char[] type;
  private int resultSize;
  /**
   * aggergatorMap
   */
  private Map<ByteArrayWrapper, MeasureAggregator[]> aggergatorMap;
  private XXHash32 xxHash32;
  private int counter;
  private int numberOfRows;
  private Object[] mergedMinValue;

  /**
   * constructer.
   *
   * @param aggType
   * @param aggClassName
   * @param factKeyGenerator
   * @param type
   */
  public CarbonSortKeyHashbasedAggregator(String[] aggType, String[] aggClassName,
      KeyGenerator factKeyGenerator, char[] type, int numberOfRows, Object[] mergedMinValue) {
    this.keyIndex = aggType.length;
    this.aggType = aggType;
    this.aggClassName = aggClassName;
    this.factKeyGenerator = factKeyGenerator;
    resultSize = aggType.length + 1;
    this.type = type;
    //        this.xxHash32 = null;
    boolean useXXHASH =
        Boolean.valueOf(CarbonProperties.getInstance().getProperty("carbon.enableXXHash", "false"));
    if (useXXHASH) {
      xxHash32 = XXHashFactory.fastestInstance().hash32();
    }
    this.numberOfRows = numberOfRows;
    aggergatorMap = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(numberOfRows + 1, 1.0f);
    dimensionsRowWrapper = new ByteArrayWrapper(xxHash32);
    this.mergedMinValue = mergedMinValue;
  }

  public void addData(Object[] row) {
    dimensionsRowWrapper.setMaskedKey((byte[]) row[this.keyIndex]);
    MeasureAggregator[] data = aggergatorMap.get(dimensionsRowWrapper);
    if (null == data) {
      data = getAggregators();
      updateMeasureValue(row, data);
      aggergatorMap.put(dimensionsRowWrapper, data);
      dimensionsRowWrapper = new ByteArrayWrapper(xxHash32);
      counter++;
    } else {
      updateMeasureValue(row, data);
    }
  }

  public int getSize() {
    return counter;
  }

  public void reset() {
    aggergatorMap = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(numberOfRows + 1, 1.0f);
    counter++;
  }

  public Object[][] getResult() {
    Object[][] rows = new Object[aggergatorMap.size()][];
    Object[] row = null;
    int index = 0;
    for (Entry<ByteArrayWrapper, MeasureAggregator[]> entry : aggergatorMap.entrySet()) {
      row = new Object[resultSize];
      row[this.keyIndex] = entry.getKey().getMaskedKey();
      MeasureAggregator[] value = entry.getValue();
      for (int i = 0; i < value.length; i++) {
        if (type[i] != 'c') {
          if (!value[i].isFirstTime()) {
            switch (type[i]) {
              case 'l':
                row[i] = value[i].getLongValue();
                break;
              case 'b':
                row[i] = value[i].getBigDecimalValue();
                break;
              default:
                row[i] = value[i].getDoubleValue();
            }
          } else {
            row[i] = null;
          }
        } else {
          row[i] = value[i].getByteArray();
        }
      }
      rows[index++] = row;
    }
    return rows;
  }

  private MeasureAggregator[] getAggregators() {
    MeasureAggregator[] aggregators = AggUtil
        .getAggregators(Arrays.asList(this.aggType), Arrays.asList(this.aggClassName), false,
            factKeyGenerator, null, mergedMinValue, this.type);
    return aggregators;
  }

  /**
   * This method will be used to update the measure value based on aggregator
   * type
   *
   * @param row row
   */
  private void updateMeasureValue(Object[] row, MeasureAggregator[] aggregators) {
    for (int i = 0; i < aggregators.length; i++) {
      if (null != row[i]) {
        if (type[i] != 'c') {
          double value = (Double) row[i];
          aggregators[i].agg(value);
        } else {
          if (row[i] instanceof byte[]) {
            aggregators[i].agg(row[i]);
          } else {
            double value = (Double) row[i];
            aggregators[i].agg(value);
          }
        }
      }
    }

  }

}
