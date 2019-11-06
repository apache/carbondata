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

package org.apache.carbondata.core.datastore.page.statistics;

import java.math.BigDecimal;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;

public abstract class LVStringStatsCollector extends ColumnPageStatsCollector {

  /**
   * allowed character limit for to be considered for storing min max
   */
  protected static final int allowedMinMaxByteLimit = Integer.parseInt(
      CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT));
  /**
   * variables for storing min max value
   */
  private byte[] min, max;
  /**
   * This flag will be used to decide whether to write min/max in the metadata or not. This will be
   * helpful for reducing the store size in scenarios where the numbers of characters in
   * string/varchar type columns are more
   */
  private boolean ignoreWritingMinMax;

  @Override
  public void updateNull(int rowId) {

  }

  @Override
  public void update(byte value) {

  }

  @Override
  public void update(short value) {

  }

  @Override
  public void update(int value) {

  }

  @Override
  public void update(long value) {

  }

  @Override
  public void update(double value) {

  }

  @Override
  public void update(float value) {

  }

  @Override
  public void update(BigDecimal value) {

  }

  protected abstract byte[] getActualValue(byte[] value);

  @Override
  public void update(byte[] value) {
    // return if min/max need not be written
    if (isIgnoreMinMaxFlagSet(value)) {
      return;
    }
    // input value is LV encoded
    byte[] newValue = getActualValue(value);

    // add value to page bloom
    if (null != bloomFilter) {
      addValueToBloom(newValue);
    }

    // update min max
    if (null == min) {
      min = newValue;
    }
    if (null == max) {
      max = newValue;
    }
    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(min, newValue) > 0) {
      min = newValue;
    }
    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(max, newValue) < 0) {
      max = newValue;
    }
  }

  private boolean isIgnoreMinMaxFlagSet(byte[] value) {
    if (!ignoreWritingMinMax) {
      if (null != value && value.length > allowedMinMaxByteLimit) {
        ignoreWritingMinMax = true;
      }
    }
    return ignoreWritingMinMax;
  }

  @Override
  public SimpleStatsResult getPageStats() {
    return new SimpleStatsResult() {

      @Override
      public Object getMin() {
        if (null == min || ignoreWritingMinMax) {
          min = new byte[0];
        }
        return min;
      }

      @Override
      public Object getMax() {
        if (null == max || ignoreWritingMinMax) {
          max = new byte[0];
        }
        return max;
      }

      @Override
      public int getDecimalCount() {
        return 0;
      }

      @Override
      public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Override
      public boolean writeMinMax() {
        return !ignoreWritingMinMax;
      }

    };
  }
}
