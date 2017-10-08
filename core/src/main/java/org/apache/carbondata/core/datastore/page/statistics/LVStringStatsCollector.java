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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;

public class LVStringStatsCollector implements ColumnPageStatsCollector {

  private byte[] min, max;

  public static LVStringStatsCollector newInstance() {
    return new LVStringStatsCollector();
  }

  private LVStringStatsCollector() {

  }

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
  public void update(BigDecimal value) {

  }

  @Override
  public void update(byte[] value) {
    // input value is LV encoded
    assert (value.length >= 2);
    if (value.length == 2) {
      assert (value[0] == 0 && value[1] == 0);
      if (min == null && max == null) {
        min = new byte[0];
        max = new byte[0];
      }
      return;
    }
    int length = (value[0] << 8) + (value[1] & 0xff);
    assert (length > 0);
    byte[] v = new byte[value.length - 2];
    System.arraycopy(value, 2, v, 0, v.length);
    if (min == null && max == null) {
      min = v;
      max = v;
    } else {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(min, v) > 0) {
        min = v;
      }
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(max, v) < 0) {
        max = v;
      }
    }
  }

  @Override
  public SimpleStatsResult getPageStats() {
    return new SimpleStatsResult() {

      @Override public Object getMin() {
        return min;
      }

      @Override public Object getMax() {
        return max;
      }

      @Override public int getDecimalCount() {
        return 0;
      }

      @Override public DataType getDataType() {
        return DataTypes.STRING;
      }

      @Override public int getScale() {
        return 0;
      }

      @Override public int getPrecision() {
        return 0;
      }
    };
  }
}
