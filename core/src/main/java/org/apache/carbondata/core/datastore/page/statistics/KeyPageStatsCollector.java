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
import org.apache.carbondata.core.util.ByteUtil;

public class KeyPageStatsCollector extends ColumnPageStatsCollector {

  private DataType dataType;

  private byte[] min, max;

  public static KeyPageStatsCollector newInstance(DataType dataType) {
    return new KeyPageStatsCollector(dataType);
  }

  private KeyPageStatsCollector(DataType dataType) {
    this.dataType = dataType;
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
  public void update(float value) {

  }

  @Override
  public void update(BigDecimal value) {

  }

  @Override
  public void update(byte[] value) {
    if (null == min) {
      min = value;
    }
    if (null == max) {
      max = value;
    }
    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(min, value) > 0) {
      min = value;
    }
    if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(max, value) < 0) {
      max = value;
    }
  }

  @Override
  public SimpleStatsResult getPageStats() {
    return new SimpleStatsResult() {

      @Override
      public Object getMin() {
        return min;
      }

      @Override
      public Object getMax() {
        return max;
      }

      @Override
      public int getDecimalCount() {
        return 0;
      }

      @Override
      public DataType getDataType() {
        return dataType;
      }

      @Override
      public boolean writeMinMax() {
        return true;
      }

    };
  }
}
