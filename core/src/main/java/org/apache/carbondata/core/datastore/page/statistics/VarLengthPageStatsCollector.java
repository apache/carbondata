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

import java.util.BitSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;

public class VarLengthPageStatsCollector implements ColumnPageStatsCollector {

  private byte[] min, max;

  public static VarLengthPageStatsCollector newInstance() {
    return new VarLengthPageStatsCollector();
  }

  private VarLengthPageStatsCollector() {
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
  public void update(byte[] value) {
    if (min == null && max == null) {
      min = value;
      max = value;
    } else {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(min, value) > 0) {
        min = value;
      }
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(max, value) < 0) {
        max = value;
      }
    }
  }

  @Override
  public Object getPageStats() {
    // for binary type, we do not collect its stats
    return new SimpleStatsResult() {

      @Override public byte[] getMinAsBytes() {
        return min;
      }

      @Override public byte[] getMaxAsBytes() {
        return max;
      }

      @Override public Object getMin() {
        return min;
      }

      @Override public Object getMax() {
        return max;
      }

      @Override public BitSet getNullBits() {
        return null;
      }

      @Override public int getDecimal() {
        return 0;
      }

      @Override public DataType getDataType() {
        return null;
      }
    };
  }
}
