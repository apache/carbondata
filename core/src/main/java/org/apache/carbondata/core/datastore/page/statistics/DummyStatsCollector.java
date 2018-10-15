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

import static org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE_ARRAY;

/**
 * Column Page dummy stats collector. This will be used for which stats generation
 * is not required for example complex type column
 */
public class DummyStatsCollector implements ColumnPageStatsCollector {

  /**
   * dummy stats used to sync with encoder
   */
  protected static final SimpleStatsResult DUMMY_STATS = new SimpleStatsResult() {
    @Override public Object getMin() {
      return new byte[0];
    }

    @Override public Object getMax() {
      return new byte[0];
    }

    @Override public int getDecimalCount() {
      return 0;
    }

    @Override public DataType getDataType() {
      return BYTE_ARRAY;
    }

    @Override public boolean writeMinMax() {
      return true;
    }

  };

  @Override public void updateNull(int rowId) {

  }

  @Override public void update(byte value) {

  }

  @Override public void update(short value) {

  }

  @Override public void update(int value) {

  }

  @Override public void update(long value) {

  }

  @Override public void update(double value) {

  }

  @Override public void update(float value) {

  }

  @Override public void update(BigDecimal value) {

  }

  @Override public void update(byte[] value) {

  }

  @Override public void updateNull(int rowId, Object nullValue) {

  }

  @Override public SimpleStatsResult getPageStats() {
    return DUMMY_STATS;
  }
}
