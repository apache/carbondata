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

public interface ColumnPageStatsCollector {

  void updateNull(int rowId);

  void updateNull(int rowId, Object nullValue);

  void update(byte value);

  void update(short value);

  void update(int value);

  void update(long value);

  void update(double value);

  void update(float value);

  void update(BigDecimal value);

  void update(byte[] value);

  /**
   * return the collected statistics
   */
  SimpleStatsResult getPageStats();
}
