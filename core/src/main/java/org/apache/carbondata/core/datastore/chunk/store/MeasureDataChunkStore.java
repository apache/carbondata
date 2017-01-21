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

package org.apache.carbondata.core.datastore.chunk.store;

import java.math.BigDecimal;

/**
 * Responsibility is store the measure data in memory,
 * memory can be on heap or offheap based on the user configuration
 */
public interface MeasureDataChunkStore<T> {

  /**
   * Below method will be used to put the data to memory
   *
   * @param data
   */
  void putData(T data);

  /**
   * to get byte value
   *
   * @param index
   * @return byte value based on index
   */
  byte getByte(int index);

  /**
   * to get the short value
   *
   * @param index
   * @return short value based on index
   */
  short getShort(int index);

  /**
   * to get the int value
   *
   * @param index
   * @return int value based on index
   */
  int getInt(int index);

  /**
   * to get the long value
   *
   * @param index
   * @return long value based on index
   */
  long getLong(int index);

  /**
   * to get the double value
   *
   * @param index
   * @return double value based on index
   */
  double getDouble(int index);

  /**
   * To get the bigdecimal value
   *
   * @param index
   * @return bigdecimal value based on index
   */
  BigDecimal getBigDecimal(int index);

  /**
   * To free the occupied memory
   */
  void freeMemory();
}
