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
package org.apache.carbondata.core.writer.sortindex;

import java.io.Closeable;
import java.util.List;

/**
 * Interface for writing the dictionary sort index and sort index revers data.
 */
public interface CarbonDictionarySortIndexWriter extends Closeable {

  /**
   * The method is used write the dictionary sortIndex data to columns
   * sortedIndex file in thrif format.
   *
   * @param sortIndexList list of sortIndex
   */
  void writeSortIndex(List<Integer> sortIndexList);

  /**
   * The method is used write the dictionary sortIndexInverted data to columns
   * sortedIndex file in thrif format.
   *
   * @param invertedSortIndexList list of  sortIndexInverted
   */
  void writeInvertedSortIndex(List<Integer> invertedSortIndexList);

}
