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

package org.apache.carbondata.processing.loading.sort;

import java.util.Iterator;

import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;

/**
 * This interface sorts all the data of iterators.
 * The life cycle of this interface is initialize -> sort -> close
 */
public interface Sorter {

  /**
   * Initialize sorter with sort parameters.
   *
   * @param sortParameters
   */
  void initialize(SortParameters sortParameters);

  /**
   * Sorts the data of all iterators, this iterators can be
   * read parallely depends on implementation.
   *
   * @param iterators array of iterators to read data.
   * @return
   * @throws CarbonDataLoadingException
   */
  Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException;

  /**
   * Close resources
   */
  void close();

}
