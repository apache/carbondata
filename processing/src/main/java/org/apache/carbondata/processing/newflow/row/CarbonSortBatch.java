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
package org.apache.carbondata.processing.newflow.row;

import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeSingleThreadFinalSortFilesMerger;

/**
 * Batch of sorted rows which are ready to be processed by
 */
public class CarbonSortBatch extends CarbonRowBatch {

  private UnsafeSingleThreadFinalSortFilesMerger iterator;

  public CarbonSortBatch(UnsafeSingleThreadFinalSortFilesMerger iterator) {
    super(0);
    this.iterator = iterator;
  }

  @Override public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override public CarbonRow next() {
    return new CarbonRow(iterator.next());
  }

  @Override public void close() {
    iterator.close();
  }
}
