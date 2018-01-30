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
package org.apache.carbondata.core.scan.result.iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.RowBatch;

public class PartitionSpliterRawResultIterator extends CarbonIterator<Object[]> {

  private CarbonIterator<RowBatch> iterator;
  private RowBatch batch;
  private int counter;

  public PartitionSpliterRawResultIterator(CarbonIterator<RowBatch> iterator) {
    this.iterator = iterator;
  }

  @Override public boolean hasNext() {
    if (null == batch || checkBatchEnd(batch)) {
      if (iterator.hasNext()) {
        batch = iterator.next();
        counter = 0;
      } else {
        return false;
      }
    }

    return !checkBatchEnd(batch);
  }

  @Override public Object[] next() {
    if (batch == null) {
      batch = iterator.next();
    }
    if (!checkBatchEnd(batch)) {
      return batch.getRawRow(counter++);
    } else {
      batch = iterator.next();
      counter = 0;
    }
    return batch.getRawRow(counter++);
  }

  /**
   * To check if the batch is processed completely
   * @param batch
   * @return
   */
  private boolean checkBatchEnd(RowBatch batch) {
    return !(counter < batch.getSize());
  }

}
