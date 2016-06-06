/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.query.carbon.result.iterator;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.result.BatchRawResult;

/**
 * This is a wrapper iterator over the detail raw query iterator.
 * This iterator will handle the processing of the raw rows.
 * This will handle the batch results and will iterate on the batches and give single row.
 */
public class RawResultIterator extends CarbonIterator<Object[]> {

  /**
   * Iterator of the Batch raw result.
   */
  private CarbonIterator<BatchRawResult> detailRawQueryResultIterator;

  /**
   * Counter to maintain the row counter.
   */
  private int counter = 0;

  /**
   * batch of the result.
   */
  private BatchRawResult batch;

  public RawResultIterator(CarbonIterator<BatchRawResult> detailRawQueryResultIterator) {
    this.detailRawQueryResultIterator = detailRawQueryResultIterator;
  }

  @Override public boolean hasNext() {

    if (null == batch || checkIfBatchIsProcessedCompletely(batch)) {
      if (detailRawQueryResultIterator.hasNext()) {
        batch = detailRawQueryResultIterator.next();
        counter = 0; // batch changed so reset the counter.
      } else {
        return false;
      }
    }

    if (!checkIfBatchIsProcessedCompletely(batch)) {
      return true;
    } else {
      return false;
    }
  }

  @Override public Object[] next() {

    if (null == batch) { // for 1st time
      batch = detailRawQueryResultIterator.next();
    }
    if (!checkIfBatchIsProcessedCompletely(batch)) {
      return batch.getRawRow(counter++);
    } else { // completed one batch.
      batch = detailRawQueryResultIterator.next();
      counter = 0;
    }
    return batch.getRawRow(counter++);

  }

  /**
   * for fetching the row with out incrementing counter.
   * @return
   */
  public Object[] fetch(){
    if(hasNext())
    {
      return batch.getRawRow(counter);
    }
    else
    {
      return null;
    }
  }

  /**
   * To check if the batch is processed completely
   * @param batch
   * @return
   */
  private boolean checkIfBatchIsProcessedCompletely(BatchRawResult batch){
    if(counter < batch.getSize())
    {
      return false;
    }
    else{
      return true;
    }
  }
}
