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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * This is a wrapper iterator over the detail raw query iterator.
 * This iterator will handle the processing of the raw rows.
 * This will handle the batch results and will iterate on the batches and give single row.
 */
public class RawResultIterator extends CarbonIterator<Object[]> {

  private final SegmentProperties sourceSegProperties;

  private final SegmentProperties destinationSegProperties;
  /**
   * Iterator of the Batch raw result.
   */
  private CarbonIterator<BatchRawResult> detailRawQueryResultIterator;

  /**
   * Counter to maintain the row counter.
   */
  private int counter = 0;

  private Object[] currentConveretedRawRow = null;

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RawResultIterator.class.getName());

  /**
   * batch of the result.
   */
  private BatchRawResult batch;

  public RawResultIterator(CarbonIterator<BatchRawResult> detailRawQueryResultIterator,
      SegmentProperties sourceSegProperties, SegmentProperties destinationSegProperties) {
    this.detailRawQueryResultIterator = detailRawQueryResultIterator;
    this.sourceSegProperties = sourceSegProperties;
    this.destinationSegProperties = destinationSegProperties;
  }

  @Override public boolean hasNext() {

    if (null == batch || checkIfBatchIsProcessedCompletely(batch)) {
      if (detailRawQueryResultIterator.hasNext()) {
        batch = null;
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
      try {
        if(null != currentConveretedRawRow){
          counter++;
          Object[] currentConveretedRawRowTemp = this.currentConveretedRawRow;
          currentConveretedRawRow = null;
          return currentConveretedRawRowTemp;
        }
        return convertRow(batch.getRawRow(counter++));
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage());
        return null;
      }
    } else { // completed one batch.
      batch = null;
      batch = detailRawQueryResultIterator.next();
      counter = 0;
    }
    try {
      if(null != currentConveretedRawRow){
        counter++;
        Object[] currentConveretedRawRowTemp = this.currentConveretedRawRow;
        currentConveretedRawRow = null;
        return currentConveretedRawRowTemp;
      }

      return convertRow(batch.getRawRow(counter++));
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage());
      return null;
    }

  }

  /**
   * for fetching the row with out incrementing counter.
   * @return
   */
  public Object[] fetchConverted() throws KeyGenException {
    if(null != currentConveretedRawRow){
      return currentConveretedRawRow;
    }
    if(hasNext())
    {
      Object[] rawRow = batch.getRawRow(counter);
      currentConveretedRawRow = convertRow(rawRow);;
      return currentConveretedRawRow;
    }
    else
    {
      return null;
    }
  }

  private Object[] convertRow(Object[] rawRow) throws KeyGenException {
    byte[] dims = ((ByteArrayWrapper) rawRow[0]).getDictionaryKey();
    long[] keyArray = sourceSegProperties.getDimensionKeyGenerator().getKeyArray(dims);
    byte[] covertedBytes =
        destinationSegProperties.getDimensionKeyGenerator().generateKey(keyArray);
    ((ByteArrayWrapper) rawRow[0]).setDictionaryKey(covertedBytes);
    return rawRow;
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
