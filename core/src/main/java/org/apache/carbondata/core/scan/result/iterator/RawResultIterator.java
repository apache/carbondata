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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.scan.result.RowBatch;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * This is a wrapper iterator over the detail raw query iterator.
 * This iterator will handle the processing of the raw rows.
 * This will handle the batch results and will iterate on the batches and give single row.
 */
public class RawResultIterator extends CarbonIterator<Object[]> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RawResultIterator.class.getName());

  private final SegmentProperties sourceSegProperties;

  private final SegmentProperties destinationSegProperties;
  /**
   * Iterator of the Batch raw result.
   */
  private CarbonIterator<RowBatch> detailRawQueryResultIterator;

  private boolean prefetchEnabled;
  private List<Object[]> currentBuffer;
  private List<Object[]> backupBuffer;
  private int currentIdxInBuffer;
  private ExecutorService executorService;
  private Future<Void> fetchFuture;
  private Object[] currentRawRow = null;
  private boolean isBackupFilled = false;

  public RawResultIterator(CarbonIterator<RowBatch> detailRawQueryResultIterator,
      SegmentProperties sourceSegProperties, SegmentProperties destinationSegProperties,
      boolean isStreamingHandOff) {
    this.detailRawQueryResultIterator = detailRawQueryResultIterator;
    this.sourceSegProperties = sourceSegProperties;
    this.destinationSegProperties = destinationSegProperties;
    this.executorService = Executors.newFixedThreadPool(1);

    if (!isStreamingHandOff) {
      init();
    }
  }

  private void init() {
    this.prefetchEnabled = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE,
        CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_ENABLE_DEFAULT).equalsIgnoreCase("true");
    try {
      new RowsFetcher(false).call();
      if (prefetchEnabled) {
        this.fetchFuture = executorService.submit(new RowsFetcher(true));
      }
    } catch (Exception e) {
      LOGGER.error(e, "Error occurs while fetching records");
      throw new RuntimeException(e);
    }
  }

  /**
   * fetch rows
   */
  private final class RowsFetcher implements Callable<Void> {
    private boolean isBackupFilling;

    private RowsFetcher(boolean isBackupFilling) {
      this.isBackupFilling = isBackupFilling;
    }

    @Override
    public Void call() throws Exception {
      if (isBackupFilling) {
        backupBuffer = fetchRows();
        isBackupFilled = true;
      } else {
        currentBuffer = fetchRows();
      }
      return null;
    }
  }

  private List<Object[]> fetchRows() {
    if (detailRawQueryResultIterator.hasNext()) {
      return detailRawQueryResultIterator.next().getRows();
    } else {
      return new ArrayList<>();
    }
  }

  private void fillDataFromPrefetch() {
    try {
      if (currentIdxInBuffer >= currentBuffer.size() && 0 != currentIdxInBuffer) {
        if (prefetchEnabled) {
          if (!isBackupFilled) {
            fetchFuture.get();
          }
          // copy backup buffer to current buffer and fill backup buffer asyn
          currentIdxInBuffer = 0;
          currentBuffer = backupBuffer;
          isBackupFilled = false;
          fetchFuture = executorService.submit(new RowsFetcher(true));
        } else {
          currentIdxInBuffer = 0;
          new RowsFetcher(false).call();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * populate a row with index counter increased
   */
  private void popRow() {
    fillDataFromPrefetch();
    currentRawRow = currentBuffer.get(currentIdxInBuffer);
    currentIdxInBuffer++;
  }

  /**
   * populate a row with index counter unchanged
   */
  private void pickRow() {
    fillDataFromPrefetch();
    currentRawRow = currentBuffer.get(currentIdxInBuffer);
  }

  @Override
  public boolean hasNext() {
    fillDataFromPrefetch();
    if (currentIdxInBuffer < currentBuffer.size()) {
      return true;
    }

    return false;
  }

  @Override
  public Object[] next() {
    try {
      popRow();
      return convertRow(this.currentRawRow);
    } catch (KeyGenException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * for fetching the row with out incrementing counter.
   * @return
   */
  public Object[] fetchConverted() throws KeyGenException {
    pickRow();
    return convertRow(this.currentRawRow);
  }

  private Object[] convertRow(Object[] rawRow) throws KeyGenException {
    byte[] dims = ((ByteArrayWrapper) rawRow[0]).getDictionaryKey();
    long[] keyArray = sourceSegProperties.getDimensionKeyGenerator().getKeyArray(dims);
    byte[] convertedBytes =
        destinationSegProperties.getDimensionKeyGenerator().generateKey(keyArray);
    ((ByteArrayWrapper) rawRow[0]).setDictionaryKey(convertedBytes);
    return rawRow;
  }

  public void close() {
    if (null != executorService) {
      executorService.shutdownNow();
    }
  }
}
