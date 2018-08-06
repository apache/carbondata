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

package org.apache.carbondata.presto;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.presto.readers.PrestoVectorBlockBuilder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;

import static com.google.common.base.Preconditions.checkState;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
class CarbondataPageSource implements ConnectorPageSource {

  private static final LogService logger =
      LogServiceFactory.getLogService(CarbondataPageSource.class.getName());
  private List<ColumnHandle> columnHandles;
  private boolean closed;
  private PrestoCarbonVectorizedRecordReader vectorReader;
  private long sizeOfData = 0;
  private int batchId;
  private long nanoStart;
  private long nanoEnd;

  CarbondataPageSource(PrestoCarbonVectorizedRecordReader vectorizedRecordReader,
      List<ColumnHandle> columnHandles) {
    this.columnHandles = columnHandles;
    vectorReader = vectorizedRecordReader;
  }

  @Override public long getCompletedBytes() {
    return sizeOfData;
  }

  @Override public long getReadTimeNanos() {
    return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
  }

  @Override public boolean isFinished() {
    return closed;
  }

  @Override public Page getNextPage() {
    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }
    CarbonVectorBatch columnarBatch = null;
    int batchSize = 0;
    try {
      batchId++;
      if (vectorReader.nextKeyValue()) {
        Object vectorBatch = vectorReader.getCurrentValue();
        if (vectorBatch instanceof CarbonVectorBatch) {
          columnarBatch = (CarbonVectorBatch) vectorBatch;
          batchSize = columnarBatch.numRows();
          if (batchSize == 0) {
            close();
            return null;
          }
        }
      } else {
        close();
        return null;
      }
      if (columnarBatch == null) {
        return null;
      }

      Block[] blocks = new Block[columnHandles.size()];
      for (int column = 0; column < blocks.length; column++) {
        blocks[column] = new LazyBlock(batchSize, new CarbondataBlockLoader(column));
      }
      Page page = new Page(batchSize, blocks);
      return page;
    } catch (PrestoException e) {
      closeWithSuppression(e);
      throw e;
    } catch (RuntimeException | InterruptedException | IOException e) {
      closeWithSuppression(e);
      throw new CarbonDataLoadingException("Exception when creating the Carbon data Block", e);
    }
  }

  @Override public long getSystemMemoryUsage() {
    return sizeOfData;
  }

  @Override public void close() {
    // some hive input formats are broken and bad things can happen if you close them multiple times
    if (closed) {
      return;
    }
    closed = true;
    try {
      vectorReader.close();
      nanoEnd = System.nanoTime();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  private void closeWithSuppression(Throwable throwable) {
    requireNonNull(throwable, "throwable is null");
    try {
      close();
    } catch (RuntimeException e) {
      // Self-suppression not permitted
      logger.error(e, e.getMessage());
      if (throwable != e) {
        throwable.addSuppressed(e);
      }
    }
  }

  /**
   * Lazy Block Implementation for the Carbondata
   */
  private final class CarbondataBlockLoader implements LazyBlockLoader<LazyBlock> {
    private final int expectedBatchId = batchId;
    private final int columnIndex;
    private boolean loaded;

    CarbondataBlockLoader(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    @Override public final void load(LazyBlock lazyBlock) {
      if (loaded) {
        return;
      }
      checkState(batchId == expectedBatchId);
      try {
        PrestoVectorBlockBuilder blockBuilder =
            (PrestoVectorBlockBuilder) vectorReader.getColumnarBatch().column(columnIndex);
        blockBuilder.setBatchSize(lazyBlock.getPositionCount());
        Block block = blockBuilder.buildBlock();
        sizeOfData += block.getSizeInBytes();
        lazyBlock.setBlock(block);
      } catch (Exception e) {
        throw new CarbonDataLoadingException("Error in Reading Data from Carbondata ", e);
      }
      loaded = true;
    }
  }

}