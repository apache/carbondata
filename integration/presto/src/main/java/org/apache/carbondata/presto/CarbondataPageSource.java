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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.presto.readers.StreamReader;
import org.apache.carbondata.presto.readers.StreamReaders;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.type.Type;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
class CarbondataPageSource implements ConnectorPageSource {

  private final RecordCursor cursor;
  private final List<Type> types;
  private final PageBuilder pageBuilder;
  private final StreamReader[] readers;
  private boolean closed;
  private CarbonIterator<BatchResult> columnCursor;
  private CarbonDictionaryDecodeReadSupport<Object[]> readSupport;
  private long sizeOfData = 0;
  private int batchId;

  public CarbondataPageSource(RecordSet recordSet) {
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
  }

  private CarbondataPageSource(List<Type> types, RecordCursor cursor) {
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
    this.columnCursor = ((CarbondataRecordCursor) cursor).getColumnCursor();
    this.readSupport = ((CarbondataRecordCursor) cursor).getReadSupport();
    this.readers = createStreamReaders();
  }

  @Override public long getTotalBytes() {
    return cursor.getTotalBytes();
  }

  @Override public long getCompletedBytes() {
    return cursor.getCompletedBytes();
  }

  @Override public long getReadTimeNanos() {
    return cursor.getReadTimeNanos();
  }

  @Override public boolean isFinished() {
    return closed && pageBuilder.isEmpty();
  }

  @Override public Page getNextPage() {

    BatchResult columnBatch;
    List<Object[]> columnData;
    int batchSize = 0;
    try {
      batchId++;
      if (columnCursor.hasNext()) {
        columnBatch = columnCursor.next();
        columnData = columnBatch.getRows();
        if (columnBatch.getSize() > 0) {
          batchSize = columnBatch.getRows().get(0).length;
          if (batchSize <= 0) {
            close();
            return null;
          }
        }
        if (columnData.size() != types.size() && types.size() != 0) {
          close();
          return null;
        }
      } else {
        close();
        return null;
      }

      Block[] blocks = new Block[types.size()];
      for (int column = 0; column < blocks.length; column++) {
        Type type = types.get(column);
        readers[column].setStreamData(columnData.get(column));
        blocks[column] = new LazyBlock(batchSize, new CarbondataBlockLoader(column, type));
      }
      Page page = new Page(batchSize, blocks);
      sizeOfData += page.getSizeInBytes();
      return page;
    } catch (PrestoException e) {
      closeWithSuppression(e);
      throw e;
    } catch (RuntimeException e) {
      closeWithSuppression(e);
      throw new RuntimeException("Exception when creating the Carbon data Block", e);
    }

  }

  @Override public long getSystemMemoryUsage() {
    ((CarbondataRecordCursor) cursor).addTotalBytes(sizeOfData);
    return cursor.getSystemMemoryUsage();
  }

  @Override public void close() {
    // some hive input formats are broken and bad things can happen if you close them multiple times
    if (closed) {
      return;
    }
    closed = true;

    try {
      columnCursor.close();
      cursor.close();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  /**
   * Method added to suppress the excecption
   *
   * @param throwable
   */
  private void closeWithSuppression(Throwable throwable) {
    requireNonNull(throwable, "throwable is null");
    try {
      close();
    } catch (RuntimeException e) {
      // Self-suppression not permitted
      if (throwable != e) {
        throwable.addSuppressed(e);
      }
    }
  }

  /**
   * Create the Stream Reader for every column based on their type
   * This method will be initialized only once based on the types.
   *
   * @return
   */
  private StreamReader[] createStreamReaders() {
    requireNonNull(types);
    StreamReader[] readers = new StreamReader[types.size()];
    for (int i = 0; i < types.size(); i++) {
      readers[i] = StreamReaders.createStreamReader(types.get(i), null);
    }
    return readers;
  }

  /**
   * Implemented the lazy Blocks for efficiency in data exchange
   */
  private final class CarbondataBlockLoader implements LazyBlockLoader<LazyBlock> {
    private final int expectedBatchId = batchId;
    private final int columnIndex;
    private final Type type;
    private boolean loaded;

    CarbondataBlockLoader(int columnIndex, Type type) {
      this.columnIndex = columnIndex;
      this.type = requireNonNull(type, "type is null");
    }

    @Override public final void load(LazyBlock lazyBlock) {
      if (loaded) {
        return;
      }

      checkState(batchId == expectedBatchId);

      try {
        Block block = readers[columnIndex].readBlock(type);
        lazyBlock.setBlock(block);
      } catch (IOException e) {
        throw new CarbonDataLoadingException("Error in Reading Data from Carbondata ", e);
      }

      loaded = true;
    }
  }

}