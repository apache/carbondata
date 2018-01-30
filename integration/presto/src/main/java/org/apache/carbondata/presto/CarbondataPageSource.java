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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.presto.readers.StreamReader;
import org.apache.carbondata.presto.readers.StreamReaders;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

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

  private static final LogService logger =
      LogServiceFactory.getLogService(CarbondataPageSource.class.getName());
  private final RecordCursor cursor;
  private final List<Type> types;
  private final PageBuilder pageBuilder;
  private boolean closed;
  private PrestoCarbonVectorizedRecordReader vectorReader;
  private CarbonDictionaryDecodeReadSupport<Object[]> readSupport;
  private long sizeOfData = 0;

  private final StreamReader[] readers ;
  private int batchId;

  private long nanoStart;
  private long nanoEnd;

  CarbondataPageSource(RecordSet recordSet) {
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(),
        recordSet.cursor());
  }

  private CarbondataPageSource(List<Type> types, RecordCursor cursor) {
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
    this.readSupport = ((CarbondataRecordCursor) cursor).getReadSupport();
    this.vectorReader = ((CarbondataRecordCursor) cursor).getVectorizedRecordReader();
    this.readers = createStreamReaders();
  }

  @Override public long getCompletedBytes() {
    return sizeOfData;
  }

  @Override public long getReadTimeNanos() {
    return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
  }

  @Override public boolean isFinished() {
    return closed && pageBuilder.isEmpty();
  }


  @Override public Page getNextPage() {
    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }
    CarbonVectorBatch columnarBatch = null;
    int batchSize = 0;
    try {
      batchId++;
      if(vectorReader.nextKeyValue()) {
        Object vectorBatch = vectorReader.getCurrentValue();
        if(vectorBatch != null && vectorBatch instanceof CarbonVectorBatch)
        {
          columnarBatch = (CarbonVectorBatch) vectorBatch;
          batchSize = columnarBatch.numRows();
          if(batchSize == 0){
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

      Block[] blocks = new Block[types.size()];
      for (int column = 0; column < blocks.length; column++) {
        Type type = types.get(column);
        readers[column].setBatchSize(columnarBatch.numRows());
        readers[column].setVectorReader(true);
        readers[column].setVector(columnarBatch.column(column));
        blocks[column] = new LazyBlock(batchSize, new CarbondataBlockLoader(column, type));
      }
      Page page = new Page(batchSize, blocks);
      sizeOfData += columnarBatch.capacity();
      return page;
    }
    catch (PrestoException e) {
      closeWithSuppression(e);
      throw e;
    }
    catch ( RuntimeException | InterruptedException | IOException e) {
      closeWithSuppression(e);
      throw new CarbonDataLoadingException("Exception when creating the Carbon data Block", e);
    }
  }

  @Override public long getSystemMemoryUsage() {
    return sizeOfData;
  }

  @Override public void close()  {
    // some hive input formats are broken and bad things can happen if you close them multiple times
    if (closed) {
      return;
    }
    closed = true;
    try {
      vectorReader.close();
      cursor.close();
      nanoEnd = System.nanoTime();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  protected void closeWithSuppression(Throwable throwable)
  {
    requireNonNull(throwable, "throwable is null");
    try {
      close();
    }
    catch (RuntimeException e) {
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
  private final class CarbondataBlockLoader
      implements LazyBlockLoader<LazyBlock>
  {
    private final int expectedBatchId = batchId;
    private final int columnIndex;
    private final Type type;
    private boolean loaded;

    public CarbondataBlockLoader(int columnIndex, Type type)
    {
      this.columnIndex = columnIndex;
      this.type = requireNonNull(type, "type is null");
    }

    @Override
    public final void load(LazyBlock lazyBlock)
    {
      if (loaded) {
        return;
      }
      checkState(batchId == expectedBatchId);
      try {
        Block block = readers[columnIndex].readBlock(type);
        lazyBlock.setBlock(block);
      }
      catch (IOException e) {
        throw new CarbonDataLoadingException("Error in Reading Data from Carbondata ", e);
      }
      loaded = true;
    }
  }


  /**
   * Create the Stream Reader for every column based on their type
   * This method will be initialized only once based on the types.
   *
   * @return
   */
  private StreamReader[] createStreamReaders( ) {
    requireNonNull(types);
    StreamReader[] readers = new StreamReader[types.size()];
    for (int i = 0; i < types.size(); i++) {
      readers[i] = StreamReaders.createStreamReader(types.get(i), readSupport
          .getSliceArrayBlock(i),readSupport.getDictionaries()[i]);
    }
    return readers;
  }

}