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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Carbondata Page Source class for custom Carbondata RecordSet Iteration.
 */
public class CarbondataPageSource implements ConnectorPageSource {

  private static final int ROWS_PER_REQUEST = 4096;
  private final RecordCursor cursor;
  private final List<Type> types;
  private final PageBuilder pageBuilder;
  private boolean closed;
  private final char[] buffer = new char[100];
  private Block[] blocks;

  public CarbondataPageSource(RecordSet recordSet) {
    this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
  }

  public CarbondataPageSource(List<Type> types, RecordCursor cursor) {
    this.cursor = requireNonNull(cursor, "cursor is null");
    this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
    this.pageBuilder = new PageBuilder(this.types);
    this.blocks = new Block[types.size()];
  }

  public RecordCursor getCursor() {
    return cursor;
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
    BlockBuilder output;
    Page page;
    int size = types.size();
    if (!closed) {
      int i;
      for (i = 0; i < ROWS_PER_REQUEST; i++) {
        if (pageBuilder.isFull()) {
          break;
        }
        if (!cursor.advanceNextPosition()) {
          closed = true;
          break;
        }

        pageBuilder.declarePosition();

        for (int column = 0; column < size; column++) {
          output = pageBuilder.getBlockBuilder(column);
          if (cursor.isNull(column)) {
            output.appendNull();
          } else {
            Type type = types.get(column);
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
              type.writeBoolean(output, cursor.getBoolean(column));
            } else if (javaType == long.class) {
              type.writeLong(output, cursor.getLong(column));
            } else if (javaType == double.class) {
              type.writeDouble(output, cursor.getDouble(column));
            } else if (javaType == Slice.class) {
              Slice slice = cursor.getSlice(column);
              if (type instanceof DecimalType) {
                if (isShortDecimal(type)) {
                  type.writeLong(output, parseLong((DecimalType) type, slice, 0, slice.length()));
                } else {
                  type.writeSlice(output, parseSlice((DecimalType) type, slice, 0, slice.length()));
                }
              } else {
                type.writeSlice(output, slice, 0, slice.length());
              }
            } else {
              type.writeObject(output, cursor.getObject(column));
            }
          }
          blocks[column] = new LazyBlock(output.getPositionCount(),
              new CarbonBlockLoader(output.build(), types.get(column)));
        }
      }
    }

    // only return a page if the buffer is full or we are finishing
    if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
      return null;
    }

    if (blocks != null && blocks.length > 0) {
      page = new Page(blocks[0].getPositionCount(), blocks);
    } else {
      page = pageBuilder.build();
    }

    pageBuilder.reset();
    return page;
  }

  @Override public long getSystemMemoryUsage() {
    return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
  }

  @Override public void close() throws IOException {
    closed = true;
    cursor.close();

  }

  private long parseLong(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return decimal.unscaledValue().longValue();
  }

  private Slice parseSlice(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return encodeUnscaledValue(decimal.unscaledValue());
  }

  private BigDecimal parseBigDecimal(DecimalType type, Slice slice, int offset, int length) {
    checkArgument(length < buffer.length);
    for (int i = 0; i < length; i++) {
      buffer[i] = (char) slice.getByte(offset + i);
    }
    BigDecimal decimal = new BigDecimal(buffer, 0, length);
    checkState(decimal.scale() <= type.getScale(),
        "Read decimal value scale larger than column scale");
    decimal = decimal.setScale(type.getScale(), HALF_UP);
    checkState(decimal.precision() <= type.getPrecision(),
        "Read decimal precision larger than column precision");
    return decimal;
  }

  /**
   * Using the LazyBlockLoader
   */
  private static final class CarbonBlockLoader implements LazyBlockLoader<LazyBlock> {
    private boolean loaded;
    private Block dataBlock;

    public CarbonBlockLoader(Block dataBlock, Type type) {
      this.dataBlock = dataBlock;
    }

    @Override public void load(LazyBlock block) {
      if (loaded) {
        return;
      }
      block.setBlock(dataBlock);
      loaded = true;
    }
  }
}
