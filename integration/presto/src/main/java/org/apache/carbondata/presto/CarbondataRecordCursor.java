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

import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.result.BatchResult;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

public class CarbondataRecordCursor implements RecordCursor {

  private final List<CarbondataColumnHandle> columnHandles;

  private CarbonIterator<BatchResult> columnCursor;

  private CarbonDictionaryDecodeReadSupport<Object[]> readSupport;

  private long totalBytes;
  private long nanoStart;
  private long nanoEnd;

  public CarbondataRecordCursor(CarbonDictionaryDecodeReadSupport<Object[]> readSupport,
      CarbonIterator<BatchResult> carbonIterator, List<CarbondataColumnHandle> columnHandles) {
    this.columnCursor = carbonIterator;
    this.columnHandles = columnHandles;
    this.readSupport = readSupport;
    this.totalBytes = 0;
  }

  @Override public long getTotalBytes() {
    return totalBytes;
  }

  @Override public long getCompletedBytes() {
    return totalBytes;
  }

  @Override public long getReadTimeNanos() {
    return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
  }

  @Override public Type getType(int field) {
    checkArgument(field < columnHandles.size(), "Invalid field index");
    return columnHandles.get(field).getColumnType();
  }

  /**
   * get next Row/Page
   */
  @Override public boolean advanceNextPosition() {

    if (nanoStart == 0) {
      nanoStart = System.nanoTime();
    }

    return columnCursor.hasNext();
  }

  @Override public boolean getBoolean(int field) {
    throw new UnsupportedOperationException("Call the stream reader instead");
  }

  @Override public long getLong(int field) {
    throw new UnsupportedOperationException("Call the stream reader instead");
  }

  @Override public double getDouble(int field) {
    throw new UnsupportedOperationException("Call the stream reader instead");
  }

  @Override public Slice getSlice(int field) {
    throw new UnsupportedOperationException("Call the stream reader instead");
  }

  @Override public Object getObject(int field) {
    return null;
  }

  @Override public boolean isNull(int field) {
    throw new UnsupportedOperationException("Call the stream reader instead");
  }

  @Override public void close() {
    nanoEnd = System.nanoTime();
    readSupport.close();
    //todo  delete cache from readSupport
  }

  public CarbonIterator<BatchResult> getColumnCursor() {
    return columnCursor;
  }

  public CarbonDictionaryDecodeReadSupport<Object[]> getReadSupport() {
    return readSupport;
  }

  @Override public long getSystemMemoryUsage() {
    return totalBytes;
  }

  public void addTotalBytes(long totalBytes) {
    this.totalBytes = totalBytes;
  }
}