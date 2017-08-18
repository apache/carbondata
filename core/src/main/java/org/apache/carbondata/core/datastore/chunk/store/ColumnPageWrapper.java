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

package org.apache.carbondata.core.datastore.chunk.store;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

public class ColumnPageWrapper implements DimensionColumnDataChunk {

  private ColumnPage columnPage;

  public ColumnPageWrapper(ColumnPage columnPage) {
    this.columnPage = columnPage;
  }

  @Override
  public int fillChunkData(byte[] data, int offset, int columnIndex,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getChunkData(int columnIndex) {
    return columnPage.getBytes(columnIndex);
  }

  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override
  public int getColumnValueSize() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int index, byte[] compareValue) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void freeMemory() {

  }

}
