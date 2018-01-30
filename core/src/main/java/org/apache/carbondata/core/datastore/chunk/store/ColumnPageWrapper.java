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

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

public class ColumnPageWrapper implements DimensionColumnPage {

  private ColumnPage columnPage;

  public ColumnPageWrapper(ColumnPage columnPage) {
    this.columnPage = columnPage;
  }

  @Override
  public int fillRawData(int rowId, int offset, byte[] data, KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex,
      KeyStructureInfo restructuringInfo) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getChunkData(int rowId) {
    return columnPage.getBytes(rowId);
  }

  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void freeMemory() {

  }

}
