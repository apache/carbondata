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
package org.apache.carbondata.core.datastore.impl.array;

import java.io.IOException;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;

/**
 * It is just a wrapper to keep the current interfaces useful with minimal changes.
 * TODO it can be removed in future after btree is removed
 */
public class BlockIndexNodeWrapper implements DataRefNode {

  private IndexStore indexStore;

  private short index;

  private BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache;

  public BlockIndexNodeWrapper(IndexStore indexStore, short index) {
    this.indexStore = indexStore;
    this.index = index;
  }

  @Override public DataRefNode getNextDataRefNode() {
    if (indexStore.isKeyAvailableAtIndex(index + 1)) {
      return new BlockIndexNodeWrapper(indexStore, (short) (index + 1));
    }
    return null;
  }

  @Override public int nodeSize() {
    return indexStore.getRowCount(index);
  }

  @Override public long nodeNumber() {
    return index;
  }

  @Override public byte[][] getColumnsMaxValue() {
    return indexStore.getMaxs(index);
  }

  @Override public byte[][] getColumnsMinValue() {
    return indexStore.getMins(index);
  }

  @Override
  public DimensionRawColumnChunk[] getDimensionChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    return null;
  }

  @Override
  public DimensionRawColumnChunk getDimensionChunk(FileHolder fileReader, int blockIndexes)
      throws IOException {
    return null;
  }

  @Override
  public MeasureRawColumnChunk[] getMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException {
    return null;
  }

  @Override public MeasureRawColumnChunk getMeasureChunk(FileHolder fileReader, int blockIndex)
      throws IOException {
    return null;
  }

  @Override
  public void setDeleteDeltaDataCache(BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache) {
    this.deleteDeltaDataCache = deleteDeltaDataCache;
  }

  @Override public BlockletLevelDeleteDeltaDataCache getDeleteDeltaDataCache() {
    return deleteDeltaDataCache;
  }

  @Override public int numberOfPages() {
    throw new UnsupportedOperationException();
  }

  public TableBlockInfo getTableBlockInfo(int index) throws IOException {
    if (indexStore instanceof BlockIndexStore) {
      return ((BlockIndexStore) indexStore).getTableBlockInfo(index);
    }
    return null;
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BlockIndexNodeWrapper that = (BlockIndexNodeWrapper) o;

    if (index != that.index) return false;
    if (!indexStore.equals(that.indexStore)) return false;
    return true;
  }

  @Override public int hashCode() {
    int result = indexStore.hashCode();
    result = 31 * result + (int) index;
    return result;
  }
}
