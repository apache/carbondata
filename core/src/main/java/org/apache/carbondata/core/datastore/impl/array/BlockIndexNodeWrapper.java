package org.apache.carbondata.core.datastore.impl.array;

import java.io.IOException;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;

/**
 * Created by root1 on 23/5/17.
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
    if(indexStore.isKeyAvailableAtIndex(index + 1)) {
      return new BlockIndexNodeWrapper(indexStore, (short)(index + 1));
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
      return ((BlockIndexStore)indexStore).getTableBlockInfo(index);
    }
    return null;
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

}
