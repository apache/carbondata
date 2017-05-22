package org.apache.carbondata.core.datastore.impl.array;

import java.util.List;

import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryAllocator;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;

/**
 * Created by root1 on 18/5/17.
 */
public class BlockArrayIndexBuilder implements BtreeBuilder {

  /**
   * Maintain pointers in the following way.
   * For suppose we have 2 indexes
   * key pointer = <key1-address><key2-address>
   * Fixed max pointer = <fmax1-address><fmax2-address>
   * Fixed min pointer = <fmin1-address><fmin2-address>
   * Variable max pointer = <vmax1-address><vmax2-address>
   * Variable min pointer = <vmin1-address><vmin2-address>
   * Rownum-pointer = <row-num-address>
   * startkey => <fixkeylength><variablekeylength><fixkey><variablekey>
   *
   * @param btreeBuilderInfo
   */
  @Override public void build(BTreeBuilderInfo btreeBuilderInfo) {
    int indexSize = 0;
    List<DataFileFooter> footerList = btreeBuilderInfo.getFooterList();
    // First calculate the size of complete index to store in memory
    for (int metadataIndex = 0; metadataIndex < footerList.size(); metadataIndex++) {
      DataFileFooter footer = footerList.get(metadataIndex);
      //Key pointer
      byte[] startKey = footer.getBlockletIndex().getBtreeIndex().getStartKey();
      indexSize += startKey.length;
      // Key pointer size in int
      indexSize += 4;

      BlockletMinMaxIndex minMaxIndex = footer.getBlockletIndex().getMinMaxIndex();
      // max calculation
      byte[][] maxValues = minMaxIndex.getMaxValues();
      for (int i = 0; i < maxValues.length; i++) {
        //If it is variable length size in short + actual data length
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] == -1) {
          indexSize += 2;
        }
        indexSize += maxValues[i].length;
      }

      // min calculation
      byte[][] minValues = minMaxIndex.getMaxValues();
      for (int i = 0; i < minValues.length; i++) {
        //If it is variable length size in short + actual data length
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] == -1) {
          indexSize += 2;
        }
        indexSize += minValues[i].length;
      }

      //row num in int
      indexSize += 4;
    }

    MemoryAllocator memoryAllocator = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator();
    MemoryBlock block = memoryAllocator.allocate(indexSize);

    int[] rowPointers = new int[footerList.size()];
    long address = block.getBaseOffset();
    Object baseObject = block.getBaseObject();
    indexSize = 0;
    // First calculate the size of complete index to store in memory
    for (int metadataIndex = 0; metadataIndex < footerList.size(); metadataIndex++) {
      DataFileFooter footer = footerList.get(metadataIndex);
      rowPointers[metadataIndex] = indexSize;
      CarbonUnsafe.unsafe.putInt(address + indexSize, (int) footer.getNumberOfRows());
      indexSize += 4;
      //Key pointer
      byte[] startKey = footer.getBlockletIndex().getBtreeIndex().getStartKey();
      CarbonUnsafe.unsafe.putInt(address + indexSize, startKey.length);
      indexSize += 4;
      CarbonUnsafe.unsafe
          .copyMemory(startKey, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + indexSize,
              startKey.length);
      indexSize += startKey.length;

      BlockletMinMaxIndex minMaxIndex = footer.getBlockletIndex().getMinMaxIndex();

      // max calculation for fixed values
      byte[][] maxValues = minMaxIndex.getMaxValues();
      for (int i = 0; i < maxValues.length; i++) {
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] > 0) {
          CarbonUnsafe.unsafe.copyMemory(maxValues[i], CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
              address + indexSize, maxValues[i].length);
          indexSize += maxValues[i].length;
        }
      }

      // For variable max data
      for (int i = 0; i < maxValues.length; i++) {
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] == -1) {
          CarbonUnsafe.unsafe.putShort(address + indexSize, (short) maxValues[i].length);
          indexSize += 2;
          CarbonUnsafe.unsafe.copyMemory(maxValues[i], CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
              address + indexSize, maxValues[i].length);
          indexSize += maxValues[i].length;
        }
      }

      // min calculation for fixed values
      byte[][] minValues = minMaxIndex.getMinValues();
      for (int i = 0; i < minValues.length; i++) {
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] > 0) {
          CarbonUnsafe.unsafe.copyMemory(minValues[i], CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
              address + indexSize, minValues[i].length);
          indexSize += minValues[i].length;
        }
      }

      // For variable min data
      for (int i = 0; i < minValues.length; i++) {
        if (btreeBuilderInfo.getDimensionColumnValueSize()[i] == -1) {
          CarbonUnsafe.unsafe.putShort(address + indexSize, (short) minValues[i].length);
          indexSize += 2;
          CarbonUnsafe.unsafe.copyMemory(minValues[i], CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
              address + indexSize, minValues[i].length);
          indexSize += minValues[i].length;
        }
      }
    }
    new BlockIndexStore(btreeBuilderInfo, block, rowPointers);
  }

  @Override public DataRefNode get() {
    return null;
  }
}
