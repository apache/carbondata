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
package org.apache.carbondata.core.datastore.impl.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.util.CarbonUtil;

import junit.framework.TestCase;
import org.junit.Test;

public class BTreeBlockFinderTest extends TestCase {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BTreeBlockFinderTest.class.getName());

  @Test public void testBtreeBuldingIsPorper() {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);

  }

  @Test public void testBtreeBuilderGetMethodIsGivingNotNullRootNode() {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getDataFileFooterList();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
  }

  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlockletWithNoDictionary1() {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getFileFooterListWithOnlyNoDictionaryKey();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
    DataRefNodeFinder finder = new BTreeDataRefNodeFinder(new int[] { -1 }, 1, 1);
    ByteBuffer buffer = ByteBuffer.allocate(4 + 2);
    buffer.rewind();
    buffer.putShort((short) 1);
    buffer.putInt(12);
    buffer.array();
    IndexKey key = new IndexKey(null, buffer.array());
    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(1, findFirstBlock.nodeIndex());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(1, findLastBlock.nodeIndex());
  }

  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlockletWithNoDictionary() {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getFileFooterListWithOnlyNoDictionaryKey();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
    DataRefNodeFinder finder = new BTreeDataRefNodeFinder(new int[] { -1 }, 1, 1);
    ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
    buffer.rewind();
    buffer.put((byte) 1);
    buffer.putInt(0);
    buffer.array();
    IndexKey key = new IndexKey(null, buffer.array());
    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(0, findFirstBlock.nodeIndex());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(0, findLastBlock.nodeIndex());
  }

  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlockletWithDictionaryKey1()
      throws KeyGenException {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getFileFooterListWithOnlyDictionaryKey();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
    DataRefNodeFinder finder = new BTreeDataRefNodeFinder(new int[] { 2, 2 }, 2, 0);
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
    KeyGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(dimensionBitLength);

    IndexKey key =
        new IndexKey(multiDimKeyVarLengthGenerator.generateKey(new int[] { 1, 1 }), null);
    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(0, findFirstBlock.nodeIndex());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(0, findLastBlock.nodeIndex());
  }

  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlockletWithDictionaryKey2()
      throws KeyGenException {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getFileFooterListWithOnlyDictionaryKey();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
    DataRefNodeFinder finder = new BTreeDataRefNodeFinder(new int[] { 2, 2 }, 2, 0);
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
    KeyGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(dimensionBitLength);

    IndexKey key =
        new IndexKey(multiDimKeyVarLengthGenerator.generateKey(new int[] { 0, 0 }), null);

    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(0, findFirstBlock.nodeIndex());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(0, findLastBlock.nodeIndex());
  }

  /**
   * Below method will test when key which is not present and key which is
   * more than
   * last node key is passes for searching it should give first block
   */
  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlockletWithDictionaryKey()
      throws KeyGenException {
    BtreeBuilder builder = new BlockBTreeBuilder();
    List<DataFileFooter> footerList = getFileFooterListWithOnlyDictionaryKey();
    BTreeBuilderInfo infos = new BTreeBuilderInfo(footerList, null);
    builder.build(infos);
    DataRefNode dataBlock = builder.get();
    assertTrue(dataBlock != null);
    DataRefNodeFinder finder = new BTreeDataRefNodeFinder(new int[] { 2, 2 }, 2, 0);
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
    KeyGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(dimensionBitLength);

    IndexKey key =
        new IndexKey(multiDimKeyVarLengthGenerator.generateKey(new int[] { 10001, 10001 }), null);

    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(99, findFirstBlock.nodeIndex());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(99, findLastBlock.nodeIndex());
  }

  private List<DataFileFooter> getDataFileFooterList() {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
      KeyGenerator multiDimKeyVarLengthGenerator =
          new MultiDimKeyVarLengthGenerator(dimensionBitLength);
      int i = 1;
      while (i < 1001) {
        byte[] startKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i, i });
        byte[] endKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i + 10, i + 10 });
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
        buffer.rewind();
        buffer.put((byte) 1);
        buffer.putInt(i);
        buffer.array();
        byte[] noDictionaryStartKey = buffer.array();

        ByteBuffer buffer1 = ByteBuffer.allocate(4 + 1);
        buffer1.rewind();
        buffer1.put((byte) 1);
        buffer1.putInt(i + 10);
        buffer1.array();
        byte[] noDictionaryEndKey = buffer1.array();
        DataFileFooter footer =
            getFileFooter(startKey, endKey, noDictionaryStartKey, noDictionaryEndKey);
        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private List<DataFileFooter> getFileFooterListWithOnlyNoDictionaryKey() {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
      KeyGenerator multiDimKeyVarLengthGenerator =
          new MultiDimKeyVarLengthGenerator(dimensionBitLength);
      int i = 1;
      while (i < 1001) {
        byte[] startKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i, i });
        byte[] endKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i + 10, i + 10 });
        ByteBuffer buffer = ByteBuffer.allocate(2 + 4);
        buffer.rewind();
        buffer.putShort((short) 1);
        buffer.putInt(i);
        buffer.array();
        byte[] noDictionaryStartKey = buffer.array();

        ByteBuffer buffer1 = ByteBuffer.allocate(2 + 4);
        buffer1.rewind();
        buffer1.putShort((short) 2);
        buffer1.putInt(i + 10);
        buffer1.array();
        byte[] noDictionaryEndKey = buffer1.array();
        DataFileFooter footer =
            getFileMatadataWithOnlyNoDictionaryKey(startKey, endKey, noDictionaryStartKey,
                noDictionaryEndKey);
        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private List<DataFileFooter> getFileFooterListWithOnlyDictionaryKey() {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
      int[] dimensionBitLength =
          CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
      KeyGenerator multiDimKeyVarLengthGenerator =
          new MultiDimKeyVarLengthGenerator(dimensionBitLength);
      int i = 1;
      while (i < 1001) {
        byte[] startKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i, i });
        byte[] endKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { i + 10, i + 10 });
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4);
        buffer.rewind();
        buffer.put((byte) 1);
        buffer.putInt(i);
        buffer.array();
        byte[] noDictionaryStartKey = buffer.array();

        ByteBuffer buffer1 = ByteBuffer.allocate(1 + 4);
        buffer1.rewind();
        buffer1.put((byte) 1);
        buffer1.putInt(i + 10);
        buffer1.array();
        byte[] noDictionaryEndKey = buffer1.array();
        DataFileFooter footer =
            getFileFooterWithOnlyDictionaryKey(startKey, endKey, noDictionaryStartKey,
                noDictionaryEndKey);
        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private DataFileFooter getFileFooter(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
    DataFileFooter footer = new DataFileFooter();
    BlockletIndex index = new BlockletIndex();
    BlockletBTreeIndex btreeIndex = new BlockletBTreeIndex();
    ByteBuffer buffer = ByteBuffer.allocate(4 + startKey.length + 4 + noDictionaryStartKey.length);
    buffer.putInt(startKey.length);
    buffer.putInt(noDictionaryStartKey.length);
    buffer.put(startKey);
    buffer.put(noDictionaryStartKey);
    buffer.rewind();
    btreeIndex.setStartKey(buffer.array());
    ByteBuffer buffer1 = ByteBuffer.allocate(4 + startKey.length + 4 + noDictionaryEndKey.length);
    buffer1.putInt(endKey.length);
    buffer1.putInt(noDictionaryEndKey.length);
    buffer1.put(endKey);
    buffer1.put(noDictionaryEndKey);
    buffer1.rewind();
    btreeIndex.setEndKey(buffer1.array());
    BlockletMinMaxIndex minMax = new BlockletMinMaxIndex();
    minMax.setMaxValues(new byte[][] { endKey, noDictionaryEndKey });
    minMax.setMinValues(new byte[][] { startKey, noDictionaryStartKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    return footer;
  }

  private DataFileFooter getFileMatadataWithOnlyNoDictionaryKey(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
    DataFileFooter footer = new DataFileFooter();
    BlockletIndex index = new BlockletIndex();
    BlockletBTreeIndex btreeIndex = new BlockletBTreeIndex();
    ByteBuffer buffer = ByteBuffer.allocate(4 + 0 + 4 + noDictionaryStartKey.length);
    buffer.putInt(0);
    buffer.putInt(noDictionaryStartKey.length);
    buffer.put(noDictionaryStartKey);
    buffer.rewind();
    btreeIndex.setStartKey(buffer.array());
    ByteBuffer buffer1 = ByteBuffer.allocate(4 + 0 + 4 + noDictionaryEndKey.length);
    buffer1.putInt(0);
    buffer1.putInt(noDictionaryEndKey.length);
    buffer1.put(noDictionaryEndKey);
    buffer1.rewind();
    btreeIndex.setEndKey(buffer1.array());
    BlockletMinMaxIndex minMax = new BlockletMinMaxIndex();
    minMax.setMaxValues(new byte[][] { endKey, noDictionaryEndKey });
    minMax.setMinValues(new byte[][] { startKey, noDictionaryStartKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    return footer;
  }

  private DataFileFooter getFileFooterWithOnlyDictionaryKey(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
    DataFileFooter footer = new DataFileFooter();
    BlockletIndex index = new BlockletIndex();
    BlockletBTreeIndex btreeIndex = new BlockletBTreeIndex();
    ByteBuffer buffer = ByteBuffer.allocate(4 + startKey.length + 4 + 0);
    buffer.putInt(startKey.length);
    buffer.putInt(0);
    buffer.put(startKey);
    buffer.rewind();
    btreeIndex.setStartKey(buffer.array());
    ByteBuffer buffer1 = ByteBuffer.allocate(4 + 0 + 4 + noDictionaryEndKey.length);
    buffer1.putInt(endKey.length);
    buffer1.putInt(0);
    buffer1.put(endKey);
    buffer1.rewind();
    btreeIndex.setEndKey(buffer1.array());
    BlockletMinMaxIndex minMax = new BlockletMinMaxIndex();
    minMax.setMaxValues(new byte[][] { endKey });
    minMax.setMinValues(new byte[][] { startKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    return footer;
  }

}
