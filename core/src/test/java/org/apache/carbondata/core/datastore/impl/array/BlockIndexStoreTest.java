package org.apache.carbondata.core.datastore.impl.array;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.util.CarbonUtil;

import junit.framework.TestCase;
import org.junit.Test;

public class BlockIndexStoreTest extends TestCase {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockIndexStoreTest.class.getName());

  private IndexStore indexStore;

  private IndexStore indexStoreNoDic;

  private IndexStore indexStoreOnlyDic;

  private List<byte[]> key;

  private List<byte[][]> min;

  private List<byte[][]> max;

  private List<byte[]> keyNoDic;

  private List<byte[][]> minNoDic;

  private List<byte[][]> maxNoDic;

  private List<byte[]> keyOnlyDic;

  private List<byte[][]> minOnlyDic;

  private List<byte[][]> maxOnlyDic;

  @Override protected void setUp() throws Exception {
    key = new ArrayList<>();
    min = new ArrayList<>();
    max = new ArrayList<>();
    keyNoDic = new ArrayList<>();
    minNoDic = new ArrayList<>();
    maxNoDic = new ArrayList<>();
    keyOnlyDic = new ArrayList<>();
    minOnlyDic = new ArrayList<>();
    maxOnlyDic = new ArrayList<>();
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
    List<DataFileFooter> dataFileFooterList = getDataFileFooterList(dimensionBitLength);
    List<DataFileFooter> fileFooterListWithOnlyDictionaryKey =
        getFileFooterListWithOnlyDictionaryKey(dimensionBitLength);
    List<DataFileFooter> fileFooterListWithOnlyNoDictionaryKey =
        getFileFooterListWithOnlyNoDictionaryKey(dimensionBitLength);
    dimensionBitLength = getSizes(dimensionBitLength);

    BlockArrayIndexBuilder blockArrayIndexBuilder = new BlockArrayIndexBuilder();
    blockArrayIndexBuilder.build(new BTreeBuilderInfo(dataFileFooterList,
        new int[] { dimensionBitLength[0], dimensionBitLength[1], -1 }));
    BlockIndexNodeWrapper dataRefNode = (BlockIndexNodeWrapper) blockArrayIndexBuilder.get();
    indexStore = dataRefNode.getIndexStore();

    BlockArrayIndexBuilder blockArrayIndexBuilder1 = new BlockArrayIndexBuilder();
    blockArrayIndexBuilder1
        .build(new BTreeBuilderInfo(fileFooterListWithOnlyNoDictionaryKey, new int[] { -1 }));
    BlockIndexNodeWrapper dataRefNode1 = (BlockIndexNodeWrapper) blockArrayIndexBuilder1.get();
    indexStoreNoDic = dataRefNode1.getIndexStore();

    BlockArrayIndexBuilder blockArrayIndexBuilder2 = new BlockArrayIndexBuilder();
    blockArrayIndexBuilder2.build(new BTreeBuilderInfo(fileFooterListWithOnlyDictionaryKey,
        new int[] { dimensionBitLength[0], dimensionBitLength[1] }));
    BlockIndexNodeWrapper dataRefNode2 = (BlockIndexNodeWrapper) blockArrayIndexBuilder2.get();
    indexStoreOnlyDic = dataRefNode2.getIndexStore();

  }

  @Test public void testIndexStoreContent() {
    assert (indexStore.getIndexKeyCount() == key.size());
    for (int i = 0; i < indexStore.getIndexKeyCount(); i++) {
      IndexKey indexKey = indexStore.getIndexKey(i);
      ByteBuffer buffer = ByteBuffer.wrap(key.get(i));
      buffer.rewind();
      int dictonaryKeySize = buffer.getInt();
      int nonDictonaryKeySize = buffer.getInt();
      byte[] dictionaryKey = new byte[dictonaryKeySize];
      buffer.get(dictionaryKey);
      byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
      buffer.get(nonDictionaryKey);
      assert (Arrays.equals(dictionaryKey, indexKey.getDictionaryKeys()));
      assert (Arrays.equals(nonDictionaryKey, indexKey.getNoDictionaryKeys()));

      byte[][] mi = min.get(i);
      byte[][] smi = indexStore.getMins(i);
      for (int j = 0; j < mi.length; j++) {
        assert (Arrays.equals(mi[j], smi[j]));
      }

      byte[][] ma = max.get(i);
      byte[][] sma = indexStore.getMaxs(i);
      for (int j = 0; j < ma.length; j++) {
        assert (Arrays.equals(ma[j], sma[j]));
      }
      assert (999 == indexStore.getRowCount(i));
    }
  }

  @Test public void testIndexStoreContentOnlyNoDic() {
    assert (indexStoreNoDic.getIndexKeyCount() == keyNoDic.size());
    for (int i = 0; i < indexStoreNoDic.getIndexKeyCount(); i++) {
      IndexKey indexKey = indexStoreNoDic.getIndexKey(i);
      ByteBuffer buffer = ByteBuffer.wrap(keyNoDic.get(i));
      buffer.rewind();
      int dictonaryKeySize = buffer.getInt();
      int nonDictonaryKeySize = buffer.getInt();
      byte[] dictionaryKey = new byte[dictonaryKeySize];
      buffer.get(dictionaryKey);
      byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
      buffer.get(nonDictionaryKey);
      assert (Arrays.equals(dictionaryKey, indexKey.getDictionaryKeys()));
      assert (Arrays.equals(nonDictionaryKey, indexKey.getNoDictionaryKeys()));

      byte[][] mi = minNoDic.get(i);
      byte[][] smi = indexStoreNoDic.getMins(i);
      for (int j = 0; j < mi.length; j++) {
        assert (Arrays.equals(mi[j], smi[j]));
      }

      byte[][] ma = maxNoDic.get(i);
      byte[][] sma = indexStoreNoDic.getMaxs(i);
      for (int j = 0; j < ma.length; j++) {
        assert (Arrays.equals(ma[j], sma[j]));
      }
    }
  }

  @Test public void testIndexStoreContentWithOnlyDic() {
    assert (indexStoreOnlyDic.getIndexKeyCount() == keyOnlyDic.size());
    for (int i = 0; i < indexStoreOnlyDic.getIndexKeyCount(); i++) {
      IndexKey indexKey = indexStoreOnlyDic.getIndexKey(i);
      ByteBuffer buffer = ByteBuffer.wrap(keyOnlyDic.get(i));
      buffer.rewind();
      int dictonaryKeySize = buffer.getInt();
      int nonDictonaryKeySize = buffer.getInt();
      byte[] dictionaryKey = new byte[dictonaryKeySize];
      buffer.get(dictionaryKey);
      byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
      buffer.get(nonDictionaryKey);
      assert (Arrays.equals(dictionaryKey, indexKey.getDictionaryKeys()));
      assert (Arrays.equals(nonDictionaryKey, indexKey.getNoDictionaryKeys()));

      byte[][] mi = minOnlyDic.get(i);
      byte[][] smi = indexStoreOnlyDic.getMins(i);
      for (int j = 0; j < mi.length; j++) {
        assert (Arrays.equals(mi[j], smi[j]));
      }

      byte[][] ma = maxOnlyDic.get(i);
      byte[][] sma = indexStoreOnlyDic.getMaxs(i);
      for (int j = 0; j < ma.length; j++) {
        assert (Arrays.equals(ma[j], sma[j]));
      }
    }
  }

  private List<DataFileFooter> getDataFileFooterList(int[] dimensionBitLength) {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
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
        byte[] noDictionaryEndKey = buffer.array();
        DataFileFooter footer =
            getFileFooter(startKey, endKey, noDictionaryStartKey, noDictionaryEndKey,
                dimensionBitLength);
        key.add(footer.getBlockletIndex().getBtreeIndex().getStartKey());
        min.add(footer.getBlockletIndex().getMinMaxIndex().getMinValues());
        max.add(footer.getBlockletIndex().getMinMaxIndex().getMaxValues());

        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private DataFileFooter getFileFooter(byte[] startKey, byte[] endKey, byte[] noDictionaryStartKey,
      byte[] noDictionaryEndKey, int[] dimensionBitLength) {
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
    byte[][] end = splitData(endKey, dimensionBitLength);
    byte[][] start = splitData(startKey, dimensionBitLength);
    minMax.setMaxValues(new byte[][] { end[0], end[1], noDictionaryEndKey });
    minMax.setMinValues(new byte[][] { start[0], start[1], noDictionaryStartKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    footer.setNumberOfRows(999);
    return footer;
  }

  private byte[][] splitData(byte[] data, int[] dimensionBitLength) {
    byte[][] output = new byte[dimensionBitLength.length][];
    int k = 0;
    for (int i = 0; i < dimensionBitLength.length; i++) {
      int i1 = dimensionBitLength[i] / 8;
      if (dimensionBitLength[i] % 8 != 0) {
        i1++;
      }

      byte[] s = new byte[i1];
      System.arraycopy(data, k, s, 0, s.length);
      k += s.length;
      output[i] = s;
    }
    return output;
  }

  private int[] getSizes(int[] dimensionBitLength) {
    int[] out = new int[dimensionBitLength.length];
    for (int i = 0; i < dimensionBitLength.length; i++) {
      int i1 = dimensionBitLength[i] / 8;
      if (dimensionBitLength[i] % 8 != 0) {
        i1++;
      }

      out[i] = i1;
    }
    return out;
  }

  private List<DataFileFooter> getFileFooterListWithOnlyNoDictionaryKey(int[] dimensionBitLength) {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
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
        byte[] noDictionaryEndKey = buffer.array();
        DataFileFooter footer =
            getFileMatadataWithOnlyNoDictionaryKey(startKey, endKey, noDictionaryStartKey,
                noDictionaryEndKey, dimensionBitLength);
        keyNoDic.add(footer.getBlockletIndex().getBtreeIndex().getStartKey());
        minNoDic.add(footer.getBlockletIndex().getMinMaxIndex().getMinValues());
        maxNoDic.add(footer.getBlockletIndex().getMinMaxIndex().getMaxValues());
        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private List<DataFileFooter> getFileFooterListWithOnlyDictionaryKey(int[] dimensionBitLength) {
    List<DataFileFooter> list = new ArrayList<DataFileFooter>();
    try {
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
        byte[] noDictionaryEndKey = buffer.array();
        DataFileFooter footer =
            getFileFooterWithOnlyDictionaryKey(startKey, endKey, noDictionaryStartKey,
                noDictionaryEndKey, dimensionBitLength);
        keyOnlyDic.add(footer.getBlockletIndex().getBtreeIndex().getStartKey());
        minOnlyDic.add(footer.getBlockletIndex().getMinMaxIndex().getMinValues());
        maxOnlyDic.add(footer.getBlockletIndex().getMinMaxIndex().getMaxValues());
        list.add(footer);
        i = i + 10;
      }
    } catch (Exception e) {
      LOGGER.error(e);
    }
    return list;
  }

  private DataFileFooter getFileMatadataWithOnlyNoDictionaryKey(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey, int[] dimensionBitLength) {
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
    minMax.setMaxValues(new byte[][] { noDictionaryEndKey });
    minMax.setMinValues(new byte[][] { noDictionaryStartKey });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    return footer;
  }

  private DataFileFooter getFileFooterWithOnlyDictionaryKey(byte[] startKey, byte[] endKey,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey, int[] dimensionBitLength) {
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
    byte[][] end = splitData(endKey, dimensionBitLength);
    byte[][] start = splitData(startKey, dimensionBitLength);
    minMax.setMaxValues(new byte[][] { end[0], end[1] });
    minMax.setMinValues(new byte[][] { start[0], start[1] });
    index.setBtreeIndex(btreeIndex);
    index.setMinMaxIndex(minMax);
    footer.setBlockletIndex(index);
    return footer;
  }

  @Test public void testBtreeSearchIsWorkingAndGivingPorperBlocklet() {
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(new int[] { 10000, 10000 }, new int[] { 1, 1 });
    KeyGenerator multiDimKeyVarLengthGenerator =
        new MultiDimKeyVarLengthGenerator(dimensionBitLength);
    DataRefNode dataBlock = new BlockIndexNodeWrapper(indexStore, (short) 0);
    DataRefNodeFinder finder = new ArrayDataRefNodeFinder(new int[] { 2, 2, -1 }, 3, 1);
    byte[] startKey = new byte[0];
    try {
      startKey = multiDimKeyVarLengthGenerator.generateKey(new int[] { 21, 21 });
    } catch (KeyGenException e) {
      e.printStackTrace();
    }
    ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
    buffer.rewind();
    buffer.put((byte) 1);
    buffer.putInt(21);
    buffer.array();
    byte[] noDictionaryStartKey = buffer.array();

    IndexKey key = new IndexKey(startKey, noDictionaryStartKey);
    DataRefNode findFirstBlock = finder.findFirstDataBlock(dataBlock, key);
    assertEquals(2, findFirstBlock.nodeNumber());
    DataRefNode findLastBlock = finder.findLastDataBlock(dataBlock, key);
    assertEquals(2, findLastBlock.nodeNumber());
  }

  @Override public void tearDown() throws Exception {
    indexStore.clear();
    indexStoreNoDic.clear();
    indexStoreOnlyDic.clear();
  }
}
