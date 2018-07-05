package org.apache.carbondata.core.localdictionary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.ColumnLocalDictionaryGenerator;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.LocalDictionaryChunk;

import org.junit.Assert;
import org.junit.Test;

public class TestPageLevelDictionary {

  @Test public void testPageLevelDictionaryGenerateDataIsGenertingProperDictionaryValues() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary = new PageLevelDictionary(generator, columnName,
        DataTypes.STRING, false);
    try {
      for (int i = 1; i <= 1000; i++) {
        Assert.assertTrue((i + 1) == pageLevelDictionary.getDictionaryValue(("" + i).getBytes()));
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
  }

  @Test public void testPageLevelDictionaryContainsOnlyUsedDictionaryValues() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 = new PageLevelDictionary(generator, columnName, DataTypes.STRING, false);
    byte[][] validateData = new byte[500][];
    try {
      for (int i = 1; i <= 500; i++) {
        byte[] data = ("vishal" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        validateData[i - 1] = data;
        pageLevelDictionary1.getDictionaryValue(byteBuffer.array());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 = new PageLevelDictionary(generator, columnName, DataTypes.STRING, false);
    try {
      for (int i = 1; i <= 500; i++) {
        byte[] data = ("vikas" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        pageLevelDictionary2.getDictionaryValue(byteBuffer.array());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      LocalDictionaryChunk localDictionaryChunkForBlocklet =
          pageLevelDictionary1.getLocalDictionaryChunkForBlocklet();
      List<Encoding> encodings = localDictionaryChunkForBlocklet.getDictionary_meta().getEncoders();
      EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();
      List<ByteBuffer> encoderMetas =
          localDictionaryChunkForBlocklet.getDictionary_meta().getEncoder_meta();
      ColumnPageDecoder decoder = encodingFactory.createDecoder(encodings, encoderMetas);
      ColumnPage decode = decoder.decode(localDictionaryChunkForBlocklet.getDictionary_data(), 0,
          localDictionaryChunkForBlocklet.getDictionary_data().length);
      for (int i = 0; i < 500; i++) {
        Arrays.equals(decode.getBytes(i), validateData[i]);
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testPageLevelDictionaryContainsOnlyUsedDictionaryValuesWhenMultiplePagesUseSameDictionary() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 = new PageLevelDictionary(generator, columnName, DataTypes.STRING, false);
    byte[][] validateData = new byte[10][];
    int index = 0;
    try {
      for (int i = 1; i <= 5; i++) {
        byte[] data = ("vishal" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        validateData[index] = data;
        pageLevelDictionary1.getDictionaryValue(byteBuffer.array());
        index++;
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 = new PageLevelDictionary(generator, columnName, DataTypes.STRING, false);
    try {
      for (int i = 1; i <= 5; i++) {
        byte[] data = ("vikas" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        pageLevelDictionary2.getDictionaryValue(byteBuffer.array());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      for (int i = 6; i <= 10; i++) {
        byte[] data = ("vishal" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        validateData[index] = data;
        pageLevelDictionary1.getDictionaryValue(byteBuffer.array());
        index++;
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      for (int i = 6; i <= 10; i++) {
        byte[] data = ("vikas" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + 2);
        byteBuffer.putShort((short)data.length);
        byteBuffer.put(data);
        pageLevelDictionary2.getDictionaryValue(byteBuffer.array());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    try {
      LocalDictionaryChunk localDictionaryChunkForBlocklet =
          pageLevelDictionary1.getLocalDictionaryChunkForBlocklet();
      List<Encoding> encodings = localDictionaryChunkForBlocklet.getDictionary_meta().getEncoders();
      EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();
      List<ByteBuffer> encoderMetas =
          localDictionaryChunkForBlocklet.getDictionary_meta().getEncoder_meta();
      ColumnPageDecoder decoder = encodingFactory.createDecoder(encodings, encoderMetas);
      ColumnPage decode = decoder.decode(localDictionaryChunkForBlocklet.getDictionary_data(), 0,
          localDictionaryChunkForBlocklet.getDictionary_data().length);
      BitSet bitSet = BitSet.valueOf(CompressorFactory.getInstance().getCompressor()
          .unCompressByte(localDictionaryChunkForBlocklet.getDictionary_values()));
      Assert.assertTrue(bitSet.cardinality()==validateData.length);
      for(int i =0; i<validateData.length;i++) {
        Assert.assertTrue(Arrays.equals(decode.getBytes(i), validateData[i]));
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }
}
