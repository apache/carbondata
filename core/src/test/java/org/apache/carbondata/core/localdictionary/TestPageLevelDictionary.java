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
package org.apache.carbondata.core.localdictionary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.Compressor;
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
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.LocalDictionaryChunk;

import org.junit.Assert;
import org.junit.Test;

public class TestPageLevelDictionary {
  private String compressorName = CompressorFactory.getInstance().getCompressor(
      CarbonCommonConstants.DEFAULT_COMPRESSOR).getName();

  @Test public void testPageLevelDictionaryGenerateDataIsGenertingProperDictionaryValues() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary =
        new PageLevelDictionary(generator, columnName, compressorName);
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
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 =
        new PageLevelDictionary(generator, columnName, compressorName);
    byte[][] validateData = new byte[500][];
    try {
      for (int i = 1; i <= 500; i++) {
        byte[] data = ("vishal" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
        byteBuffer.put(data);
        validateData[i - 1] = data;
        pageLevelDictionary1.getDictionaryValue(byteBuffer.array());
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 =
        new PageLevelDictionary(generator, columnName, compressorName);
    try {
      for (int i = 1; i <= 500; i++) {
        byte[] data = ("vikas" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
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
      BitSet bitSet = BitSet.valueOf(CompressorFactory.getInstance().getCompressor(compressorName)
          .unCompressByte(localDictionaryChunkForBlocklet.getDictionary_values()));
      Assert.assertTrue(bitSet.cardinality()==validateData.length);
      Compressor compressor = CompressorFactory.getInstance().getCompressor(compressorName);
      CarbonDictionary dictionary = DimensionRawColumnChunk
          .getDictionary(pageLevelDictionary1.getLocalDictionaryChunkForBlocklet(), compressor);
      byte[][] dictionaryValues = new byte[bitSet.cardinality()][];
      int index = 0;
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
        dictionaryValues[index++] = dictionary.getDictionaryValue(i);
      }
      for(int i =0; i<validateData.length;i++) {
        Assert.assertTrue(Arrays.equals(dictionaryValues[i], validateData[i]));
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testPageLevelDictionaryContainsOnlyUsedDictionaryValuesWhenMultiplePagesUseSameDictionary() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000);
    String columnName = "column1";
    PageLevelDictionary pageLevelDictionary1 =
        new PageLevelDictionary(generator, columnName, compressorName);
    byte[][] validateData = new byte[10][];
    int index = 0;
    try {
      for (int i = 1; i <= 5; i++) {
        byte[] data = ("vishal" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
        byteBuffer.put(data);
        validateData[index] = data;
        pageLevelDictionary1.getDictionaryValue(byteBuffer.array());
        index++;
      }
      Assert.assertTrue(true);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(false);
    }
    PageLevelDictionary pageLevelDictionary2 =
        new PageLevelDictionary(generator, columnName, compressorName);
    try {
      for (int i = 1; i <= 5; i++) {
        byte[] data = ("vikas" + i).getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
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
      BitSet bitSet = BitSet.valueOf(CompressorFactory.getInstance().getCompressor(compressorName)
          .unCompressByte(localDictionaryChunkForBlocklet.getDictionary_values()));
      Assert.assertTrue(bitSet.cardinality()==validateData.length);
      Compressor compressor = CompressorFactory.getInstance().getCompressor(compressorName);
      CarbonDictionary dictionary = DimensionRawColumnChunk
          .getDictionary(pageLevelDictionary1.getLocalDictionaryChunkForBlocklet(), compressor);
      byte[][] dictionaryValues = new byte[bitSet.cardinality()][];
      index = 0;
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
        dictionaryValues[index++] = dictionary.getDictionaryValue(i);
      }
      for(int i =0; i<validateData.length;i++) {
        Assert.assertTrue(Arrays.equals(dictionaryValues[i], validateData[i]));
      }
    } catch (MemoryException e) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
  }
}
