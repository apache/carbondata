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

package org.apache.carbondata.presto.readers;

import java.util.Optional;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slices;

import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * This class reads the String data and convert it into Slice Block
 */
public class SliceStreamReader extends CarbonColumnVectorImpl implements PrestoVectorBlockBuilder {

  protected int batchSize;

  protected Type type = VarcharType.VARCHAR;

  protected BlockBuilder builder;

  int[] values;

  private Block dictionaryBlock;

  public SliceStreamReader(int batchSize, DataType dataType,
      Block dictionaryBlock) {
    super(batchSize, dataType);
    this.batchSize = batchSize;
    if (dictionaryBlock == null) {
      this.builder = type.createBlockBuilder(null, batchSize);
    } else {
      this.dictionaryBlock = dictionaryBlock;
      this.values = new int[batchSize];
    }
  }

  @Override public Block buildBlock() {
    if (dictionaryBlock == null) {
      return builder.build();
    } else {
      return new DictionaryBlock(batchSize, dictionaryBlock, values);
    }
  }

  @Override public void setDictionary(CarbonDictionary dictionary) {
    super.setDictionary(dictionary);
    if (dictionary == null) {
      dictionaryBlock = null;
      return;
    }
    boolean[] nulls = new boolean[dictionary.getDictionarySize()];
    nulls[0] = true;
    nulls[1] = true;
    int[] dictOffsets = new int[dictionary.getDictionarySize() + 1];
    int size = 0;
    for (int i = 0; i < dictionary.getDictionarySize(); i++) {
      dictOffsets[i] = size;
      if (dictionary.getDictionaryValue(i) != null) {
        size += dictionary.getDictionaryValue(i).length;
      }
    }
    byte[] singleArrayDictValues = new byte[size];
    for (int i = 0; i < dictionary.getDictionarySize(); i++) {
      if (dictionary.getDictionaryValue(i) != null) {
        System.arraycopy(dictionary.getDictionaryValue(i), 0, singleArrayDictValues, dictOffsets[i],
            dictionary.getDictionaryValue(i).length);
      }
    }
    dictOffsets[dictOffsets.length - 1] = size;
    dictionaryBlock = new VariableWidthBlock(dictionary.getDictionarySize(),
        Slices.wrappedBuffer(singleArrayDictValues), dictOffsets, Optional.of(nulls));
    values = (int[]) ((CarbonColumnVectorImpl) getDictionaryVector()).getDataArray();
  }

  @Override public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override public void putInt(int rowId, int value) {
    values[rowId] = value;
  }

  @Override public void putByteArray(int rowId, byte[] value) {
    type.writeSlice(builder, wrappedBuffer(value));
  }

  @Override public void putByteArray(int rowId, int offset, int length, byte[] value) {
    byte[] byteArr = new byte[length];
    System.arraycopy(value, offset, byteArr, 0, length);
    type.writeSlice(builder, wrappedBuffer(byteArr));
  }

  @Override public void putNull(int rowId) {
    if (dictionaryBlock == null) {
      builder.appendNull();
    }
  }

  @Override public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }
}
