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

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.DataTypeUtil;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;

/**
 * Class for Reading the Double value and setting it in Block
 */
public class DoubleStreamReader extends CarbonColumnVectorImpl implements PrestoVectorBlockBuilder {

  protected int batchSize;

  protected Type type = DoubleType.DOUBLE;

  protected BlockBuilder builder;

  private Dictionary dictionary;

  public DoubleStreamReader(int batchSize, DataType dataType, Dictionary dictionary) {
    super(batchSize, dataType);
    this.batchSize = batchSize;
    this.builder = type.createBlockBuilder(null, batchSize);
    this.dictionary = dictionary;
  }

  @Override public Block buildBlock() {
    return builder.build();
  }

  @Override public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override public void putInt(int rowId, int value) {
    Object data = DataTypeUtil
        .getDataBasedOnDataType(dictionary.getDictionaryValueForKey(value), DataTypes.DOUBLE);
    if (data != null) {
      type.writeDouble(builder, (Double) data);
    } else {
      builder.appendNull();
    }
  }

  @Override public void putDouble(int rowId, double value) {
    type.writeDouble(builder, value);
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    for (int i = 0; i < count; i++) {
      type.writeDouble(builder, value);
    }
  }

  @Override public void putNull(int rowId) {
    builder.appendNull();
  }

  @Override public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      builder.appendNull();
    }
  }

  @Override public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }

  @Override public void putObject(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
    } else {
      if (dictionary == null) {
        putDouble(rowId, (double) value);
      } else {
        putInt(rowId, (int) value);
      }
    }
  }
}
