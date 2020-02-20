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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

/**
 * Class to read the Array Stream
 */

public class ArrayStreamReader extends CarbonColumnVectorImpl implements PrestoVectorBlockBuilder {

  protected int batchSize;

  protected Type type = new ArrayType(VarcharType.VARCHAR);

  protected BlockBuilder builder;
  public ArrayStreamReader(int batchSize, DataType dataType) {
    super(batchSize, dataType);
    this.batchSize = batchSize;
    this.builder = type.createBlockBuilder(null, batchSize);
  }

  @Override
  public Block buildBlock() {
    return builder.build();
  }

  Block childBlock = null;

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public void putObject(int rowId, Object value) {
    if (this.childBlock == null) {
      childBlock = ((SliceStreamReader) getChildrenVector()).buildBlock();
      // byte[] val = (byte[]) value;
      // offsetVector = createOffsetVector(val);
      // valueIsNull = createValueIsNull(offsetVector);
      type.writeObject(builder, childBlock);
    } else {
      // Block arrayBlock = ArrayBlock.fromElementBlock(this.batchSize,
      // Optional.ofNullable(valueIsNull), offsetVector, childBlock);
      // type.writeObject(builder, childBlock);
      // arrayBlock.writePositionTo(0, (BlockBuilder) arrayBlock);
      // type.writeObject(builder, StructuralTestUtil().arrayBlockOf());
    }
  }

  @Override
  public void putNull(int rowId) {
    builder.appendNull();
  }

  @Override
  public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }

  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      builder.appendNull();
    }
  }
}

