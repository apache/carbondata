/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.complex.querytypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.carbon.util.DataTypeUtil;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

public class PrimitiveQueryType extends ComplexQueryType implements GenericQueryType {

  private int index;

  private String name;
  private String parentname;

  private int keySize;

  private int blockIndex;

  private Dictionary dictionary;

  private org.carbondata.core.carbon.metadata.datatype.DataType dataType;

  public PrimitiveQueryType(String name, String parentname, int blockIndex,
      org.carbondata.core.carbon.metadata.datatype.DataType dataType, int keySize,
      Dictionary dictionary) {
    super(name, parentname, blockIndex);
    this.dataType = dataType;
    this.keySize = keySize;
    this.dictionary = dictionary;
    this.name = name;
    this.parentname = parentname;
    this.blockIndex = blockIndex;
  }

  @Override public void addChildren(GenericQueryType children) {

  }

  @Override public String getName() {
    return name;
  }

  @Override public void setName(String name) {
    this.name = name;
  }

  @Override public String getParentname() {
    return parentname;
  }

  @Override public void setParentname(String parentname) {
    this.parentname = parentname;

  }

  @Override public void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild) {

  }

  @Override public int getSurrogateIndex() {
    return index;
  }

  @Override public void setSurrogateIndex(int surrIndex) {
    index = surrIndex;
  }

  @Override public int getBlockIndex() {
    return blockIndex;
  }

  @Override public void setBlockIndex(int blockIndex) {
    this.blockIndex = blockIndex;
  }

  @Override public int getColsCount() {
    return 1;
  }

  @Override public void parseBlocksAndReturnComplexColumnByteArray(
      DimensionColumnDataChunk[] dimensionDataChunks, int rowNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] currentVal =
        new byte[dimensionDataChunks[blockIndex].getAttributes().getColumnValueSize()];
    copyBlockDataChunk(dimensionDataChunks, rowNumber, currentVal);
    dataOutputStream.write(currentVal);
  }

  @Override public void setKeySize(int[] keyBlockSize) {
    this.keySize = keyBlockSize[this.blockIndex];
  }

  @Override public void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput)
      throws IOException {
  }

  @Override public DataType getSchemaType() {
    switch (dataType) {
      case INT:
        return new IntegerType();
      case DOUBLE:
        return new DoubleType();
      case LONG:
        return new LongType();
      case BOOLEAN:
        return new BooleanType();
      case TIMESTAMP:
        return new TimestampType();
      default:
        return new IntegerType();
    }
  }

  @Override public int getKeyOrdinalForQuery() {
    return 0;
  }

  @Override public void setKeyOrdinalForQuery(int keyOrdinalForQuery) {
  }

  @Override public void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder) {
    readBlockDataChunk(blockChunkHolder);
  }

  @Override public Object getDataBasedOnDataTypeFromSurrogates(ByteBuffer surrogateData) {

    byte[] data = new byte[keySize];
    surrogateData.get(data);
    String dictionaryValueForKey =
        dictionary.getDictionaryValueForKey(unsignedIntFromByteArray(data));
    Object actualData = DataTypeUtil.getDataBasedOnDataType(dictionaryValueForKey, this.dataType);
    if (null != actualData
        && this.dataType == org.carbondata.core.carbon.metadata.datatype.DataType.STRING) {
      byte[] dataBytes = ((String) actualData).getBytes(Charset.defaultCharset());
      return UTF8String.fromBytes(dataBytes);
    }
    return actualData;
  }

  private int unsignedIntFromByteArray(byte[] bytes) {
    int res = 0;
    if (bytes == null) return res;

    for (int i = 0; i < bytes.length; i++) {
      res = (res * 10) + ((bytes[i] & 0xff));
    }
    return res;
  }
}
