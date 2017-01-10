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

package org.apache.carbondata.scan.complextypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.mdkey.Bits;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import org.apache.spark.sql.types.*;

public class PrimitiveQueryType extends ComplexQueryType implements GenericQueryType {

  private String name;
  private String parentname;

  private int keySize;

  private int blockIndex;

  private Dictionary dictionary;

  private org.apache.carbondata.core.carbon.metadata.datatype.DataType dataType;

  private boolean isDirectDictionary;

  public PrimitiveQueryType(String name, String parentname, int blockIndex,
      org.apache.carbondata.core.carbon.metadata.datatype.DataType dataType, int keySize,
      Dictionary dictionary, boolean isDirectDictionary) {
    super(name, parentname, blockIndex);
    this.dataType = dataType;
    this.keySize = keySize;
    this.dictionary = dictionary;
    this.name = name;
    this.parentname = parentname;
    this.blockIndex = blockIndex;
    this.isDirectDictionary = isDirectDictionary;
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

  @Override public int getColsCount() {
    return 1;
  }

  @Override public void parseBlocksAndReturnComplexColumnByteArray(
      DimensionColumnDataChunk[] dimensionDataChunks, int rowNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] currentVal =
        new byte[dimensionDataChunks[blockIndex].getColumnValueSize()];
    copyBlockDataChunk(dimensionDataChunks, rowNumber, currentVal);
    dataOutputStream.write(currentVal);
  }

  @Override public DataType getSchemaType() {
    switch (dataType) {
      case INT:
        return IntegerType$.MODULE$;
      case DOUBLE:
        return DoubleType$.MODULE$;
      case LONG:
        return LongType$.MODULE$;
      case BOOLEAN:
        return BooleanType$.MODULE$;
      case TIMESTAMP:
        return TimestampType$.MODULE$;
      case DATE:
        return DateType$.MODULE$;
      default:
        return IntegerType$.MODULE$;
    }
  }

  @Override public void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder)
      throws IOException {
    readBlockDataChunk(blockChunkHolder);
  }

  @Override public Object getDataBasedOnDataTypeFromSurrogates(ByteBuffer surrogateData) {
    byte[] data = new byte[keySize];
    surrogateData.get(data);
    Bits bit = new Bits(new int[]{keySize * 8});
    int surrgateValue = (int)bit.getKeyArray(data, 0)[0];
    Object actualData = null;
    if (isDirectDictionary) {
      DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(dataType);
      actualData = directDictionaryGenerator.getValueFromSurrogate(surrgateValue);
    } else {
      String dictionaryValueForKey = dictionary.getDictionaryValueForKey(surrgateValue);
      actualData = DataTypeUtil.getDataBasedOnDataType(dictionaryValueForKey, this.dataType);
    }
    return actualData;
  }
}
