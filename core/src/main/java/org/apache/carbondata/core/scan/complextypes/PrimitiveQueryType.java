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

package org.apache.carbondata.core.scan.complextypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.mdkey.Bits;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.TimestampType$;

public class PrimitiveQueryType extends ComplexQueryType implements GenericQueryType {

  private String name;
  private String parentname;

  private int keySize;

  private Dictionary dictionary;

  private org.apache.carbondata.core.metadata.datatype.DataType dataType;

  private boolean isDirectDictionary;

  public PrimitiveQueryType(String name, String parentname, int blockIndex,
      org.apache.carbondata.core.metadata.datatype.DataType dataType, int keySize,
      Dictionary dictionary, boolean isDirectDictionary) {
    super(name, parentname, blockIndex);
    this.dataType = dataType;
    this.keySize = keySize;
    this.dictionary = dictionary;
    this.name = name;
    this.parentname = parentname;
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
      DimensionRawColumnChunk[] rawColumnChunks, int rowNumber,
      int pageNumber, DataOutputStream dataOutputStream) throws IOException {
    byte[] currentVal = copyBlockDataChunk(rawColumnChunks, rowNumber, pageNumber);
    dataOutputStream.write(currentVal);
  }

  @Override public DataType getSchemaType() {
    if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
      return IntegerType$.MODULE$;
    } else if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
      return DoubleType$.MODULE$;
    } else if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
      return LongType$.MODULE$;
    } else if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
      return BooleanType$.MODULE$;
    } else if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
      return TimestampType$.MODULE$;
    } else if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      return DateType$.MODULE$;
    } else {
      return IntegerType$.MODULE$;
    }
  }

  @Override public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
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
