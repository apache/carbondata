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
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.mdkey.Bits;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

public class PrimitiveQueryType extends ComplexQueryType implements GenericQueryType {

  private String name;
  private String parentName;

  private int keySize;

  private Dictionary dictionary;

  private org.apache.carbondata.core.metadata.datatype.DataType dataType;

  private boolean isDirectDictionary;

  private boolean isDictionary;

  private DirectDictionaryGenerator directDictGenForDate;

  public PrimitiveQueryType(String name, String parentName, int blockIndex,
      DataType dataType, int keySize,
      Dictionary dictionary, boolean isDirectDictionary) {
    super(name, parentName, blockIndex);
    this.dataType = dataType;
    this.keySize = keySize;
    this.dictionary = dictionary;
    this.name = name;
    this.parentName = parentName;
    this.isDirectDictionary = isDirectDictionary;
    this.isDictionary = (dictionary != null && !isDirectDictionary);
    this.directDictGenForDate =
        DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(DataTypes.DATE);
  }

  @Override public void addChildren(GenericQueryType children) {

  }

  @Override public String getName() {
    return name;
  }

  @Override public void setName(String name) {
    this.name = name;
  }

  @Override public String getParentName() {
    return parentName;
  }

  @Override public void setParentName(String parentName) {
    this.parentName = parentName;

  }

  @Override public int getColsCount() {
    return 1;
  }

  @Override
  public void parseBlocksAndReturnComplexColumnByteArray(DimensionRawColumnChunk[] rawColumnChunks,
      DimensionColumnPage[][] dimensionColumnPages, int rowNumber, int pageNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] currentVal =
        copyBlockDataChunk(rawColumnChunks, dimensionColumnPages, rowNumber, pageNumber);
    if (!this.isDictionary && !this.isDirectDictionary) {
      dataOutputStream.writeShort(currentVal.length);
    }
    dataOutputStream.write(currentVal);
  }

  @Override public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
      throws IOException {
    readBlockDataChunk(blockChunkHolder);
  }

  @Override public Object getDataBasedOnDataType(ByteBuffer dataBuffer) {
    return getDataObject(dataBuffer, -1);
  }

  @Override public Object getDataBasedOnColumn(ByteBuffer dataBuffer, CarbonDimension parent,
      CarbonDimension child) {
    Object actualData;

    if (parent.getOrdinal() != child.getOrdinal() || null == dataBuffer || !dataBuffer
        .hasRemaining()) {
      return null;
    }
    int size;
    if (!DataTypeUtil.isFixedSizeDataType(child.getDataType())) {
      size = dataBuffer.array().length;
    } else if (child.getDataType() == DataTypes.TIMESTAMP) {
      size = DataTypes.LONG.getSizeInBytes();
    } else {
      size = child.getDataType().getSizeInBytes();
    }
    actualData = getDataObject(dataBuffer, size);

    return actualData;
  }

  private Object getDataObject(ByteBuffer dataBuffer, int size) {
    Object actualData;
    if (isDirectDictionary) {
      // Direct Dictionary Column
      byte[] data = new byte[keySize];
      dataBuffer.get(data);
      Bits bit = new Bits(new int[] { keySize * 8 });
      int surrgateValue = (int) bit.getKeyArray(data, 0)[0];
      DirectDictionaryGenerator directDictionaryGenerator =
          DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(dataType);
      actualData = directDictionaryGenerator.getValueFromSurrogate(surrgateValue);
    } else if (!isDictionary) {
      if (size == -1) {
        size = dataBuffer.getShort();
      }
      byte[] value = new byte[size];
      dataBuffer.get(value, 0, size);
      if (dataType == DataTypes.DATE) {
        if (value.length == 0) {
          actualData = null;
        } else {
          actualData = this.directDictGenForDate.getValueFromSurrogate(
              ByteUtil.toXorInt(value, 0, CarbonCommonConstants.INT_SIZE_IN_BYTE));
        }
      } else {
        actualData = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(value, this.dataType);
      }
    } else {
      // Dictionary Column
      byte[] data = new byte[keySize];
      dataBuffer.get(data);
      Bits bit = new Bits(new int[] { keySize * 8 });
      int surrgateValue = (int) bit.getKeyArray(data, 0)[0];
      String dictionaryValueForKey = dictionary.getDictionaryValueForKey(surrgateValue);
      actualData = DataTypeUtil.getDataBasedOnDataType(dictionaryValueForKey, this.dataType);
    }
    return actualData;
  }

  @Override public Object getDataBasedOnColumnList(Map<CarbonDimension, ByteBuffer> childBuffer,
      CarbonDimension presentColumn) {
    return getDataBasedOnColumn(childBuffer.get(presentColumn), presentColumn, presentColumn);
  }
}
