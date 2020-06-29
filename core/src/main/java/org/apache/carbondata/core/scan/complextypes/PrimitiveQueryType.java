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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
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

  private org.apache.carbondata.core.metadata.datatype.DataType dataType;

  private boolean isDirectDictionary;

  public PrimitiveQueryType(String name, String parentName, int columnIndex, DataType dataType,
      boolean isDirectDictionary) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    super(name, parentName, columnIndex);
    this.dataType = dataType;
    this.name = name;
    this.parentName = parentName;
    this.isDirectDictionary = isDirectDictionary;
  }

  @Override
  public void addChildren(GenericQueryType children) {

  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getParentName() {
    return parentName;
  }

  @Override
  public void setParentName(String parentName) {
    this.parentName = parentName;

  }

  @Override
  public int getColsCount() {
    return 1;
  }

  @Override
  public void parseBlocksAndReturnComplexColumnByteArray(DimensionRawColumnChunk[] rawColumnChunks,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3145
      DimensionColumnPage[][] dimensionColumnPages, int rowNumber, int pageNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] currentVal =
        copyBlockDataChunk(rawColumnChunks, dimensionColumnPages, rowNumber, pageNumber);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    if (!this.isDirectDictionary) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3653
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        dataOutputStream.writeInt(currentVal.length);
      } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
        dataOutputStream.writeShort(currentVal.length);
      }
    }
    dataOutputStream.write(currentVal);
  }

  @Override
  public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
      throws IOException {
    readBlockDataChunk(blockChunkHolder);
  }

  @Override
  public Object getDataBasedOnDataType(ByteBuffer dataBuffer) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
    return getDataObject(dataBuffer, -1);
  }

  @Override
  public Object getDataBasedOnColumn(ByteBuffer dataBuffer, CarbonDimension parent,
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
      // Direct Dictionary Column, only for DATE type
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      byte[] data = new byte[4];
      dataBuffer.get(data);
      actualData = ByteUtil.convertBytesToInt(data);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
      if (size == -1) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3653
        if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
          size = dataBuffer.getInt();
        } else {
          size = dataBuffer.getShort();
        }
      }
      byte[] value = new byte[size];
      dataBuffer.get(value, 0, size);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2471
      if (dataType == DataTypes.DATE) {
        if (value.length == 0) {
          actualData = null;
        } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
          DirectDictionaryGenerator directDictGenForDate =
              DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(DataTypes.DATE);
          actualData = directDictGenForDate.getValueFromSurrogate(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2884
              ByteUtil.toXorInt(value, 0, CarbonCommonConstants.INT_SIZE_IN_BYTE));
        }
      } else {
        actualData = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(value, this.dataType);
      }
    }
    return actualData;
  }

  @Override
  public Object getDataBasedOnColumnList(Map<CarbonDimension, ByteBuffer> childBuffer,
      CarbonDimension presentColumn) {
    return getDataBasedOnColumn(childBuffer.get(presentColumn), presentColumn, presentColumn);
  }
}
