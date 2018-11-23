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

import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.DataTypeUtil;

public class ArrayQueryType extends ComplexQueryType implements GenericQueryType {

  private GenericQueryType children;

  public ArrayQueryType(String name, String parentname, int blockIndex) {
    super(name, parentname, blockIndex);
  }

  @Override public void addChildren(GenericQueryType children) {
    if (this.getName().equals(children.getParentname())) {
      this.children = children;
    } else {
      this.children.addChildren(children);
    }
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

  public void parseBlocksAndReturnComplexColumnByteArray(DimensionRawColumnChunk[] rawColumnChunks,
      int rowNumber, int pageNumber, DataOutputStream dataOutputStream) throws IOException {
    byte[] input = copyBlockDataChunk(rawColumnChunks, rowNumber, pageNumber);
    ByteBuffer byteArray = ByteBuffer.wrap(input);
    int dataLength = byteArray.getInt();
    dataOutputStream.writeInt(dataLength);
    if (dataLength > 0) {
      int dataOffset = byteArray.getInt();
      for (int i = 0; i < dataLength; i++) {
        children
            .parseBlocksAndReturnComplexColumnByteArray(rawColumnChunks, dataOffset++, pageNumber,
                dataOutputStream);
      }
    }
  }

  @Override public int getColsCount() {
    return children.getColsCount() + 1;
  }

  @Override public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
      throws IOException {
    readBlockDataChunk(blockChunkHolder);
    children.fillRequiredBlockData(blockChunkHolder);
  }

  @Override public Object getDataBasedOnDataType(ByteBuffer dataBuffer) {
    Object[] data = fillData(dataBuffer);
    if (data == null) {
      return null;
    }
    return DataTypeUtil.getDataTypeConverter().wrapWithGenericArrayData(data);
  }

  protected Object[] fillData(ByteBuffer dataBuffer) {
    int dataLength = dataBuffer.getInt();
    if (dataLength == -1) {
      return null;
    }
    Object[] data = new Object[dataLength];
    for (int i = 0; i < dataLength; i++) {
      data[i] = children.getDataBasedOnDataType(dataBuffer);
    }
    return data;
  }

  @Override public Object getDataBasedOnColumn(ByteBuffer dataBuffer, CarbonDimension parent,
      CarbonDimension child) {
    throw new UnsupportedOperationException("Operation Unsupported for ArrayType");
  }

  @Override public Object getDataBasedOnColumnList(Map<CarbonDimension, ByteBuffer> childBuffer,
      CarbonDimension presentColumn) {
    throw new UnsupportedOperationException("Operation Unsupported for ArrayType");
  }

}
