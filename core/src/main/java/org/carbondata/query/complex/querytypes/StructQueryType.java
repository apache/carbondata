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
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructQueryType extends ComplexQueryType implements GenericQueryType {

  private List<GenericQueryType> children = new ArrayList<GenericQueryType>();
  private String name;
  private String parentname;
  private int blockIndex;
  private int keyOrdinalForQuery;

  public StructQueryType(String name, String parentname, int blockIndex) {
    super(name, parentname, blockIndex);
    this.name = name;
    this.parentname = parentname;
    this.blockIndex = blockIndex;
  }

  @Override public void addChildren(GenericQueryType newChild) {
    if (this.getName().equals(newChild.getParentname())) {
      this.children.add(newChild);
    } else {
      for (GenericQueryType child : this.children) {
        child.addChildren(newChild);
      }
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

  @Override public void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild) {
    for (int i = 0; i < children.size(); i++) {
      GenericQueryType child = children.get(i);
      if (child instanceof PrimitiveQueryType) {
        primitiveChild.add(child);
      } else {
        child.getAllPrimitiveChildren(primitiveChild);
      }
    }
  }

  @Override public int getSurrogateIndex() {
    return 0;
  }

  @Override public void setSurrogateIndex(int surrIndex) {

  }

  @Override public int getBlockIndex() {
    return blockIndex;
  }

  @Override public void setBlockIndex(int blockIndex) {
    this.blockIndex = blockIndex;
  }

  @Override public int getColsCount() {
    int colsCount = 1;
    for (int i = 0; i < children.size(); i++) {
      colsCount += children.get(i).getColsCount();
    }
    return colsCount;
  }

  @Override public void parseBlocksAndReturnComplexColumnByteArray(
      DimensionColumnDataChunk[] dimensionColumnDataChunks, int rowNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] input = new byte[8];
    copyBlockDataChunk(dimensionColumnDataChunks, rowNumber, input);
    ByteBuffer byteArray = ByteBuffer.wrap(input);
    int childElement = byteArray.getInt();
    dataOutputStream.writeInt(childElement);
    if (childElement == 0) {
      // b.putInt(0);
    } else {
      for (int i = 0; i < childElement; i++) {
        children.get(i)
            .parseBlocksAndReturnComplexColumnByteArray(dimensionColumnDataChunks, rowNumber,
                dataOutputStream);
      }
    }
  }

  @Override public void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput)
      throws IOException {
    int childElement = complexData.getInt();
    dataOutput.writeInt(childElement);
    for (int i = 0; i < childElement; i++) {
      children.get(i).parseAndGetResultBytes(complexData, dataOutput);
    }
  }

  @Override public void setKeySize(int[] keyBlockSize) {
    for (int i = 0; i < children.size(); i++) {
      children.get(i).setKeySize(keyBlockSize);
    }
  }

  @Override public DataType getSchemaType() {
    StructField[] fields = new StructField[children.size()];
    for (int i = 0; i < children.size(); i++) {
      fields[i] = new StructField(children.get(i).getName(), null, true,
          Metadata.empty());
    }
    return new StructType(fields);
  }

  @Override public int getKeyOrdinalForQuery() {
    return keyOrdinalForQuery;
  }

  @Override public void setKeyOrdinalForQuery(int keyOrdinalForQuery) {
    this.keyOrdinalForQuery = keyOrdinalForQuery;
  }

  @Override public void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder) {
    readBlockDataChunk(blockChunkHolder);

    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillRequiredBlockData(blockChunkHolder);
    }
  }

  @Override public Object getDataBasedOnDataTypeFromSurrogates(ByteBuffer surrogateData) {
    int childLength = surrogateData.getInt();
    Object[] fields = new Object[childLength];
    for (int i = 0; i < childLength; i++) {
      fields[i] =  children.get(i).getDataBasedOnDataTypeFromSurrogates(surrogateData);
    }

    return new GenericInternalRowWithSchema(fields, (StructType) getSchemaType());
  }
}
