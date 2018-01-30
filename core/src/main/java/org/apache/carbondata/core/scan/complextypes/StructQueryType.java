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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructQueryType extends ComplexQueryType implements GenericQueryType {

  private List<GenericQueryType> children = new ArrayList<GenericQueryType>();
  private String name;
  private String parentname;

  public StructQueryType(String name, String parentname, int blockIndex) {
    super(name, parentname, blockIndex);
    this.name = name;
    this.parentname = parentname;
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

  @Override public int getColsCount() {
    int colsCount = 1;
    for (int i = 0; i < children.size(); i++) {
      colsCount += children.get(i).getColsCount();
    }
    return colsCount;
  }

  @Override public void parseBlocksAndReturnComplexColumnByteArray(
      DimensionRawColumnChunk[] dimensionColumnDataChunks, int rowNumber,
      int pageNumber, DataOutputStream dataOutputStream) throws IOException {
    byte[] input = copyBlockDataChunk(dimensionColumnDataChunks, rowNumber, pageNumber);
    ByteBuffer byteArray = ByteBuffer.wrap(input);
    int childElement = byteArray.getInt();
    dataOutputStream.writeInt(childElement);
    if (childElement > 0) {
      for (int i = 0; i < childElement; i++) {
        children.get(i)
            .parseBlocksAndReturnComplexColumnByteArray(dimensionColumnDataChunks, rowNumber,
                pageNumber, dataOutputStream);
      }
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

  @Override public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
      throws IOException {
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

    return new GenericInternalRow(fields);
  }
}
