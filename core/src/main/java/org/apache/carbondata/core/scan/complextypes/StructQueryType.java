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
import java.util.Map;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.DataTypeUtil;

public class StructQueryType extends ComplexQueryType implements GenericQueryType {

  private List<GenericQueryType> children = new ArrayList<GenericQueryType>();
  private String name;
  private String parentName;

  public StructQueryType(String name, String parentName, int columnIndex) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    super(name, parentName, columnIndex);
    this.name = name;
    this.parentName = parentName;
  }

  @Override
  public void addChildren(GenericQueryType newChild) {
    if (this.getName().equals(newChild.getParentName())) {
      this.children.add(newChild);
    } else {
      for (GenericQueryType child : this.children) {
        child.addChildren(newChild);
      }
    }

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
    int colsCount = 1;
    for (int i = 0; i < children.size(); i++) {
      colsCount += children.get(i).getColsCount();
    }
    return colsCount;
  }

  @Override
  public void parseBlocksAndReturnComplexColumnByteArray(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3145
      DimensionRawColumnChunk[] dimensionColumnDataChunks,
      DimensionColumnPage[][] dimensionColumnPages, int rowNumber, int pageNumber,
      DataOutputStream dataOutputStream) throws IOException {
    byte[] input =
        copyBlockDataChunk(dimensionColumnDataChunks, dimensionColumnPages, rowNumber, pageNumber);
    ByteBuffer byteArray = ByteBuffer.wrap(input);
    int childElement = byteArray.getShort();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    dataOutputStream.writeShort(childElement);
    if (childElement > 0) {
      for (int i = 0; i < childElement; i++) {
        children.get(i).parseBlocksAndReturnComplexColumnByteArray(dimensionColumnDataChunks,
            dimensionColumnPages, rowNumber, pageNumber, dataOutputStream);
      }
    }
  }

  @Override
  public void fillRequiredBlockData(RawBlockletColumnChunks blockChunkHolder)
      throws IOException {
    readBlockDataChunk(blockChunkHolder);

    for (int i = 0; i < children.size(); i++) {
      children.get(i).fillRequiredBlockData(blockChunkHolder);
    }
  }

  @Override
  public Object getDataBasedOnDataType(ByteBuffer dataBuffer) {
    int childLength = dataBuffer.getShort();
    Object[] fields = new Object[childLength];
    for (int i = 0; i < childLength; i++) {
      fields[i] =  children.get(i).getDataBasedOnDataType(dataBuffer);
    }
    return DataTypeUtil.getDataTypeConverter().wrapWithGenericRow(fields);
  }

  @Override
  public Object getDataBasedOnColumn(ByteBuffer dataBuffer, CarbonDimension parent,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
      CarbonDimension child) {
    int childLength;
    if (parent.getOrdinal() < child.getOrdinal()) {
      childLength = parent.getNumberOfChild();
      Object[] fields = new Object[childLength];
      for (int i = 0; i < childLength; i++) {
        fields[i] = children.get(i)
            .getDataBasedOnColumn(dataBuffer, parent.getListOfChildDimensions().get(i), child);
      }
      return DataTypeUtil.getDataTypeConverter().wrapWithGenericRow(fields);
    } else if (parent.getOrdinal() > child.getOrdinal()) {
      return null;
    } else {
      //      childLength = dataBuffer.getShort();
      Object field = getDataBasedOnDataType(dataBuffer);
      return field;
    }
  }

  @Override
  public Object getDataBasedOnColumnList(Map<CarbonDimension, ByteBuffer> childBuffer,
      CarbonDimension presentColumn) {
    // Traverse through the Complex Tree and check if the at present column is same as the
    // column present in the child column then fill it up else add null to the column.
    if (childBuffer.get(presentColumn) != null) {
      if (presentColumn.getNumberOfChild() > 0) {
        // This is complex Column. And all its child will be present in the corresponding data
        // buffer.
        Object field = getDataBasedOnDataType(childBuffer.get(presentColumn));
        return field;
      } else {
        // This is a child column with with primitive data type.
        Object field = children.get(0)
            .getDataBasedOnColumn(childBuffer.get(presentColumn), presentColumn, presentColumn);
        return field;
      }
    } else {
      int childLength;
      childLength = presentColumn.getNumberOfChild();
      Object[] fields = new Object[childLength];
      for (int i = 0; i < childLength; i++) {
        fields[i] = children.get(i)
            .getDataBasedOnColumnList(childBuffer, presentColumn.getListOfChildDimensions().get(i));
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2163
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2164
      return DataTypeUtil.getDataTypeConverter().wrapWithGenericRow(fields);
    }
  }
}
