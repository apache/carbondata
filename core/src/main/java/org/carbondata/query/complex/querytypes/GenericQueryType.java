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
import java.util.List;

import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;

import org.apache.spark.sql.types.DataType;

public interface GenericQueryType {

  String getName();

  void setName(String name);

  String getParentname();

  void setParentname(String parentname);

  int getBlockIndex();

  void setBlockIndex(int blockIndex);

  void addChildren(GenericQueryType children);

  void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild);

  int getSurrogateIndex();

  void setSurrogateIndex(int surrIndex);

  int getColsCount();

  void setKeySize(int[] keyBlockSize);

  int getKeyOrdinalForQuery();

  void setKeyOrdinalForQuery(int keyOrdinalForQuery);

  void parseBlocksAndReturnComplexColumnByteArray(DimensionColumnDataChunk[] dimensionDataChunks,
      int rowNumber, DataOutputStream dataOutputStream) throws IOException;

  DataType getSchemaType();

  void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput)
      throws IOException;

  void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder);

  Object getDataBasedOnDataTypeFromSurrogates(ByteBuffer surrogateData);

}
