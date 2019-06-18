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

package org.apache.carbondata.sdk.file;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.sdk.file.arrow.ArrowConverter;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * Reader for CarbonData file which fills the arrow vector
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class ArrowCarbonReader<T> extends CarbonReader<T> {

  /**
   * Call {@link #builder(String)} to construct an instance
   */
  ArrowCarbonReader(List<RecordReader<Void, T>> readers) {
    super(readers);
  }

  /**
   * Carbon reader will fill the arrow vector after reading the carbondata files.
   * This arrow byte[] can be used to create arrow table and used for in memory analytics
   * Note: create a reader at blocklet level, so that arrow byte[] will not exceed INT_MAX
   *
   * @param carbonSchema org.apache.carbondata.sdk.file.Schema
   * @return Serialized byte array
   * @throws Exception
   */
  public byte[] readArrowBatch(Schema carbonSchema) throws Exception {
    ArrowConverter arrowConverter = new ArrowConverter(carbonSchema, 0);
    while (hasNext()) {
      arrowConverter.addToArrowBuffer(readNextBatchRow());
    }
    return arrowConverter.toSerializeArray();
  }

  /**
   * Carbon reader will fill the arrow vector after reading the carbondata files.
   * This arrow byte[] can be used to create arrow table and used for in memory analytics
   * Note: create a reader at blocklet level, so that arrow byte[] will not exceed INT_MAX
   * User need to close the VectorSchemaRoot after usage by calling VectorSchemaRoot.close()
   *
   * @param carbonSchema org.apache.carbondata.sdk.file.Schema
   * @return Arrow VectorSchemaRoot
   * @throws Exception
   */
  public VectorSchemaRoot readArrowVectors(Schema carbonSchema) throws Exception {
    ArrowConverter arrowConverter = new ArrowConverter(carbonSchema, 0);
    while (hasNext()) {
      arrowConverter.addToArrowBuffer(readNextBatchRow());
    }
    return arrowConverter.getArrowVectors();
  }

  /**
   * Carbon reader will fill the arrow vector after reading carbondata files.
   * Here unsafe memory address will be returned instead of byte[],
   * so that this address can be sent across java to python or c modules and
   * can directly read the content from this unsafe memory
   * Note:Create a carbon reader at blocklet level using CarbonReader.buildWithSplits(split) method,
   * so that arrow byte[] will not exceed INT_MAX.
   *
   * @param carbonSchema org.apache.carbondata.sdk.file.Schema
   * @return address of the unsafe memory where arrow buffer is stored
   * @throws Exception
   */
  public long readArrowBatchAddress(Schema carbonSchema) throws Exception {
    ArrowConverter arrowConverter = new ArrowConverter(carbonSchema, 0);
    while (hasNext()) {
      arrowConverter.addToArrowBuffer(readNextBatchRow());
    }
    return arrowConverter.copySerializeArrayToOffHeap();
  }

  /**
   * free the unsafe memory allocated , if unsafe arrow batch is used.
   *
   * @param address address of the unsafe memory where arrow buffer is stored
   */
  public void freeArrowBatchMemory(long address) {
    CarbonUnsafe.getUnsafe().freeMemory(address);
  }
}
