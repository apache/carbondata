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

package org.apache.carbondata.processing.store.writer;

import java.util.BitSet;

import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public interface CarbonFactDataWriter<T> {

  /**
   * This method will be used to write leaf data to file
   * file format
   * <key><measure1><measure2>....
   *
   * @param dataArray            measure array
   * @param entryCount           number of entries
   * @param startKey             start key of leaf
   * @param endKey               end key of leaf
   * @param noDictionaryEndKey
   * @param noDictionaryStartKey
   * @throws CarbonDataWriterException throws new CarbonDataWriterException if any problem
   */

  NodeHolder buildDataNodeHolder(IndexStorage<T>[] keyStorageArray, byte[][] dataArray,
      int entryCount, byte[] startKey, byte[] endKey, WriterCompressModel compressionModel,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey, BitSet[] nullValueIndexBitSet)
      throws CarbonDataWriterException;

  /**
   * If node holder flag is enabled the object will be added to list
   * and all the blocklets will be return together. If disabled then this
   * method will itself will call for writing the fact data
   *
   * @param holder
   */
  void writeBlockletData(NodeHolder holder) throws CarbonDataWriterException;

  /**
   * Below method will be used to write the leaf meta data to file
   *
   * @throws CarbonDataWriterException
   */
  void writeBlockletInfoToFile() throws CarbonDataWriterException;

  /**
   * Below method will be used to initialise the writer
   */
  void initializeWriter() throws CarbonDataWriterException;

  /**
   * Below method will be used to close the writer
   */
  void closeWriter() throws CarbonDataWriterException;

  /**
   * @param isNoDictionary
   */
  void setIsNoDictionary(boolean[] isNoDictionary);

}
