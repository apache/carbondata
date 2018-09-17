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
package org.apache.carbondata.core.datastore.chunk.reader;

import java.io.IOException;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Reader interface for reading the measure blocks from file
 */
public interface MeasureColumnChunkReader {

  /**
   * Method to read the blocks data based on block indexes
   *
   * @param fileReader   file reader to read the blocks
   * @param columnIndexRange blocks to be read
   * @return measure data chunks
   */
  MeasureRawColumnChunk[] readRawMeasureChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException;

  /**
   * Method to read the blocks data based on block index
   *
   * @param fileReader file reader to read the blocks
   * @param columnIndex block to be read
   * @return measure data chunk
   */
  MeasureRawColumnChunk readRawMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException;

  /**
   * Convert raw data to measure chunk
   * @param measureRawColumnChunk
   * @param pageNumber
   * @return
   * @throws IOException
   */
  ColumnPage decodeColumnPage(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber) throws IOException, MemoryException;

  /**
   * Decode raw data and fill the vector
   */
  void decodeColumnPageAndFillVector(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException;

}
