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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Interface for reading the data chunk
 * Its concrete implementation can be used to read the chunk.
 * compressed or uncompressed chunk
 */
public interface DimensionColumnChunkReader {

  /**
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param columnIndexRange blocklets to be read
   * @return dimension column chunks
   */
  DimensionRawColumnChunk[] readRawDimensionChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException;

  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param columnIndex column to be read
   * @return dimension column chunk
   */
  DimensionRawColumnChunk readRawDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException;

  /**
   * Converts the raw data chunk to processed chunk based on blocklet indexes and page numbers
   *
   * @param dimensionRawColumnChunk raw data chunk
   * @param pageNumber page number to be processed
   * @return
   * @throws IOException
   */
  DimensionColumnPage decodeColumnPage(DimensionRawColumnChunk dimensionRawColumnChunk,
      int pageNumber) throws IOException, MemoryException;

  /**
   * Decodes the raw data chunk of given page number and fill the vector with decoded data.
   */
  void decodeColumnPageAndFillVector(DimensionRawColumnChunk dimensionRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException;
}
