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
package org.apache.carbondata.core.datastore.chunk.reader.measure;

import java.io.IOException;

import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Measure block reader abstract class
 */
public abstract class AbstractMeasureChunkReader implements MeasureColumnChunkReader {
  protected Compressor compressor;

  protected EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  /**
   * file path from which blocks will be read
   */
  protected String filePath;

  /**
   * number of rows for blocklet
   */
  protected int numberOfRows;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param filePath file from which data will be read
   */
  public AbstractMeasureChunkReader(String filePath, int numberOfRows) {
    this.filePath = filePath;
    this.numberOfRows = numberOfRows;
  }

  @Override
  public void decodeColumnPageAndFillVector(MeasureRawColumnChunk measureRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException {
    throw new UnsupportedOperationException(
        "This operation is not supported in this class " + getClass().getName());
  }
}
