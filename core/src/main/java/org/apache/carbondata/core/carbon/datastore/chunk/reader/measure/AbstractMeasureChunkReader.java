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
package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;

/**
 * Measure block reader abstract class
 */
public abstract class AbstractMeasureChunkReader implements MeasureColumnChunkReader {

  /**
   * metadata which was to used to compress and uncompress the measure value
   */
  protected ValueCompressionModel compressionModel;

  /**
   * file path from which blocks will be read
   */
  protected String filePath;

  /**
   * measure chunk have the information about the metadata present in the file
   */
  protected List<DataChunk> measureColumnChunk;

  /**
   * type of valu comprssion model selected for each measure column
   */
  protected UnCompressValue[] values;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param measureColumnChunk measure chunk metadata
   * @param compression        model metadata which was to used to compress and uncompress
   *                           the measure value
   * @param filePath           file from which data will be read
   * @param isInMemory         in case of in memory it will read and holds the data and when
   *                           query request will come it will uncompress and the data
   */
  public AbstractMeasureChunkReader(List<DataChunk> measureColumnChunk,
      ValueCompressionModel compressionModel, String filePath, boolean isInMemory) {
    this.measureColumnChunk = measureColumnChunk;
    this.compressionModel = compressionModel;
    this.filePath = filePath;
    values =
        new ValueCompressonHolder.UnCompressValue[compressionModel.getUnCompressValues().length];
    for (int i = 0; i < values.length; i++) {
      values[i] = compressionModel.getUnCompressValues()[i].getNew().getCompressorObject();
    }
  }
}
