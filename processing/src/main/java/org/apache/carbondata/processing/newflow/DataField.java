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

package org.apache.carbondata.processing.newflow;

import java.io.Serializable;

import org.apache.carbondata.core.carbon.metadata.blocklet.compressor.CompressionCodec;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;

/**
 * Metadata class for each column of table.
 */
public class DataField implements Serializable {

  private CarbonColumn column;

  private CompressionCodec compressionCodec;

  public boolean hasDictionaryEncoding() {
    return column.hasEncoding(Encoding.DICTIONARY);
  }

  public CarbonColumn getColumn() {
    return column;
  }

  public void setColumn(CarbonColumn column) {
    this.column = column;
  }

  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

}
