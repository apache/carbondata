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

import org.apache.carbondata.core.carbon.datastore.chunk.reader.MeasureColumnChunkReader;

/**
 * Measure block reader abstract class
 */
public abstract class AbstractMeasureChunkReader implements MeasureColumnChunkReader {

  /**
   * file path from which blocks will be read
   */
  protected String filePath;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param filePath           file from which data will be read
   */
  public AbstractMeasureChunkReader(String filePath) {
    this.filePath = filePath;
  }
}
