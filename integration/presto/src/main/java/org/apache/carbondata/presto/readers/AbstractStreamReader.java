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

package org.apache.carbondata.presto.readers;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

/**
 * Abstract class for Stream Readers
 */
public abstract class AbstractStreamReader implements StreamReader {

  protected Object[] streamData;

  protected ColumnVector columnVector;

  protected boolean isVectorReader;

  protected int batchSize;

  /**
   * Setter for StreamData
   * @param data
   */
  @Override public void setStreamData(Object[] data) {
    this.streamData = data;
  }

  /**
   * Setter for Vector data
   * @param vector
   */
  @Override public void setVector(ColumnVector vector) {
    this.columnVector = vector;
  }

  /**
   * Setter for vector Reader
   * @param isVectorReader
   */
  public void setVectorReader(boolean isVectorReader) {
    this.isVectorReader = isVectorReader;
  }

  /**
   * Setter for BatchSize
   * @param batchSize
   */
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
}
