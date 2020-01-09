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

package org.apache.carbondata.core.segmentmeta;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.format.ColumnSchema;

/**
 * Hold's columnSchemas and  updated min-max values for all columns in a segment
 */
public class BlockColumnMetaDataInfo implements Serializable {

  private List<org.apache.carbondata.format.ColumnSchema> columnSchemas;

  private byte[][] min;

  private byte[][] max;

  public BlockColumnMetaDataInfo(List<org.apache.carbondata.format.ColumnSchema> columnSchemas,
      byte[][] min, byte[][] max) {
    this.columnSchemas = columnSchemas;
    this.min = min;
    this.max = max;
  }

  public byte[][] getMin() {
    return min;
  }

  public void setMinMax(byte[][] min, byte[][] max) {
    this.min = min;
    this.max = max;
  }

  public byte[][] getMax() {
    return max;
  }

  public List<ColumnSchema> getColumnSchemas() {
    return columnSchemas;
  }

  public void setColumnSchemas(List<ColumnSchema> columnSchemas) {
    this.columnSchemas = columnSchemas;
  }
}

