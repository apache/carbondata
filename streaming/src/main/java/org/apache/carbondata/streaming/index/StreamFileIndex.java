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

package org.apache.carbondata.streaming.index;

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;

@InterfaceAudience.Internal
public class StreamFileIndex implements Serializable {

  /**
   * the name of file, it doesn't contain the whole path.
   */
  private String fileName;

  private BlockletMinMaxIndex minMaxIndex;

  private long rowCount;

  private DataType[] msrDataTypes;

  public StreamFileIndex(String fileName, BlockletMinMaxIndex minMaxIndex, long rowCount) {
    this.fileName = fileName;
    this.minMaxIndex = minMaxIndex;
    this.rowCount = rowCount;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public BlockletMinMaxIndex getMinMaxIndex() {
    return minMaxIndex;
  }

  public void setMinMaxIndex(BlockletMinMaxIndex minMaxIndex) {
    this.minMaxIndex = minMaxIndex;
  }

  public long getRowCount() {
    return rowCount;
  }

  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public DataType[] getMsrDataTypes() {
    return msrDataTypes;
  }

  public void setMsrDataTypes(DataType[] msrDataTypes) {
    this.msrDataTypes = msrDataTypes;
  }
}
