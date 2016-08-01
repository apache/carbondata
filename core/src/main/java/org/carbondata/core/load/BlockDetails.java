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

package org.carbondata.core.load;

import java.io.Serializable;

import org.carbondata.core.datastorage.store.impl.FileFactory;

/**
 * blocks info
 */
public class BlockDetails implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 2293906691860002339L;
  //block offset
  private long blockOffset;
  //block length
  private long blockLength;
  //file path which block belong to
  private String filePath;
  // locations where this block exists
  private String[] locations;

  public BlockDetails(String filePath, long blockOffset, long blockLength, String[] locations) {
    this.filePath = filePath;
    this.blockOffset = blockOffset;
    this.blockLength = blockLength;
    this.locations = locations;
  }

  public long getBlockOffset() {
    return blockOffset;
  }

  public void setBlockOffset(long blockOffset) {
    this.blockOffset = blockOffset;
  }

  public long getBlockLength() {
    return blockLength;
  }

  public void setBlockLength(long blockLength) {
    this.blockLength = blockLength;
  }

  public String getFilePath() {
    return FileFactory.getUpdatedFilePath(filePath);
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String[] getLocations() {
    return locations;
  }
}
