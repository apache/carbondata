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
package org.apache.carbondata.core.metadata.index;

import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;

/**
 * Below class will be used hold the information
 * about block index
 */
public class BlockIndexInfo {

  /**
   * total number of rows present in the file
   */
  private long numberOfRows;

  /**
   * file name
   */
  private String fileName;

  /**
   * offset of metadata in data file
   */
  private long offset;

  /**
   * to store min max and start and end key
   */
  private BlockletIndex blockletIndex;

  /**
   * to store blocklet info like offsets and lengths of each column.
   */
  private BlockletInfo blockletInfo;

  /**
   * file size
   */
  private long fileSize;

  /**
   * Constructor
   *
   * @param numberOfRows  number of rows
   * @param fileName      full qualified name
   * @param offset        offset of the metadata in data file
   * @param blockletIndex block let index
   */
  public BlockIndexInfo(long numberOfRows, String fileName, long offset,
      BlockletIndex blockletIndex) {
    this.numberOfRows = numberOfRows;
    this.fileName = fileName;
    this.offset = offset;
    this.blockletIndex = blockletIndex;
  }

  /**
   *
   * @param numberOfRows
   * @param fileName
   * @param offset
   * @param blockletIndex
   * @param blockletInfo
   */
  public BlockIndexInfo(long numberOfRows, String fileName, long offset,
      BlockletIndex blockletIndex, BlockletInfo blockletInfo) {
    this(numberOfRows, fileName, offset, blockletIndex);
    this.blockletInfo = blockletInfo;
  }

  public BlockIndexInfo(long numberOfRows, String fileName, long offset,
      BlockletIndex blockletIndex, BlockletInfo blockletInfo, long fileSize) {
    this(numberOfRows, fileName, offset, blockletIndex, blockletInfo);
    this.fileSize = fileSize;
  }

  /**
   * @return the numberOfRows
   */
  public long getNumberOfRows() {
    return numberOfRows;
  }

  /**
   * @return the fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return the offset
   */
  public long getOffset() {
    return offset;
  }

  /**
   * @return the blockletIndex
   */
  public BlockletIndex getBlockletIndex() {
    return blockletIndex;
  }

  /**
   * @return BlockletInfo
   */
  public BlockletInfo getBlockletInfo() {
    return blockletInfo;
  }

  /**
   * @return file size
   */
  public long getFileSize() {
    return fileSize;
  }
}
