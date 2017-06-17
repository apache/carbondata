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
package org.apache.carbondata.core.indexstore;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;

/**
 * Blocklet
 */
public class Blocklet implements Serializable {

  private String path;

  private String blockletId;

  private BlockletDetailInfo detailInfo;

  public Blocklet(String path, String blockletId) {
    this.path = path;
    this.blockletId = blockletId;
  }

  public String getPath() {
    return path;
  }

  public String getBlockletId() {
    return blockletId;
  }

  public BlockletDetailInfo getDetailInfo() {
    return detailInfo;
  }

  public void setDetailInfo(BlockletDetailInfo detailInfo) {
    this.detailInfo = detailInfo;
  }

  public static class BlockletDetailInfo implements Serializable {

    private int rowCount;

    private int pagesCount;

    private int versionNumber;

    private BlockletInfo blockletInfo;

    public int getRowCount() {
      return rowCount;
    }

    public void setRowCount(int rowCount) {
      this.rowCount = rowCount;
    }

    public int getPagesCount() {
      return pagesCount;
    }

    public void setPagesCount(int pagesCount) {
      this.pagesCount = pagesCount;
    }

    public int getVersionNumber() {
      return versionNumber;
    }

    public void setVersionNumber(int versionNumber) {
      this.versionNumber = versionNumber;
    }

    public BlockletInfo getBlockletInfo() {
      return blockletInfo;
    }

    public void setBlockletInfo(BlockletInfo blockletInfo) {
      this.blockletInfo = blockletInfo;
    }
  }
}
