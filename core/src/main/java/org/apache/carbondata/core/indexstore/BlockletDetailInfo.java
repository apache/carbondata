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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;

import org.apache.hadoop.io.Writable;

/**
 * Blocklet detail information to be sent to each executor
 */
public class BlockletDetailInfo implements Serializable, Writable {

  private int rowCount;

  private short pagesCount;

  private short versionNumber;

  private short blockletId;

  private int[] dimLens;

  private long schemaUpdatedTimeStamp;

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

  public void setPagesCount(short pagesCount) {
    this.pagesCount = pagesCount;
  }

  public short getVersionNumber() {
    return versionNumber;
  }

  public void setVersionNumber(short versionNumber) {
    this.versionNumber = versionNumber;
  }

  public BlockletInfo getBlockletInfo() {
    return blockletInfo;
  }

  public void setBlockletInfo(BlockletInfo blockletInfo) {
    this.blockletInfo = blockletInfo;
  }

  public int[] getDimLens() {
    return dimLens;
  }

  public void setDimLens(int[] dimLens) {
    this.dimLens = dimLens;
  }

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(rowCount);
    out.writeShort(pagesCount);
    out.writeShort(versionNumber);
    out.writeShort(blockletId);
    out.writeShort(dimLens.length);
    for (int i = 0; i < dimLens.length; i++) {
      out.writeInt(dimLens[i]);
    }
    out.writeLong(schemaUpdatedTimeStamp);
    blockletInfo.write(out);
  }

  @Override public void readFields(DataInput in) throws IOException {
    rowCount = in.readInt();
    pagesCount = in.readShort();
    versionNumber = in.readShort();
    blockletId = in.readShort();
    dimLens = new int[in.readShort()];
    for (int i = 0; i < dimLens.length; i++) {
      dimLens[i] = in.readInt();
    }
    schemaUpdatedTimeStamp = in.readLong();
    blockletInfo = new BlockletInfo();
    blockletInfo.readFields(in);
  }

  public Short getBlockletId() {
    return blockletId;
  }

  public void setBlockletId(Short blockletId) {
    this.blockletId = blockletId;
  }
}
