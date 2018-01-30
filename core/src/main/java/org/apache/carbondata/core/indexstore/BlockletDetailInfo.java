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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.hadoop.io.Writable;
import org.xerial.snappy.Snappy;

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

  private long blockFooterOffset;

  private List<ColumnSchema> columnSchemas;

  private byte[] columnSchemaBinary;

  private long blockSize;

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

  public long getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
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
    out.writeBoolean(blockletInfo != null);
    if (blockletInfo != null) {
      blockletInfo.write(out);
    }
    out.writeLong(blockFooterOffset);
    out.writeInt(columnSchemaBinary.length);
    out.write(columnSchemaBinary);
    out.writeLong(blockSize);
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
    if (in.readBoolean()) {
      blockletInfo = new BlockletInfo();
      blockletInfo.readFields(in);
    }
    blockFooterOffset = in.readLong();
    int bytesSize = in.readInt();
    byte[] schemaArray = new byte[bytesSize];
    in.readFully(schemaArray);
    readColumnSchema(schemaArray);
    blockSize = in.readLong();
  }

  /**
   * Read column schema from binary
   * @param schemaArray
   * @throws IOException
   */
  public void readColumnSchema(byte[] schemaArray) throws IOException {
    // uncompress it.
    schemaArray = Snappy.uncompress(schemaArray);
    ByteArrayInputStream schemaStream = new ByteArrayInputStream(schemaArray);
    DataInput schemaInput = new DataInputStream(schemaStream);
    columnSchemas = new ArrayList<>();
    int size = schemaInput.readShort();
    for (int i = 0; i < size; i++) {
      ColumnSchema columnSchema = new ColumnSchema();
      columnSchema.readFields(schemaInput);
      columnSchemas.add(columnSchema);
    }
  }

  /**
   * Create copy of BlockletDetailInfo
   */
  public BlockletDetailInfo copy() {
    BlockletDetailInfo detailInfo = new BlockletDetailInfo();
    detailInfo.rowCount = rowCount;
    detailInfo.pagesCount = pagesCount;
    detailInfo.versionNumber = versionNumber;
    detailInfo.blockletId = blockletId;
    detailInfo.dimLens = dimLens;
    detailInfo.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
    detailInfo.blockletInfo = blockletInfo;
    detailInfo.blockFooterOffset = blockFooterOffset;
    detailInfo.columnSchemas = columnSchemas;
    detailInfo.blockSize = blockSize;
    return detailInfo;
  }

  public Short getBlockletId() {
    return blockletId;
  }

  public void setBlockletId(Short blockletId) {
    this.blockletId = blockletId;
  }

  public long getBlockFooterOffset() {
    return blockFooterOffset;
  }

  public void setBlockFooterOffset(long blockFooterOffset) {
    this.blockFooterOffset = blockFooterOffset;
  }

  public List<ColumnSchema> getColumnSchemas() {
    return columnSchemas;
  }

  public void setColumnSchemaBinary(byte[] columnSchemaBinary) {
    this.columnSchemaBinary = columnSchemaBinary;
  }

  public byte[] getColumnSchemaBinary() {
    return columnSchemaBinary;
  }
}
