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
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Blocklet detail information to be sent to each executor
 */
public class BlockletDetailInfo implements Serializable, Writable {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BlockletDetailInfo.class.getName());

  private static final long serialVersionUID = 7957493757421513808L;

  private int rowCount;

  private short pagesCount;

  private short versionNumber;

  // default blockletId should be -1,which means consider all the blocklets in block
  private short blockletId = -1;

  private int[] dimLens;

  private long schemaUpdatedTimeStamp;

  private BlockletInfo blockletInfo;

  private byte[] blockletInfoBinary;

  private long blockFooterOffset;

  private List<ColumnSchema> columnSchemas;

  private byte[] columnSchemaBinary;

  private long blockSize;

  /**
   * flag to check whether to serialize min max values. The flag will be set to true in case
   * 1. When CACHE_LEVEL = BLOCKLET and filter column min/max in not cached in the driver using the
   * property COLUMN_META_CACHE
   * 2. for CACHE_LEVEL = BLOCK, it will always be true which is also the default value
   */
  private boolean useMinMaxForPruning = true;

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
    if (null == blockletInfo) {
      try {
        synchronized (this) {
          if (null == blockletInfo) {
            setBlockletInfoFromBinary();
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return blockletInfo;
  }

  public void setBlockletInfo(BlockletInfo blockletInfo) {
    this.blockletInfo = blockletInfo;
  }

  private void setBlockletInfoFromBinary() throws IOException {
    if (null == this.blockletInfo && null != blockletInfoBinary && blockletInfoBinary.length > 0) {
      blockletInfo = new BlockletInfo();
      ByteArrayInputStream stream = new ByteArrayInputStream(blockletInfoBinary);
      DataInputStream inputStream = new DataInputStream(stream);
      try {
        blockletInfo.readFields(inputStream);
      } catch (IOException e) {
        LOGGER.error("Problem in reading blocklet info", e);
        throw new IOException("Problem in reading blocklet info." + e.getMessage(), e);
      } finally {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.error("Problem in closing input stream of reading blocklet info.", e);
        }
      }
    }
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
    // convert column schema list to binary format for serializing
    convertColumnSchemaToBinary();
    if (null != columnSchemaBinary) {
      out.writeInt(columnSchemaBinary.length);
      out.write(columnSchemaBinary);
    } else {
      // write -1 if columnSchemaBinary is null so that at the time of reading it can distinguish
      // whether schema is written or not
      out.writeInt(-1);
    }
    out.writeInt(blockletInfoBinary.length);
    out.write(blockletInfoBinary);
    out.writeLong(blockSize);
    out.writeBoolean(useMinMaxForPruning);
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
    // if byteSize is -1 that means schema binary is not written
    if (bytesSize != -1) {
      byte[] schemaArray = new byte[bytesSize];
      in.readFully(schemaArray);
      readColumnSchema(schemaArray);
    }
    int byteSize = in.readInt();
    blockletInfoBinary = new byte[byteSize];
    in.readFully(blockletInfoBinary);
    setBlockletInfoFromBinary();
    blockSize = in.readLong();
    useMinMaxForPruning = in.readBoolean();
  }

  /**
   * Read column schema from binary
   * @param schemaArray
   * @throws IOException
   */
  public void readColumnSchema(byte[] schemaArray) throws IOException {
    if (null != schemaArray) {
      columnSchemas = BlockletDataMapUtil.readColumnSchema(schemaArray);
    }
  }

  private void convertColumnSchemaToBinary() throws IOException {
    if (null != columnSchemas) {
      columnSchemaBinary = BlockletDataMapUtil.convertSchemaToBinary(columnSchemas);
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
    detailInfo.blockletInfoBinary = blockletInfoBinary;
    detailInfo.blockFooterOffset = blockFooterOffset;
    detailInfo.columnSchemas = columnSchemas;
    detailInfo.columnSchemaBinary = columnSchemaBinary;
    detailInfo.blockSize = blockSize;
    detailInfo.useMinMaxForPruning = useMinMaxForPruning;
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

  public List<ColumnSchema> getColumnSchemas() throws IOException {
    if (columnSchemas == null && columnSchemaBinary != null) {
      readColumnSchema(columnSchemaBinary);
    }
    return columnSchemas;
  }

  public byte[] getColumnSchemaBinary() {
    return columnSchemaBinary;
  }

  public void setBlockletInfoBinary(byte[] blockletInfoBinary) {
    this.blockletInfoBinary = blockletInfoBinary;
  }

  public void setColumnSchemas(List<ColumnSchema> columnSchemas) {
    this.columnSchemas = columnSchemas;
  }

  public boolean isUseMinMaxForPruning() {
    return useMinMaxForPruning;
  }

  public void setUseMinMaxForPruning(boolean useMinMaxForPruning) {
    this.useMinMaxForPruning = useMinMaxForPruning;
  }
}
