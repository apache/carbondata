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
import org.apache.carbondata.core.util.BlockletIndexUtil;

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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    if (null == blockletInfo) {
      try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3395
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    if (null == this.blockletInfo && null != blockletInfoBinary && blockletInfoBinary.length > 0) {
      blockletInfo = new BlockletInfo();
      ByteArrayInputStream stream = new ByteArrayInputStream(blockletInfoBinary);
      DataInputStream inputStream = new DataInputStream(stream);
      try {
        blockletInfo.readFields(inputStream);
      } catch (IOException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024
        LOGGER.error("Problem in reading blocklet info", e);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
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

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  public long getBlockSize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
    return blockSize;
  }

  public void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(rowCount);
    out.writeShort(pagesCount);
    out.writeShort(versionNumber);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1731
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1728
    out.writeShort(blockletId);
    out.writeLong(schemaUpdatedTimeStamp);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2020
    out.writeBoolean(blockletInfo != null);
    if (blockletInfo != null) {
      blockletInfo.write(out);
    }
    out.writeLong(blockFooterOffset);
    // convert column schema list to binary format for serializing
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2701
    convertColumnSchemaToBinary();
    if (null != columnSchemaBinary) {
      out.writeInt(columnSchemaBinary.length);
      out.write(columnSchemaBinary);
    } else {
      // write -1 if columnSchemaBinary is null so that at the time of reading it can distinguish
      // whether schema is written or not
      out.writeInt(-1);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    out.writeInt(blockletInfoBinary.length);
    out.write(blockletInfoBinary);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
    out.writeLong(blockSize);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    out.writeBoolean(useMinMaxForPruning);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rowCount = in.readInt();
    pagesCount = in.readShort();
    versionNumber = in.readShort();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1731
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1728
    blockletId = in.readShort();
    schemaUpdatedTimeStamp = in.readLong();
    if (in.readBoolean()) {
      blockletInfo = new BlockletInfo();
      blockletInfo.readFields(in);
    }
    blockFooterOffset = in.readLong();
    int bytesSize = in.readInt();
    // if byteSize is -1 that means schema binary is not written
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2701
    if (bytesSize != -1) {
      byte[] schemaArray = new byte[bytesSize];
      in.readFully(schemaArray);
      readColumnSchema(schemaArray);
    }
    int byteSize = in.readInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    blockletInfoBinary = new byte[byteSize];
    in.readFully(blockletInfoBinary);
    setBlockletInfoFromBinary();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
    blockSize = in.readLong();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    useMinMaxForPruning = in.readBoolean();
  }

  /**
   * Read column schema from binary
   * @param schemaArray
   * @throws IOException
   */
  public void readColumnSchema(byte[] schemaArray) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    if (null != schemaArray) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      columnSchemas = BlockletIndexUtil.readColumnSchema(schemaArray);
    }
  }

  private void convertColumnSchemaToBinary() throws IOException {
    if (null != columnSchemas) {
      columnSchemaBinary = BlockletIndexUtil.convertSchemaToBinary(columnSchemas);
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
    detailInfo.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
    detailInfo.blockletInfo = blockletInfo;
    detailInfo.blockletInfoBinary = blockletInfoBinary;
    detailInfo.blockFooterOffset = blockFooterOffset;
    detailInfo.columnSchemas = columnSchemas;
    detailInfo.columnSchemaBinary = columnSchemaBinary;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
    detailInfo.blockSize = blockSize;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    detailInfo.useMinMaxForPruning = useMinMaxForPruning;
    return detailInfo;
  }

  public Short getBlockletId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1731
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1728
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1998
    if (columnSchemas == null && columnSchemaBinary != null) {
      readColumnSchema(columnSchemaBinary);
    }
    return columnSchemas;
  }

  public byte[] getColumnSchemaBinary() {
    return columnSchemaBinary;
  }

  public void setBlockletInfoBinary(byte[] blockletInfoBinary) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    this.blockletInfoBinary = blockletInfoBinary;
  }

  public void setColumnSchemas(List<ColumnSchema> columnSchemas) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2701
    this.columnSchemas = columnSchemas;
  }

  public boolean isUseMinMaxForPruning() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2649
    return useMinMaxForPruning;
  }

  public void setUseMinMaxForPruning(boolean useMinMaxForPruning) {
    this.useMinMaxForPruning = useMinMaxForPruning;
  }
}
