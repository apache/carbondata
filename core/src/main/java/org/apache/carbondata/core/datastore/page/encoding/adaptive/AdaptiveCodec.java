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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorage;
import org.apache.carbondata.core.datastore.columnar.ObjectArrayBlockIndexerStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.SortState;

/**
 * Subclass of this codec depends on statistics of the column page (adaptive) to perform apply
 * and decode, it also employs compressor to compress the encoded data
 */
public abstract class AdaptiveCodec implements ColumnPageCodec {

  // TODO: cache and reuse the same encoder since snappy is thread-safe

  // statistics of this page, can be used by subclass
  protected final SimpleStatsResult stats;

  // the data type used for storage
  protected final DataType targetDataType;

  // the data type specified in schema
  protected final DataType srcDataType;

  protected boolean isInvertedIndex;

  protected BlockIndexerStorage<Object[]> indexStorage;

  protected ColumnPage encodedPage;

  protected AdaptiveCodec(DataType srcDataType, DataType targetDataType,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
      SimpleStatsResult stats, boolean isInvertedIndex) {
    this.stats = stats;
    this.srcDataType = srcDataType;
    this.targetDataType = targetDataType;
    this.isInvertedIndex = isInvertedIndex;
  }

  public DataType getTargetDataType() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1435
    return targetDataType;
  }

  /**
   * Convert the data of the page based on the data type for each row
   * While preparing the inverted index for the page,
   * we need the data based on data type for no dict measure column if adaptive encoding is applied
   * This is similar to page.getByteArrayPage()
   *
   * @param input
   * @return
   */
  public Object[] getPageBasedOnDataType(ColumnPage input) {
    Object[] data = new Object[input.getActualRowCount()];
    if (srcDataType == DataTypes.BYTE || srcDataType == DataTypes.BOOLEAN) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getByte(i);
      }
    } else if (srcDataType == DataTypes.SHORT) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getShort(i);
      }
    } else if (srcDataType == DataTypes.SHORT_INT) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getShortInt(i);
      }
    } else if (srcDataType == DataTypes.INT) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getInt(i);
      }
    } else if (srcDataType == DataTypes.LONG) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getLong(i);
      }
    } else if (srcDataType == DataTypes.FLOAT) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getFloat(i);
      }
    } else if (srcDataType == DataTypes.DOUBLE) {
      for (int i = 0; i < input.getActualRowCount(); i++) {
        data[i] = input.getDouble(i);
      }
    }
    return data;
  }

  /**
   * Put the data to the page based on the data type for each row
   *
   * @param page
   * @return
   */
  public void putDataToPage(ColumnPage page, Object[] dataPage) {
    if (srcDataType == DataTypes.BYTE || srcDataType == DataTypes.BOOLEAN) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putByte(i, (byte) dataPage[i]);
      }
    } else if (srcDataType == DataTypes.SHORT) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putShort(i, (short) dataPage[i]);
      }
    } else if (srcDataType == DataTypes.SHORT_INT) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putShortInt(i, (int) dataPage[i]);
      }
    } else if (srcDataType == DataTypes.INT) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putInt(i, (int) dataPage[i]);
      }
    } else if (srcDataType == DataTypes.LONG) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putLong(i, (long) dataPage[i]);
      }
    } else if (srcDataType == DataTypes.DOUBLE) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putDouble(i, (double) dataPage[i]);
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2948
    } else if (srcDataType == DataTypes.FLOAT) {
      for (int i = 0; i < dataPage.length; i++) {
        page.putFloat(i, (float) dataPage[i]);
      }
    }
  }

  /**
   * Write the inverted index to the page if required
   *
   * @param result
   * @throws IOException
   */
  public ByteBuffer writeInvertedIndexIfRequired(ByteBuffer result) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    if (null != indexStorage) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3799
      if (result.isDirect()) {
        // cannot apply result.array() on direct buffers
        for (int i = result.position(); i < result.limit(); i++) {
          out.writeByte(result.get(i));
        }
      } else {
        out.write(result.array(), result.position(), result.limit());
      }
      if (indexStorage.getRowIdPageLengthInBytes() > 0) {
        out.writeInt(indexStorage.getRowIdPageLengthInBytes());
        short[] rowIdPage = (short[]) indexStorage.getRowIdPage();
        for (short rowId : rowIdPage) {
          out.writeShort(rowId);
        }
        if (indexStorage.getRowIdRlePageLengthInBytes() > 0) {
          short[] rowIdRlePage = (short[]) indexStorage.getRowIdRlePage();
          for (short rowIdRle : rowIdRlePage) {
            out.writeShort(rowIdRle);
          }
        }
      }
    }
    byte[] bytes = stream.toByteArray();
    stream.close();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Fill legacy fields if required
   *
   * @param dataChunk
   * @param result
   */
  public void fillLegacyFieldsIfRequired(DataChunk2 dataChunk, ByteBuffer result) {
    if (null != indexStorage) {
      SortState sort = (indexStorage.getRowIdPageLengthInBytes() > 0) ?
          SortState.SORT_EXPLICIT :
          SortState.SORT_NATIVE;
      dataChunk.setSort_state(sort);
      if (indexStorage.getRowIdPageLengthInBytes() > 0) {
        int rowIdPageLength =
            CarbonCommonConstants.INT_SIZE_IN_BYTE + indexStorage.getRowIdPageLengthInBytes()
                + indexStorage.getRowIdRlePageLengthInBytes();
        dataChunk.setRowid_page_length(rowIdPageLength);
      }
    } else {
      dataChunk.setRowid_page_length(0);
    }
    if (null != result) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731
      dataChunk.setData_page_length(result.limit() - result.position());
    }
  }

  /**
   * Get the new column page based on the sorted data
   *
   * @param input
   * @return
   */
  public ColumnPage getSortedColumnPageIfRequired(ColumnPage input) {
    if (null != indexStorage) {
      Object[] dataPage = indexStorage.getDataPage();
      ColumnPageEncoderMeta columnPageEncoderMeta =
          new ColumnPageEncoderMeta(input.getColumnSpec(), input.getDataType(),
              input.getColumnPageEncoderMeta().getCompressorName());
      ColumnPage columnPage = ColumnPage.newPage(columnPageEncoderMeta, input.getPageSize());
      putDataToPage(columnPage, dataPage);
      return columnPage;
    } else {
      return input;
    }
  }

  public ByteBuffer encodeAndCompressPage(ColumnPage input, ColumnPageValueConverter converter,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      Compressor compressor) throws IOException {
    encodedPage = ColumnPage.newPage(
        new ColumnPageEncoderMeta(input.getColumnPageEncoderMeta().getColumnSpec(), targetDataType,
            input.getColumnPageEncoderMeta().getCompressorName()), input.getPageSize());
    if (isInvertedIndex) {
      indexStorage =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3730
          new ObjectArrayBlockIndexerStorage(getPageBasedOnDataType(input), input.getDataType(),
              isInvertedIndex);
    }
    ColumnPage columnPage = getSortedColumnPageIfRequired(input);
    columnPage.convertValue(converter);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    return encodedPage.compress(compressor);
  }

  @Override
  public String toString() {
    return String.format("%s[src type: %s, target type: %s, stats(%s)]",
        getClass().getName(), srcDataType, targetDataType, stats);
  }

  protected String debugInfo() {
    return this.toString();
  }

}
