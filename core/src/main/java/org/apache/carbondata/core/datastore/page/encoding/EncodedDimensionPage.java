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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DimensionType;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.statistics.TablePageStatistics;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.SortState;

/**
 * Encoded dimension page that include data and inverted index
 */
public class EncodedDimensionPage extends EncodedColumnPage {
  private IndexStorage indexStorage;
  private DimensionType dimensionType;

  public EncodedDimensionPage(int pageSize, byte[] encodedData, IndexStorage indexStorage,
      DimensionType dimensionType) {
    super(pageSize, encodedData);
    this.indexStorage = indexStorage;
    this.dimensionType = dimensionType;
    this.dataChunk2 = buildDataChunk2();
  }

  private int getTotalRowIdPageLengthInBytes() {
    return CarbonCommonConstants.INT_SIZE_IN_BYTE +
        indexStorage.getRowIdPageLengthInBytes() + indexStorage.getRowIdRlePageLengthInBytes();
  }

  @Override
  public int getSerializedSize() {
    int size = encodedData.length;
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      size += getTotalRowIdPageLengthInBytes();
    }
    if (indexStorage.getDataRlePageLengthInBytes() > 0) {
      size += indexStorage.getDataRlePageLengthInBytes();
    }
    return size;
  }

  @Override
  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(getSerializedSize());
    buffer.put(encodedData);
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      buffer.putInt(indexStorage.getRowIdPageLengthInBytes());
      short[] rowIdPage = (short[])indexStorage.getRowIdPage();
      for (short rowId : rowIdPage) {
        buffer.putShort(rowId);
      }
      if (indexStorage.getRowIdRlePageLengthInBytes() > 0) {
        short[] rowIdRlePage = (short[])indexStorage.getRowIdRlePage();
        for (short rowIdRle : rowIdRlePage) {
          buffer.putShort(rowIdRle);
        }
      }
    }
    if (indexStorage.getDataRlePageLengthInBytes() > 0) {
      short[] dataRlePage = (short[])indexStorage.getDataRlePage();
      for (short dataRle : dataRlePage) {
        buffer.putShort(dataRle);
      }
    }
    buffer.flip();
    return buffer;
  }

  @Override
  public DataChunk2 buildDataChunk2() {
    DataChunk2 dataChunk = new DataChunk2();
    dataChunk.min_max = new BlockletMinMaxIndex();
    dataChunk.setChunk_meta(CarbonMetadataUtil.getSnappyChunkCompressionMeta());
    dataChunk.setNumberOfRowsInpage(pageSize);
    List<Encoding> encodings = new ArrayList<Encoding>();
    dataChunk.setData_page_length(encodedData.length);
    if (dimensionType == DimensionType.GLOBAL_DICTIONARY ||
        dimensionType == DimensionType.DIRECT_DICTIONARY ||
        dimensionType == DimensionType.COMPLEX) {
      encodings.add(Encoding.DICTIONARY);
    }
    if (dimensionType == DimensionType.DIRECT_DICTIONARY) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
    }
    if (indexStorage.getDataRlePageLengthInBytes() > 0 ||
        dimensionType == DimensionType.GLOBAL_DICTIONARY) {
      dataChunk.setRle_page_length(indexStorage.getDataRlePageLengthInBytes());
      encodings.add(Encoding.RLE);
    }
    SortState sort = (indexStorage.getRowIdPageLengthInBytes() > 0) ?
        SortState.SORT_EXPLICIT : SortState.SORT_NATIVE;
    dataChunk.setSort_state(sort);
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      dataChunk.setRowid_page_length(getTotalRowIdPageLengthInBytes());
      encodings.add(Encoding.INVERTED_INDEX);
    }
    if (dimensionType == DimensionType.PLAIN_VALUE) {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(indexStorage.getMax())));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(
          TablePageStatistics.updateMinMaxForNoDictionary(indexStorage.getMin())));
    } else {
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(indexStorage.getMax()));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(indexStorage.getMin()));
    }
    dataChunk.setEncoders(encodings);
    return dataChunk;
  }

  public static DataChunk3 getDataChunk3(List<EncodedTablePage> encodedTablePageList,
      int columnIndex) throws IOException {
    List<DataChunk2> dataChunksList = new ArrayList<>(encodedTablePageList.size());
    for (EncodedTablePage encodedTablePage : encodedTablePageList) {
      dataChunksList.add(encodedTablePage.getDimension(columnIndex).getDataChunk2());
    }
    return CarbonMetadataUtil.getDataChunk3(dataChunksList);
  }

  public IndexStorage getIndexStorage() {
    return indexStorage;
  }

  public DimensionType getDimensionType() {
    return dimensionType;
  }
}