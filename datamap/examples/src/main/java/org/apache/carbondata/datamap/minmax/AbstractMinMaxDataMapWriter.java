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

package org.apache.carbondata.datamap.minmax;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.KeyPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * We will record the min & max value for each index column in each blocklet.
 * Since the size of index is quite small, we will combine the index for all index columns
 * in one file.
 */
public abstract class AbstractMinMaxDataMapWriter extends DataMapWriter {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      AbstractMinMaxDataMapWriter.class.getName());

  private ColumnPageStatsCollector[] indexColumnMinMaxCollectors;
  protected int currentBlockletId;
  private String currentIndexFile;
  private DataOutputStream currentIndexFileOutStream;

  public AbstractMinMaxDataMapWriter(String tablePath, String dataMapName,
      List<CarbonColumn> indexColumns, Segment segment, String shardName) throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    initStatsCollector();
    initDataMapFile();
  }

  private void initStatsCollector() {
    indexColumnMinMaxCollectors = new ColumnPageStatsCollector[indexColumns.size()];
    for (int i = 0; i < indexColumns.size(); i++) {
      if (indexColumns.get(i).isDimension()) {
        indexColumnMinMaxCollectors[i] = KeyPageStatsCollector.newInstance(DataTypes.BYTE_ARRAY);
      } else if (indexColumns.get(i).isMeasure()) {
        indexColumnMinMaxCollectors[i] = PrimitivePageStatsCollector.newInstance(
            indexColumns.get(i).getDataType());
      } else {
        throw new UnsupportedOperationException(
            "MinMax datamap only supports dimension and measure");
      }
    }
  }

  private void initDataMapFile() throws IOException {
    if (!FileFactory.isFileExist(dataMapPath) &&
        !FileFactory.mkdirs(dataMapPath, FileFactory.getFileType(dataMapPath))) {
      throw new IOException("Failed to create directory " + dataMapPath);
    }

    try {
      currentIndexFile = MinMaxIndexDataMap.getIndexFile(dataMapPath,
          MinMaxIndexHolder.MINMAX_INDEX_PREFFIX + indexColumns.size());
      FileFactory.createNewFile(currentIndexFile, FileFactory.getFileType(currentIndexFile));
      currentIndexFileOutStream = FileFactory.getDataOutputStream(currentIndexFile,
          FileFactory.getFileType(currentIndexFile));
    } catch (IOException e) {
      CarbonUtil.closeStreams(currentIndexFileOutStream);
      LOGGER.error(e, "Failed to init datamap index file");
      throw e;
    }
  }

  protected void resetBlockletLevelMinMax() {
    for (int i = 0; i < indexColumns.size(); i++) {
      indexColumnMinMaxCollectors[i].getPageStats().clear();
    }
  }

  @Override
  public void onBlockStart(String blockId) {
  }

  @Override
  public void onBlockEnd(String blockId) {
  }

  @Override public void onBlockletStart(int blockletId) {
  }

  @Override public void onBlockletEnd(int blockletId) {
    flushMinMaxIndexFile();
    currentBlockletId++;
  }

  @Override
  public void onPageAdded(int blockletId, int pageId, int pageSize, ColumnPage[] pages) {
    // as an example, we don't use page-level min-max generated by native carbondata here, we get
    // the min-max by comparing each row
    for (int rowId = 0; rowId < pageSize; rowId++) {
      for (int colIdx = 0; colIdx < indexColumns.size(); colIdx++) {
        Object originValue = pages[colIdx].getData(rowId);
        updateBlockletMinMax(colIdx, originValue);
      }
    }
  }

  protected void updateBlockletMinMax(int indexColIdx, Object value) {
    if (null == value) {
      indexColumnMinMaxCollectors[indexColIdx].updateNull(0);
      return;
    }

    DataType dataType = indexColumns.get(indexColIdx).getDataType();
    if (indexColumns.get(indexColIdx).isMeasure()) {
      if (DataTypes.BOOLEAN == dataType) {
        indexColumnMinMaxCollectors[indexColIdx].update(
            BooleanConvert.boolean2Byte((boolean) value));
      } else if (DataTypes.SHORT == dataType) {
        indexColumnMinMaxCollectors[indexColIdx].update((short) value);
      } else if (DataTypes.INT == dataType) {
        indexColumnMinMaxCollectors[indexColIdx].update((int) value);
      } else if (DataTypes.LONG == dataType) {
        indexColumnMinMaxCollectors[indexColIdx].update((long) value);
      } else if (DataTypes.DOUBLE == dataType) {
        indexColumnMinMaxCollectors[indexColIdx].update((double) value);
      } else if (DataTypes.isDecimal(dataType)) {
        indexColumnMinMaxCollectors[indexColIdx].update((BigDecimal) value);
      } else {
        throw new UnsupportedOperationException("unsupported data type " + dataType);
      }
    } else {
      // While pruning for query, we want to reuse the pruning method from carbon, so here for
      // dictionary columns, we need to store the mdk value in the minmax index.
      // For direct generating, the input value is already MDK; For late building, the input value
      // is surrogate key, so we need to handle it here.
      if (indexColumns.get(indexColIdx).hasEncoding(Encoding.DICTIONARY)) {
        indexColumnMinMaxCollectors[indexColIdx].update(convertDictValueToMdk(indexColIdx, value));
      } else {
        byte[] plainValue = convertNonDicValueToPlain(indexColIdx, (byte[]) value);
        indexColumnMinMaxCollectors[indexColIdx].update(plainValue);
      }
    }
  }

  protected abstract byte[] convertDictValueToMdk(int indexColIdx, Object value);

  protected abstract byte[] convertNonDicValueToPlain(int indexColIdx, byte[] value);

  private void logMinMaxInfo(int indexColId) {
    StringBuilder sb = new StringBuilder("flush blockletId->").append(currentBlockletId)
        .append(", column->").append(indexColumns.get(indexColId).getColName())
        .append(", dataType->").append(indexColumns.get(indexColId).getDataType().getName());
    Object min = indexColumnMinMaxCollectors[indexColId].getPageStats().getMin();
    Object max = indexColumnMinMaxCollectors[indexColId].getPageStats().getMax();
    if (indexColumns.get(indexColId).isDimension()) {
      sb.append(", min->")
          .append(new String((byte[]) min, CarbonCommonConstants.DEFAULT_CHARSET_CLASS))
          .append(", max->")
          .append(new String((byte[]) max, CarbonCommonConstants.DEFAULT_CHARSET_CLASS));
    } else {
      sb.append(", min->").append(min)
          .append(", max->").append(max);
    }
    LOGGER.debug(sb.toString());
  }

  /**
   * Write the data to a file.
   */
  protected void flushMinMaxIndexFile() {
    try {
      MinMaxIndexHolder minMaxIndexHolder = new MinMaxIndexHolder(indexColumns.size());
      minMaxIndexHolder.setBlockletId(currentBlockletId);
      for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
        if (LOGGER.isDebugEnabled()) {
          logMinMaxInfo(indexColId);
        }
        minMaxIndexHolder.setMinValueAtPos(indexColId, CarbonUtil.getValueAsBytes(
            indexColumnMinMaxCollectors[indexColId].getPageStats().getDataType(),
            indexColumnMinMaxCollectors[indexColId].getPageStats().getMin()));
        minMaxIndexHolder.setMaxValueAtPos(indexColId, CarbonUtil.getValueAsBytes(
            indexColumnMinMaxCollectors[indexColId].getPageStats().getDataType(),
            indexColumnMinMaxCollectors[indexColId].getPageStats().getMax()));
      }
      minMaxIndexHolder.write(currentIndexFileOutStream);
      currentIndexFileOutStream.flush();
    } catch (IOException e) {
      LOGGER.error(e, "Failed to flush minmax index to file " + currentIndexFile);
      releaseResource();
      throw new RuntimeException(e);
    } finally {
      resetBlockletLevelMinMax();
    }
  }

  @Override
  public void finish() throws IOException {
    if (!isWritingFinished()) {
      releaseResource();
      setWritingFinished(true);
    }
  }

  protected void releaseResource() {
    CarbonUtil.closeStreams(currentIndexFileOutStream);
  }
}
