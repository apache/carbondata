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

package org.apache.carbondata.tool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.BlockletInfo3;
import org.apache.carbondata.format.DataChunk3;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.LocalDictionaryChunk;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.FILE_SEPARATOR;

/**
 * Contains information extracted from a .carbondata file
 */
class DataFile {
  private CarbonFile dataFile;

  // file full path
  private String filePath;

  // reader for this file
  private FileReader fileReader;

  // shard name
  private String shardName;

  // part id
  private String partNo;

  private long fileSizeInBytes;
  private long footerSizeInBytes;

  // size in bytes of each blocklet
  private LinkedList<Long> blockletSizeInBytes = new LinkedList<>();

  // data size in bytes of each column in each blocklet
  private LinkedList<LinkedList<Long>> columnDataSizeInBytes = new LinkedList<>();
  // meta size (DataChunk3) in bytes of each column in each blocklet
  private LinkedList<LinkedList<Long>> columnMetaSizeInBytes = new LinkedList<>();

  private FileHeader header;
  private FileFooter3 footer;
  private long footerOffset;
  private List<ColumnSchema> schema;
  private List<Blocklet> blocklets;

  DataFile(CarbonFile dataFile) {
    this.dataFile = dataFile;
    this.filePath = dataFile.getPath();
    this.fileSizeInBytes = dataFile.getSize();
  }

  void collectAllMeta() throws IOException {
    FileHeader header = null;
    FileFooter3 footer = null;
    try {
      header = readHeader();
    } catch (IOException e) {
      throw new IOException("failed to read header in " + dataFile.getPath(), e);
    }
    if (header.isSetSync_marker()) {
      // if sync_marker is set, it is a streaming format file
      throw new UnsupportedOperationException("streaming file is not supported");
    }
    try {
      footer = readFooter();
    } catch (IOException e) {
      throw new IOException("failed to read footer in " + dataFile.getPath(), e);
    }

    this.header = header;
    this.footer = footer;
    String filePath = dataFile.getPath();
    // folder path that contains this file
    String fileName = filePath.substring(filePath.lastIndexOf(FILE_SEPARATOR));
    this.shardName = CarbonTablePath.getShardName(fileName);
    this.partNo = CarbonTablePath.DataFileUtil.getPartNo(fileName);

    // calculate blocklet size and column size
    // first calculate the header size, it equals the offset of first
    // column chunk in first blocklet
    long headerSizeInBytes = footer.blocklet_info_list3.get(0).column_data_chunks_offsets.get(0);
    long previousOffset = headerSizeInBytes;
    for (BlockletInfo3 blockletInfo3 : footer.blocklet_info_list3) {
      // calculate blocklet size in bytes
      long blockletOffset = blockletInfo3.column_data_chunks_offsets.get(0);
      blockletSizeInBytes.add(blockletOffset - previousOffset);
      previousOffset = blockletOffset;

      // calculate column size in bytes for each column
      LinkedList<Long> columnDataSize = new LinkedList<>();
      LinkedList<Long> columnMetaSize = new LinkedList<>();
      long previousChunkOffset = blockletInfo3.column_data_chunks_offsets.get(0);
      for (int i = 0; i < schema.size(); i++) {
        columnDataSize.add(blockletInfo3.column_data_chunks_offsets.get(i) - previousChunkOffset);
        columnMetaSize.add(blockletInfo3.column_data_chunks_length.get(i).longValue());
        previousChunkOffset = blockletInfo3.column_data_chunks_offsets.get(i);
      }
      // last column chunk data size
      columnDataSize.add(fileSizeInBytes - footerSizeInBytes - previousChunkOffset);
      columnDataSize.removeFirst();
      this.columnDataSizeInBytes.add(columnDataSize);
      this.columnMetaSizeInBytes.add(columnMetaSize);

    }
    // last blocklet size
    blockletSizeInBytes.add(
        fileSizeInBytes - footerSizeInBytes - headerSizeInBytes - previousOffset);
    this.blockletSizeInBytes.removeFirst();

    assert (blockletSizeInBytes.size() == getNumBlocklets());
  }

  FileHeader readHeader() throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFile.getPath());
    this.schema = reader.readSchema();
    return reader.readHeader();
  }

  FileFooter3 readFooter() throws IOException {
    this.fileReader = FileFactory.getFileHolder(FileFactory.getFileType(dataFile.getPath()));
    ByteBuffer buffer = fileReader.readByteBuffer(FileFactory.getUpdatedFilePath(
        dataFile.getPath()), dataFile.getSize() - 8, 8);
    this.footerOffset = buffer.getLong();
    this.footerSizeInBytes = this.fileSizeInBytes - footerOffset;
    CarbonFooterReaderV3 footerReader =
        new CarbonFooterReaderV3(dataFile.getAbsolutePath(), footerOffset);
    return footerReader.readFooterVersion3();
  }

  String getFilePath() {
    return filePath;
  }

  String getShardName() {
    return shardName;
  }

  String getPartNo() {
    return partNo;
  }

  FileHeader getHeader() {
    return header;
  }

  FileFooter3 getFooter() {
    return footer;
  }

  List<ColumnSchema> getSchema() {
    return schema;
  }

  FileReader getFileReader() {
    return fileReader;
  }

  long getFooterOffset() {
    return footerOffset;
  }

  int getNumBlocklet() {
    return blockletSizeInBytes.size();
  }

  long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  int getColumnIndex(String columnName) {
    List<ColumnSchema> columns = getSchema();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    throw new IllegalArgumentException(columnName + " not found");
  }

  ColumnSchema getColumn(String columnName) {
    List<ColumnSchema> columns = getSchema();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equalsIgnoreCase(columnName)) {
        return columns.get(i);
      }
    }
    throw new IllegalArgumentException(columnName + " not found");
  }

  int numDimensions() {
    int numDimensions = 0;
    List<ColumnSchema> columns = getSchema();
    for (ColumnSchema column : columns) {
      if (column.isDimensionColumn()) {
        numDimensions++;
      }
    }
    return numDimensions;
  }

  private int getNumBlocklets() {
    return footer.blocklet_info_list3.size();
  }

  Long getBlockletSizeInBytes(int blockletId) {
    if (blockletId < 0 || blockletId >= getNumBlocklets()) {
      throw new IllegalArgumentException("invalid blockletId: " + blockletId);
    }
    return blockletSizeInBytes.get(blockletId);
  }

  Long getColumnDataSizeInBytes(int blockletId, int columnIndex) {
    if (blockletId < 0 || blockletId >= getNumBlocklets()) {
      throw new IllegalArgumentException("invalid blockletId: " + blockletId);
    }
    LinkedList<Long> columnSize = this.columnDataSizeInBytes.get(blockletId);
    if (columnIndex >= columnSize.size()) {
      throw new IllegalArgumentException("invalid columnIndex: " + columnIndex);
    }
    return columnSize.get(columnIndex);
  }

  Long getColumnMetaSizeInBytes(int blockletId, int columnIndex) {
    if (blockletId < 0 || blockletId >= getNumBlocklets()) {
      throw new IllegalArgumentException("invalid blockletId: " + blockletId);
    }
    LinkedList<Long> columnSize = this.columnMetaSizeInBytes.get(blockletId);
    if (columnIndex >= columnSize.size()) {
      throw new IllegalArgumentException("invalid columnIndex: " + columnIndex);
    }
    return columnSize.get(columnIndex);
  }

  void initAllBlockletStats(String columnName) throws IOException, MemoryException {
    int columnIndex = -1;
    ColumnSchema column = null;
    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getColumnName().equalsIgnoreCase(columnName)) {
        columnIndex = i;
        column = schema.get(i);
      }
    }
    if (column == null) {
      throw new IllegalArgumentException("column name " + columnName + " not exist");
    }
    List<Blocklet> blocklets = new LinkedList<>();
    for (int blockletId = 0; blockletId < footer.blocklet_index_list.size(); blockletId++) {
      blocklets.add(new Blocklet(this, blockletId, column, columnIndex, footer));
    }
    this.blocklets = blocklets;
  }

  List<Blocklet> getAllBlocklets() {
    return blocklets;
  }

  // Column chunk in one blocklet
  class ColumnChunk {

    ColumnSchema column;

    // true if local dictionary is used in this column chunk
    boolean localDict;

    // average length in bytes for all pages in this column chunk
    long avgPageLengthInBytes;

    // the size in bytes of local dictionary for this column chunk
    long blocketletDictionarySize;

    // the number of entry in local dictionary for this column chunk
    long blockletDictionaryEntries;

    // min/max stats of this column chunk
    byte[] min, max;

    // percentage of min/max comparing to min/max scope collected in all blocklets
    // they are set after calculation in DataSummary
    double minPercentage, maxPercentage;

    /**
     * Constructor
     * @param blockletInfo blocklet info which this column chunk belongs to
     * @param index blocklet index which this column chunk belongs to
     * @param column column schema of this column chunk
     * @param columnIndex column index of this column chunk
     */
    ColumnChunk(BlockletInfo3 blockletInfo, BlockletIndex index, ColumnSchema column,
        int columnIndex) throws IOException, MemoryException {
      this.column = column;
      min = index.min_max_index.min_values.get(columnIndex).array();
      max = index.min_max_index.max_values.get(columnIndex).array();

      // read the column chunk metadata: DataChunk3
      ByteBuffer buffer = fileReader.readByteBuffer(
          filePath, blockletInfo.column_data_chunks_offsets.get(columnIndex),
          blockletInfo.column_data_chunks_length.get(columnIndex));
      DataChunk3 dataChunk = CarbonUtil.readDataChunk3(new ByteArrayInputStream(buffer.array()));
      this.localDict = dataChunk.isSetLocal_dictionary();
      if (this.localDict) {
        String compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
            dataChunk.data_chunk_list.get(0).chunk_meta);
        LocalDictionaryChunk dictionaryChunk = dataChunk.local_dictionary;
        Compressor comp = CompressorFactory.getInstance().getCompressor(compressorName);
        CarbonDictionary dictionary = DimensionRawColumnChunk.getDictionary(dictionaryChunk, comp);
        blockletDictionaryEntries = dictionary.getDictionaryActualSize();
        blocketletDictionarySize = dataChunk.local_dictionary.dictionary_data.array().length;
      }
      long pageLength = 0;
      for (int size : dataChunk.page_length) {
        pageLength += size;
      }
      avgPageLengthInBytes = pageLength / dataChunk.page_length.size();
    }

    void setMinPercentage(double minPercentage) {
      this.minPercentage = minPercentage;
    }

    void setMaxPercentage(double maxPercentage) {
      this.maxPercentage = maxPercentage;
    }

    double getMinPercentage() {
      return minPercentage;
    }

    double getMaxPercentage() {
      return maxPercentage;
    }

    DataType getDataType() {
      return column.getDataType();
    }

    byte[] min(byte[] minValue) {
      if (minValue == null) {
        return min;
      } else {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(minValue, min) < 0 ? minValue : min;
      }
    }

    byte[] max(byte[] maxValue) {
      if (maxValue == null) {
        return max;
      } else {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(maxValue, max) > 0 ? maxValue : max;
      }
    }
  }

  class Blocklet {
    DataFile file;
    int id;
    ColumnChunk columnChunk;

    Blocklet(DataFile file, int blockletId, ColumnSchema column, int columnIndex,
        FileFooter3 footer) throws IOException, MemoryException {
      this.file = file;
      this.id = blockletId;
      BlockletIndex index = footer.blocklet_index_list.get(blockletId);
      BlockletInfo3 info = footer.blocklet_info_list3.get(blockletId);
      this.columnChunk = new ColumnChunk(info, index, column, columnIndex);
    }

    String getShardName() {
      return file.getShardName();
    }

    ColumnChunk getColumnChunk() {
      return columnChunk;
    }

    // compute and set min and max percentage for this blocklet
    void computePercentage(byte[] shardMin, byte[] shardMax) {
      double min = computePercentage(columnChunk.min, shardMin, shardMax, columnChunk.column);
      double max = computePercentage(columnChunk.max, shardMin, shardMax, columnChunk.column);
      columnChunk.setMinPercentage(min);
      columnChunk.setMaxPercentage(max);
    }

    /**
     * Calculate data percentage in [min, max] scope based on data type
     * @param data data to calculate the percentage
     * @param min min value
     * @param max max value
     * @param column column schema including data type
     * @return result
     */
    private double computePercentage(byte[] data, byte[] min, byte[] max, ColumnSchema column) {
      if (column.getDataType() == DataTypes.STRING) {
        // for string, we do not calculate
        return 0;
      } else if (DataTypes.isDecimal(column.getDataType())) {
        BigDecimal minValue = DataTypeUtil.byteToBigDecimal(min);
        BigDecimal dataValue = DataTypeUtil.byteToBigDecimal(data).subtract(minValue);
        BigDecimal factorValue = DataTypeUtil.byteToBigDecimal(max).subtract(minValue);
        return dataValue.divide(factorValue).doubleValue();
      }
      double dataValue, minValue, factorValue;
      if (column.getDataType() == DataTypes.SHORT) {
        minValue = ByteUtil.toShort(min, 0);
        dataValue = ByteUtil.toShort(data, 0) - minValue;
        factorValue = ByteUtil.toShort(max, 0) - ByteUtil.toShort(min, 0);
      } else if (column.getDataType() == DataTypes.INT) {
        if (column.isSortColumn()) {
          minValue = ByteUtil.toXorInt(min, 0, min.length);
          dataValue = ByteUtil.toXorInt(data, 0, data.length) - minValue;
          factorValue =
              ByteUtil.toXorInt(max, 0, max.length) - ByteUtil.toXorInt(min, 0, min.length);
        } else {
          minValue = ByteUtil.toLong(min, 0, min.length);
          dataValue = ByteUtil.toLong(data, 0, data.length) - minValue;
          factorValue =
              ByteUtil.toLong(max, 0, max.length) - ByteUtil.toLong(min, 0, min.length);
        }
      } else if (column.getDataType() == DataTypes.LONG) {
        minValue = ByteUtil.toLong(min, 0, min.length);
        dataValue = ByteUtil.toLong(data, 0, data.length) - minValue;
        factorValue =
            ByteUtil.toLong(max, 0, max.length) - ByteUtil.toLong(min, 0, min.length);
      } else if (column.getDataType() == DataTypes.DATE) {
        minValue = ByteUtil.toInt(min, 0, min.length);
        dataValue = ByteUtil.toInt(data, 0, data.length) - minValue;
        factorValue =
            ByteUtil.toInt(max, 0, max.length) - ByteUtil.toInt(min, 0, min.length);
      } else if (column.getDataType() == DataTypes.TIMESTAMP) {
        minValue = ByteUtil.toLong(min, 0, min.length);
        dataValue = ByteUtil.toLong(data, 0, data.length) - minValue;
        factorValue =
            ByteUtil.toLong(max, 0, max.length) - ByteUtil.toLong(min, 0, min.length);
      } else if (column.getDataType() == DataTypes.DOUBLE) {
        minValue = ByteUtil.toDouble(min, 0, min.length);
        dataValue = ByteUtil.toDouble(data, 0, data.length) - minValue;
        factorValue =
            ByteUtil.toDouble(max, 0, max.length) - ByteUtil.toDouble(min, 0, min.length);
      } else {
        throw new UnsupportedOperationException("data type: " + column.getDataType());
      }

      if (factorValue == 0d) {
        return 1;
      }
      return dataValue / factorValue;
    }
  }

}