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

package org.apache.carbondata.streaming.file;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.BlockletInfo;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.MutationType;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.DataLoadProcessBuilder;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.parser.RowParser;
import org.apache.carbondata.processing.loading.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.loading.steps.DataConverterProcessorStepImpl;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

/**
 * Stream record writer
 */
public class CarbonStreamRecordWriter extends RecordWriter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonStreamRecordWriter.class.getName());

  // basic info
  private Configuration hadoopConf;
  private CarbonDataLoadConfiguration configuration;
  private String segmentId;
  private int taskNo;
  private CarbonTable carbonTable;
  private int maxRowNums;
  private int maxCacheSize;

  // parser and converter
  private RowParser rowParser;
  private RowConverter converter;
  private CarbonRow currentRow = new CarbonRow(null);

  // encoder
  private DataField[] dataFields;
  private BitSet nullBitSet;
  private boolean[] isNoDictionaryDimensionColumn;
  private int dimensionWithComplexCount;
  private int measureCount;
  private int[] measureDataTypes;
  private BlockletOutputStream bos = null;

  // data write
  private String segmentDir;
  private String fileName;
  private DataOutputStream outputStream;
  private boolean isFirstRow = true;
  private boolean hasException = false;

  CarbonStreamRecordWriter(TaskAttemptContext job) throws IOException {
    initialize(job);
  }

  /**
   *
   */
  private void initialize(TaskAttemptContext job) throws IOException {
    // set basic information
    hadoopConf = job.getConfiguration();
    CarbonLoadModel carbonLoadModel = CarbonStreamOutputFormat.getCarbonLoadModel(hadoopConf);
    if (carbonLoadModel == null) {
      throw new IOException(
          "CarbonStreamRecordWriter require configuration: mapreduce.output.carbon.load.model");
    }
    segmentId = carbonLoadModel.getSegmentId();
    carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
    taskNo = TaskID.forName(hadoopConf.get("mapred.tip.id")).getId();
    carbonLoadModel.setTaskNo("" + taskNo);
    configuration = DataLoadProcessBuilder.createConfiguration(carbonLoadModel);
    maxRowNums = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_STREAM_BLOCKLET_ROW_NUMS,
        CarbonStreamOutputFormat.CARBON_STREAM_BLOCKLET_ROW_NUMS_DEFAULT) - 1;
    maxCacheSize = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_STREAM_CACHE_SIZE,
        CarbonStreamOutputFormat.CARBON_STREAM_CACHE_SIZE_DEFAULT);
    // try recover data file from fault for task at first
    tryRecoverFromFault();
  }

  /**
   * try recover data file from fault for task
   */
  private void tryRecoverFromFault() throws IOException {
    CarbonTablePath tablePath =
        CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier());
    segmentDir = tablePath.getSegmentDir("0", segmentId);
    fileName = CarbonTablePath.getCarbonDataFileName(0, taskNo, 0, 0, "0");
    String indexName = CarbonTablePath.getCarbonStreamIndexFileName();
    CarbonStreamRecordWriter.recoverDataFile(segmentDir, fileName, indexName);
  }

  public static void recoverDataFile(String segmentDir, String fileName, String indexName)
      throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
    String filePath = segmentDir + File.separator + fileName;
    CarbonFile file = FileFactory.getCarbonFile(filePath, fileType);
    String indexPath = segmentDir + File.separator + indexName;
    CarbonFile index = FileFactory.getCarbonFile(indexPath, fileType);
    if (file.exists() && index.exists()) {
      CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
      try {
        indexReader.openThriftReader(indexPath);
        while (indexReader.hasNext()) {
          BlockIndex blockIndex = indexReader.readBlockIndexInfo();
          if (blockIndex.getFile_name().equals(fileName)) {
            if (blockIndex.getFile_size() == 0) {
              file.delete();
            } else if (blockIndex.getFile_size() < file.getSize()) {
              FileFactory.truncateFile(filePath, fileType, blockIndex.getFile_size());
            }
          }
        }
      } finally {
        indexReader.closeThriftReader();
      }
    }
  }

  private void initializeAtFirstRow() throws IOException, InterruptedException {
    isFirstRow = false;

    // initialize metadata
    isNoDictionaryDimensionColumn =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    dimensionWithComplexCount = configuration.getDimensionCount();
    measureCount = configuration.getMeasureCount();
    dataFields = configuration.getDataFields();
    measureDataTypes = new int[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] =
          dataFields[dimensionWithComplexCount + i].getColumn().getDataType().getId();
    }

    // initialize parser and converter
    rowParser = new RowParserImpl(dataFields, configuration);
    BadRecordsLogger badRecordLogger =
        DataConverterProcessorStepImpl.createBadRecordLogger(configuration);
    converter = new RowConverterImpl(configuration.getDataFields(), configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    converter.initialize();

    // initialize encoder
    nullBitSet = new BitSet(dataFields.length);
    int rowBufferSize = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_ENCODER_ROW_BUFFER_SIZE,
        CarbonStreamOutputFormat.CARBON_ENCODER_ROW_BUFFER_SIZE_DEFAULT);
    bos = new BlockletOutputStream(maxCacheSize, maxRowNums, rowBufferSize);

    // initialize data writer
    String filePath = segmentDir + File.separator + fileName;
    FileFactory.FileType fileType = FileFactory.getFileType(filePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(filePath, fileType);
    if (carbonFile.exists()) {
      // if the file is existed, use the append api
      outputStream = FileFactory.getDataOutputStreamUsingAppend(filePath, fileType);
    } else {
      // IF the file is not existed, use the create api
      outputStream = FileFactory.getDataOutputStream(filePath, fileType);
      writeFileHeader();
    }
  }

  @Override public void write(Object key, Object value) throws IOException, InterruptedException {
    try {
      if (isFirstRow) {
        initializeAtFirstRow();
      }

      // parse and convert row
      currentRow.setData(rowParser.parseRow((Object[]) value));
      converter.convert(currentRow);

      // null bit set
      nullBitSet.clear();
      for (int i = 0; i < dataFields.length; i++) {
        if (null == currentRow.getObject(i)) {
          nullBitSet.set(i);
        }
      }
      bos.nextRow();
      bos.skip();
      bos.writeBytes(nullBitSet.toByteArray());
      int dimCount = 0;
      Object columnValue;

      // primitive type dimension
      for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
        columnValue = currentRow.getObject(dimCount);
        if (columnValue != null) {
          if (isNoDictionaryDimensionColumn[dimCount]) {
            byte[] col = (byte[]) columnValue;
            bos.writeShort(col.length);
            bos.writeBytes(col);
          } else {
            bos.writeInt((int) columnValue);
          }
        }
      }
      // complex type dimension
      for (; dimCount < dimensionWithComplexCount; dimCount++) {
        columnValue = currentRow.getObject(dimCount);
        if (columnValue != null) {
          byte[] col = (byte[]) columnValue;
          bos.writeShort(col.length);
          bos.writeBytes(col);
        }
      }
      // measure
      int dataType;
      for (int msrCount = 0; msrCount < measureCount; msrCount++) {
        columnValue = currentRow.getObject(dimCount + msrCount);
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN_TYPE_ID) {
          bos.writeBoolean((boolean) columnValue);
        } else if (dataType == DataTypes.SHORT_TYPE_ID) {
          bos.writeShort((Short) columnValue);
        } else if (dataType == DataTypes.INT_TYPE_ID) {
          bos.writeInt((Integer) columnValue);
        } else if (dataType == DataTypes.LONG_TYPE_ID) {
          bos.writeLong((Long) columnValue);
        } else if (dataType == DataTypes.DOUBLE_TYPE_ID) {
          bos.writeDouble((Double) columnValue);
        } else if (dataType == DataTypes.DECIMAL_TYPE_ID) {
          BigDecimal val = (BigDecimal) columnValue;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
          bos.writeInt(bigDecimalInBytes.length);
          bos.writeBytes(bigDecimalInBytes);
        } else {
          String msg =
              "unsupported data type:" + dataFields[dimCount + msrCount].getColumn().getDataType()
                  .getName();
          LOGGER.error(msg);
          throw new IllegalArgumentException(msg);
        }
      }
      bos.backFill();

      if (bos.isFull()) {
        appendBlockletToDataFile();
      }
    } catch (Exception ex) {
      hasException = true;
      LOGGER.error(ex, "Failed to write value");
      throw ex;
    }
  }

  private void writeFileHeader() throws IOException {
    List<Integer> cardinality = new ArrayList<>();
    int[] dimLensWithComplex = configuration.getCardinalityFinder().getCardinality();
    if (!configuration.isSortTable()) {
      for (int i = 0; i < dimLensWithComplex.length; i++) {
        if (dimLensWithComplex[i] != 0) {
          dimLensWithComplex[i] = Integer.MAX_VALUE;
        }
      }
    }
    List<ColumnSchema> wrapperColumnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getFactTableName()),
            carbonTable.getMeasureByTableName(carbonTable.getFactTableName()));
    int[] dictionaryColumnCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList);

    List<org.apache.carbondata.format.ColumnSchema> columnSchemaList = AbstractFactDataWriter
        .getColumnSchemaListAndCardinality(cardinality, dictionaryColumnCardinality,
            wrapperColumnSchemaList);
    FileHeader fileHeader =
        CarbonMetadataUtil.getFileHeader(true, columnSchemaList, System.currentTimeMillis());
    fileHeader.setIs_footer_present(false);
    fileHeader.setIs_splitable(true);
    fileHeader.setSync_marker(CarbonStreamOutputFormat.CARBON_SYNC_MARKER);
    outputStream.write(CarbonUtil.getByteArray(fileHeader));
  }

  /**
   * write a blocklet to file
   */
  private void appendBlockletToDataFile() throws IOException {
    if (bos.getRowIndex() == -1) {
      return;
    }
    bos.apppendBlocklet(outputStream);
    outputStream.flush();
    // reset data
    bos.reset();
  }

  @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      // append remain buffer data
      if (!hasException) {
        appendBlockletToDataFile();
      }
    } finally {
      // close resource
      CarbonUtil.closeStreams(outputStream);
      bos.close();
    }
  }

  static class BlockletOutputStream {
    private byte[] buffer;
    private int maxSize;
    private int maxRowNum;
    private int rowSize;
    private int count = 0;
    private int skip = 0;
    private int rowIndex = -1;
    private Compressor compressor = CompressorFactory.getInstance().getCompressor();

    BlockletOutputStream(int maxSize, int maxRowNum, int rowSize) {
      buffer = new byte[maxSize];
      this.maxSize = maxSize;
      this.maxRowNum = maxRowNum;
      this.rowSize = rowSize;
    }

    private void ensureCapacity(int space) {
      int newcount = space + count;
      if (newcount > buffer.length) {
        byte[] newbuf = new byte[Math.max(newcount, buffer.length + rowSize)];
        System.arraycopy(buffer, 0, newbuf, 0, count);
        buffer = newbuf;
      }
    }

    void reset() {
      count = 0;
      skip = 0;
      rowIndex = -1;
    }

    public String toString() {
      return new String(buffer, 0, count);
    }

    void close() {
    }

    byte[] getBytes() {
      return buffer;
    }

    int getCount() {
      return count;
    }

    int getRowIndex() {
      return rowIndex;
    }

    void skip() {
      ensureCapacity(4);
      skip = count;
      count += 4;
    }

    void backFill() {
      int val = count - skip - 4;
      buffer[skip + 3] = (byte) (val);
      buffer[skip + 2] = (byte) (val >>> 8);
      buffer[skip + 1] = (byte) (val >>> 16);
      buffer[skip] = (byte) (val >>> 24);
    }

    void nextRow() {
      rowIndex++;
    }

    boolean isFull() {
      return rowIndex == maxRowNum || count >= maxSize;
    }

    void writeBoolean(boolean val) {
      ensureCapacity(1);
      buffer[count] = (byte) (val ? 1 : 0);
      count += 1;
    }

    void writeShort(int val) {
      ensureCapacity(2);
      buffer[count + 1] = (byte) (val);
      buffer[count] = (byte) (val >>> 8);
      count += 2;
    }

    void writeInt(int val) {
      ensureCapacity(4);
      buffer[count + 3] = (byte) (val);
      buffer[count + 2] = (byte) (val >>> 8);
      buffer[count + 1] = (byte) (val >>> 16);
      buffer[count] = (byte) (val >>> 24);
      count += 4;
    }

    void writeLong(long val) {
      ensureCapacity(8);
      buffer[count + 7] = (byte) (val);
      buffer[count + 6] = (byte) (val >>> 8);
      buffer[count + 5] = (byte) (val >>> 16);
      buffer[count + 4] = (byte) (val >>> 24);
      buffer[count + 3] = (byte) (val >>> 32);
      buffer[count + 2] = (byte) (val >>> 40);
      buffer[count + 1] = (byte) (val >>> 48);
      buffer[count] = (byte) (val >>> 56);
      count += 8;
    }

    void writeDouble(double val) {
      writeLong(Double.doubleToLongBits(val));
    }

    void writeBytes(byte[] b) {
      writeBytes(b, 0, b.length);
    }

    void writeBytes(byte[] b, int off, int len) {
      ensureCapacity(len);
      System.arraycopy(b, off, buffer, count, len);
      count += len;
    }

    void apppendBlocklet(DataOutputStream outputStream) throws IOException {
      BlockletInfo blockletInfo = new BlockletInfo();
      blockletInfo.setNum_rows(getRowIndex() + 1);
      BlockletHeader blockletHeader = new BlockletHeader();
      byte[] compressed = compressor.compressByte(getBytes(), getCount());
      blockletHeader.setBlocklet_length(compressed.length);
      blockletHeader.setMutation(MutationType.INSERT);
      blockletHeader.setBlocklet_info(blockletInfo);
      byte[] headerBytes = CarbonUtil.getByteArray(blockletHeader);
      outputStream.write(headerBytes);
      outputStream.write(compressed);
      outputStream.write(CarbonStreamOutputFormat.CARBON_SYNC_MARKER);
    }
  }
}