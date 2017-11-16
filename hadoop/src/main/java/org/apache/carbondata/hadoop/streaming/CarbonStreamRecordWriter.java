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

package org.apache.carbondata.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileHeader;
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
public class CarbonStreamRecordWriter extends RecordWriter<Void, Object> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonStreamRecordWriter.class.getName());

  // basic info
  private Configuration hadoopConf;
  private CarbonDataLoadConfiguration configuration;
  private CarbonTable carbonTable;
  private int maxRowNums;
  private int maxCacheSize;

  // parser and converter
  private RowParser rowParser;
  private BadRecordsLogger badRecordLogger;
  private RowConverter converter;
  private CarbonRow currentRow = new CarbonRow(null);

  // encoder
  private DataField[] dataFields;
  private BitSet nullBitSet;
  private boolean[] isNoDictionaryDimensionColumn;
  private int dimensionWithComplexCount;
  private int measureCount;
  private DataType[] measureDataTypes;
  private StreamBlockletWriter output = null;

  // data write
  private String segmentDir;
  private String fileName;
  private DataOutputStream outputStream;
  private boolean isFirstRow = true;
  private boolean hasException = false;

  CarbonStreamRecordWriter(TaskAttemptContext job) throws IOException {
    initialize(job);
  }

  private void initialize(TaskAttemptContext job) throws IOException {
    // set basic information
    hadoopConf = job.getConfiguration();
    CarbonLoadModel carbonLoadModel = CarbonStreamOutputFormat.getCarbonLoadModel(hadoopConf);
    if (carbonLoadModel == null) {
      throw new IOException(
          "CarbonStreamRecordWriter require configuration: mapreduce.output.carbon.load.model");
    }
    String segmentId = CarbonStreamOutputFormat.getSegmentId(hadoopConf);
    carbonLoadModel.setSegmentId(segmentId);
    carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
    int taskNo = TaskID.forName(hadoopConf.get("mapred.tip.id")).getId();
    carbonLoadModel.setTaskNo("" + taskNo);
    configuration = DataLoadProcessBuilder.createConfiguration(carbonLoadModel);
    maxRowNums = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_STREAM_BLOCKLET_ROW_NUMS,
        CarbonStreamOutputFormat.CARBON_STREAM_BLOCKLET_ROW_NUMS_DEFAULT) - 1;
    maxCacheSize = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_STREAM_CACHE_SIZE,
        CarbonStreamOutputFormat.CARBON_STREAM_CACHE_SIZE_DEFAULT);

    CarbonTablePath tablePath =
        CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier());
    segmentDir = tablePath.getSegmentDir("0", segmentId);
    fileName = CarbonTablePath.getCarbonDataFileName(0, taskNo, 0, 0, "0");
  }

  private void initializeAtFirstRow() throws IOException, InterruptedException {
    isFirstRow = false;

    // initialize metadata
    isNoDictionaryDimensionColumn =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    dimensionWithComplexCount = configuration.getDimensionCount();
    measureCount = configuration.getMeasureCount();
    dataFields = configuration.getDataFields();
    measureDataTypes = new DataType[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] =
          dataFields[dimensionWithComplexCount + i].getColumn().getDataType();
    }

    // initialize parser and converter
    rowParser = new RowParserImpl(dataFields, configuration);
    badRecordLogger = DataConverterProcessorStepImpl.createBadRecordLogger(configuration);
    converter = new RowConverterImpl(configuration.getDataFields(), configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    converter.initialize();

    // initialize encoder
    nullBitSet = new BitSet(dataFields.length);
    int rowBufferSize = hadoopConf.getInt(CarbonStreamOutputFormat.CARBON_ENCODER_ROW_BUFFER_SIZE,
        CarbonStreamOutputFormat.CARBON_ENCODER_ROW_BUFFER_SIZE_DEFAULT);
    output = new StreamBlockletWriter(maxCacheSize, maxRowNums, rowBufferSize);

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

  @Override public void write(Void key, Object value) throws IOException, InterruptedException {
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
    output.nextRow();
    byte[] b = nullBitSet.toByteArray();
    output.writeShort(b.length);
    if (b.length > 0) {
      output.writeBytes(b);
    }
    int dimCount = 0;
    Object columnValue;

    // primitive type dimension
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      columnValue = currentRow.getObject(dimCount);
      if (null != columnValue) {
        if (isNoDictionaryDimensionColumn[dimCount]) {
          byte[] col = (byte[]) columnValue;
          output.writeShort(col.length);
          output.writeBytes(col);
        } else {
          output.writeInt((int) columnValue);
        }
      }
    }
    // complex type dimension
    for (; dimCount < dimensionWithComplexCount; dimCount++) {
      columnValue = currentRow.getObject(dimCount);
      if (null != columnValue) {
        byte[] col = (byte[]) columnValue;
        output.writeShort(col.length);
        output.writeBytes(col);
      }
    }
    // measure
    DataType dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++) {
      columnValue = currentRow.getObject(dimCount + msrCount);
      if (null != columnValue) {
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN) {
          output.writeBoolean((boolean) columnValue);
        } else if (dataType == DataTypes.SHORT) {
          output.writeShort((short) columnValue);
        } else if (dataType == DataTypes.INT) {
          output.writeInt((int) columnValue);
        } else if (dataType == DataTypes.LONG) {
          output.writeLong((long) columnValue);
        } else if (dataType == DataTypes.DOUBLE) {
          output.writeDouble((double) columnValue);
        } else if (DataTypes.isDecimal(dataType)) {
          BigDecimal val = (BigDecimal) columnValue;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
          output.writeShort(bigDecimalInBytes.length);
          output.writeBytes(bigDecimalInBytes);
        } else {
          String msg =
              "unsupported data type:" + dataFields[dimCount + msrCount].getColumn().getDataType()
                  .getName();
          LOGGER.error(msg);
          throw new IOException(msg);
        }
      }
    }

    if (output.isFull()) {
      appendBlockletToDataFile();
    }
  }

  private void writeFileHeader() throws IOException {
    List<ColumnSchema> wrapperColumnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName()),
            carbonTable.getMeasureByTableName(carbonTable.getTableName()));
    int[] dimLensWithComplex = new int[wrapperColumnSchemaList.size()];
    for (int i = 0; i < dimLensWithComplex.length; i++) {
      dimLensWithComplex[i] = Integer.MAX_VALUE;
    }
    int[] dictionaryColumnCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList);
    List<Integer> cardinality = new ArrayList<>();
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
    if (output.getRowIndex() == -1) {
      return;
    }
    output.apppendBlocklet(outputStream);
    outputStream.flush();
    // reset data
    output.reset();
  }

  @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      // append remain buffer data
      if (!hasException && !isFirstRow) {
        appendBlockletToDataFile();
        converter.finish();
      }
    } finally {
      // close resource
      CarbonUtil.closeStreams(outputStream);
      if (output != null) {
        output.close();
      }
      if (badRecordLogger != null) {
        badRecordLogger.closeStreams();
      }
    }
  }

  public String getSegmentDir() {
    return segmentDir;
  }

  public String getFileName() {
    return fileName;
  }
}