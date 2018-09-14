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

package org.apache.carbondata.stream;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.InputMetricsStats;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.streaming.CarbonStreamInputFormat;
import org.apache.carbondata.streaming.StreamBlockletReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

/**
 * Stream record reader
 */
public class CarbonStreamRecordReader extends RecordReader<Void, Object> {
  // vector reader
  private boolean isVectorReader;

  // metadata
  private CarbonTable carbonTable;
  private CarbonColumn[] storageColumns;
  private boolean[] isRequired;
  private DataType[] measureDataTypes;
  private int dimensionCount;
  private int measureCount;

  // input
  private FileSplit fileSplit;
  private Configuration hadoopConf;
  private StreamBlockletReader input;
  private boolean isFirstRow = true;
  private QueryModel model;

  // decode data
  private BitSet allNonNull;
  private boolean[] isNoDictColumn;
  private DirectDictionaryGenerator[] directDictionaryGenerators;
  private CacheProvider cacheProvider;
  private Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache;
  private GenericQueryType[] queryTypes;
  private String compressorName;

  // vectorized reader
  private StructType outputSchema;
  private CarbonVectorProxy vectorProxy;
  private boolean isFinished = false;

  // filter
  private FilterExecuter filter;
  private boolean[] isFilterRequired;
  private Object[] filterValues;
  private RowIntf filterRow;
  private int[] filterMap;

  // output
  private CarbonColumn[] projection;
  private boolean[] isProjectionRequired;
  private int[] projectionMap;
  private Object[] outputValues;
  private InternalRow outputRow;

  // empty project, null filter
  private boolean skipScanData;

  // return raw row for handoff
  private boolean useRawRow = false;

  // InputMetricsStats
  private InputMetricsStats inputMetricsStats;

  public CarbonStreamRecordReader(boolean isVectorReader, InputMetricsStats inputMetricsStats,
      QueryModel mdl, boolean useRawRow) {
    this.isVectorReader = isVectorReader;
    this.inputMetricsStats = inputMetricsStats;
    this.model = mdl;
    this.useRawRow = useRawRow;

  }
  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // input
    if (split instanceof CarbonInputSplit) {
      fileSplit = (CarbonInputSplit) split;
    } else if (split instanceof CarbonMultiBlockSplit) {
      fileSplit = ((CarbonMultiBlockSplit) split).getAllSplits().get(0);
    } else {
      fileSplit = (FileSplit) split;
    }

    // metadata
    hadoopConf = context.getConfiguration();
    if (model == null) {
      CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
      model = format.createQueryModel(split, context);
    }
    carbonTable = model.getTable();
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    dimensionCount = dimensions.size();
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    measureCount = measures.size();
    List<CarbonColumn> carbonColumnList =
        carbonTable.getStreamStorageOrderColumn(carbonTable.getTableName());
    storageColumns = carbonColumnList.toArray(new CarbonColumn[carbonColumnList.size()]);
    isNoDictColumn = CarbonDataProcessorUtil.getNoDictionaryMapping(storageColumns);
    directDictionaryGenerators = new DirectDictionaryGenerator[storageColumns.length];
    for (int i = 0; i < storageColumns.length; i++) {
      if (storageColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(storageColumns[i].getDataType());
      }
    }
    measureDataTypes = new DataType[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] = storageColumns[dimensionCount + i].getDataType();
    }

    // decode data
    allNonNull = new BitSet(storageColumns.length);
    projection = model.getProjectionColumns();

    isRequired = new boolean[storageColumns.length];
    boolean[] isFiltlerDimensions = model.getIsFilterDimensions();
    boolean[] isFiltlerMeasures = model.getIsFilterMeasures();
    isFilterRequired = new boolean[storageColumns.length];
    filterMap = new int[storageColumns.length];
    for (int i = 0; i < storageColumns.length; i++) {
      if (storageColumns[i].isDimension()) {
        if (isFiltlerDimensions[storageColumns[i].getOrdinal()]) {
          isRequired[i] = true;
          isFilterRequired[i] = true;
          filterMap[i] = storageColumns[i].getOrdinal();
        }
      } else {
        if (isFiltlerMeasures[storageColumns[i].getOrdinal()]) {
          isRequired[i] = true;
          isFilterRequired[i] = true;
          filterMap[i] = carbonTable.getDimensionOrdinalMax() + storageColumns[i].getOrdinal();
        }
      }
    }

    isProjectionRequired = new boolean[storageColumns.length];
    projectionMap = new int[storageColumns.length];
    for (int j = 0; j < projection.length; j++) {
      for (int i = 0; i < storageColumns.length; i++) {
        if (storageColumns[i].getColName().equals(projection[j].getColName())) {
          isRequired[i] = true;
          isProjectionRequired[i] = true;
          projectionMap[i] = j;
          break;
        }
      }
    }

    // initialize filter
    if (null != model.getFilterExpressionResolverTree()) {
      initializeFilter();
    } else if (projection.length == 0) {
      skipScanData = true;
    }

  }

  private void initializeFilter() {

    List<ColumnSchema> wrapperColumnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName()),
            carbonTable.getMeasureByTableName(carbonTable.getTableName()));
    int[] dimLensWithComplex = new int[wrapperColumnSchemaList.size()];
    for (int i = 0; i < dimLensWithComplex.length; i++) {
      dimLensWithComplex[i] = Integer.MAX_VALUE;
    }

    int[] dictionaryColumnCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList);
    SegmentProperties segmentProperties =
        new SegmentProperties(wrapperColumnSchemaList, dictionaryColumnCardinality);
    Map<Integer, GenericQueryType> complexDimensionInfoMap = new HashMap<>();

    FilterResolverIntf resolverIntf = model.getFilterExpressionResolverTree();
    filter = FilterUtil.getFilterExecuterTree(resolverIntf, segmentProperties,
        complexDimensionInfoMap);
    // for row filter, we need update column index
    FilterUtil.updateIndexOfColumnExpression(resolverIntf.getFilterExpression(),
        carbonTable.getDimensionOrdinalMax());

  }

  private byte[] getSyncMarker(String filePath) throws IOException {
    CarbonHeaderReader headerReader = new CarbonHeaderReader(filePath);
    FileHeader header = headerReader.readHeader();
    // legacy store does not have this member
    if (header.isSetCompressor_name()) {
      compressorName = header.getCompressor_name();
    } else {
      compressorName = CompressorFactory.SupportedCompressor.SNAPPY.getName();
    }
    return header.getSync_marker();
  }

  private void initializeAtFirstRow() throws IOException {
    filterValues = new Object[carbonTable.getDimensionOrdinalMax() + measureCount];
    filterRow = new RowImpl();
    filterRow.setValues(filterValues);

    outputValues = new Object[projection.length];
    outputRow = new GenericInternalRow(outputValues);

    Path file = fileSplit.getPath();

    byte[] syncMarker = getSyncMarker(file.toString());

    FileSystem fs = file.getFileSystem(hadoopConf);

    int bufferSize = Integer.parseInt(hadoopConf.get(CarbonStreamInputFormat.READ_BUFFER_SIZE,
        CarbonStreamInputFormat.READ_BUFFER_SIZE_DEFAULT));

    FSDataInputStream fileIn = fs.open(file, bufferSize);
    fileIn.seek(fileSplit.getStart());
    input = new StreamBlockletReader(syncMarker, fileIn, fileSplit.getLength(),
        fileSplit.getStart() == 0, compressorName);

    cacheProvider = CacheProvider.getInstance();
    cache = cacheProvider.createCache(CacheType.FORWARD_DICTIONARY);
    queryTypes = CarbonStreamInputFormat.getComplexDimensions(carbonTable, storageColumns, cache);

    outputSchema = new StructType((StructField[])
        DataTypeUtil.getDataTypeConverter().convertCarbonSchemaToSparkSchema(projection));
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isFirstRow) {
      isFirstRow = false;
      initializeAtFirstRow();
    }
    if (isFinished) {
      return false;
    }

    if (isVectorReader) {
      return nextColumnarBatch();
    }

    return nextRow();
  }

  /**
   * for vector reader, check next columnar batch
   */
  private boolean nextColumnarBatch() throws IOException {
    boolean hasNext;
    boolean scanMore = false;
    do {
      // move to the next blocklet
      hasNext = input.nextBlocklet();
      if (hasNext) {
        // read blocklet header
        BlockletHeader header = input.readBlockletHeader();
        if (isScanRequired(header)) {
          scanMore = !scanBlockletAndFillVector(header);
        } else {
          input.skipBlockletData(true);
          scanMore = true;
        }
      } else {
        isFinished = true;
        scanMore = false;
      }
    } while (scanMore);
    return hasNext;
  }

  /**
   * check next Row
   */
  private boolean nextRow() throws IOException {
    // read row one by one
    try {
      boolean hasNext;
      boolean scanMore = false;
      do {
        hasNext = input.hasNext();
        if (hasNext) {
          if (skipScanData) {
            input.nextRow();
            scanMore = false;
          } else {
            if (useRawRow) {
              // read raw row for streaming handoff which does not require decode raw row
              readRawRowFromStream();
            } else {
              readRowFromStream();
            }
            if (null != filter) {
              scanMore = !filter.applyFilter(filterRow, carbonTable.getDimensionOrdinalMax());
            } else {
              scanMore = false;
            }
          }
        } else {
          if (input.nextBlocklet()) {
            BlockletHeader header = input.readBlockletHeader();
            if (isScanRequired(header)) {
              if (skipScanData) {
                input.skipBlockletData(false);
              } else {
                input.readBlockletData(header);
              }
            } else {
              input.skipBlockletData(true);
            }
            scanMore = true;
          } else {
            isFinished = true;
            scanMore = false;
          }
        }
      } while (scanMore);
      return hasNext;
    } catch (FilterUnsupportedException e) {
      throw new IOException("Failed to filter row in detail reader", e);
    }
  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

    @Override public Object getCurrentValue() throws IOException, InterruptedException {
        if (isVectorReader) {
            int value = vectorProxy.numRows();
            if (inputMetricsStats != null) {
                inputMetricsStats.incrementRecordRead((long) value);
            }

            return vectorProxy.getColumnarBatch();
        }

    if (inputMetricsStats != null) {
      inputMetricsStats.incrementRecordRead(1L);
    }

    return outputRow;
  }

  private boolean isScanRequired(BlockletHeader header) {
    if (filter != null && header.getBlocklet_index() != null) {
      BlockletMinMaxIndex minMaxIndex = CarbonMetadataUtil.convertExternalMinMaxIndex(
          header.getBlocklet_index().getMin_max_index());
      if (minMaxIndex != null) {
        BitSet bitSet = filter
            .isScanRequired(minMaxIndex.getMaxValues(), minMaxIndex.getMinValues(),
                minMaxIndex.getIsMinMaxSet());
        if (bitSet.isEmpty()) {
          return false;
        } else {
          return true;
        }
      }
    }
    return true;
  }

    private boolean scanBlockletAndFillVector(BlockletHeader header) throws IOException {
        // if filter is null and output projection is empty, use the row number of blocklet header
        if (skipScanData) {
            int rowNums = header.getBlocklet_info().getNum_rows();
            vectorProxy= new CarbonVectorProxy(MemoryMode.OFF_HEAP,outputSchema,rowNums);
            vectorProxy.setNumRows(rowNums);
            input.skipBlockletData(true);
            return rowNums > 0;
        }

        input.readBlockletData(header);
        vectorProxy= new CarbonVectorProxy(MemoryMode.OFF_HEAP,outputSchema,input.getRowNums());
        int rowNum = 0;
        if (null == filter) {
            while (input.hasNext()) {
                readRowFromStream();
                putRowToColumnBatch(rowNum++);
            }
        } else {
            try {
                while (input.hasNext()) {
                    readRowFromStream();
                    if (filter.applyFilter(filterRow, carbonTable.getDimensionOrdinalMax())) {
                        putRowToColumnBatch(rowNum++);
                    }
                }
            } catch (FilterUnsupportedException e) {
                throw new IOException("Failed to filter row in vector reader", e);
            }
        }
        vectorProxy.setNumRows(rowNum);
        return rowNum > 0;
    }

  private void readRowFromStream() {
    input.nextRow();
    short nullLen = input.readShort();
    BitSet nullBitSet = allNonNull;
    if (nullLen > 0) {
      nullBitSet = BitSet.valueOf(input.readBytes(nullLen));
    }
    int colCount = 0;
    // primitive type dimension
    for (; colCount < isNoDictColumn.length; colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        if (isNoDictColumn[colCount]) {
          int v = input.readShort();
          if (isRequired[colCount]) {
            byte[] b = input.readBytes(v);
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = b;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] =
                  DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(b,
                      storageColumns[colCount].getDataType());
            }
          } else {
            input.skipBytes(v);
          }
        } else if (null != directDictionaryGenerators[colCount]) {
          if (isRequired[colCount]) {
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = input.copy(4);
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] =
                  directDictionaryGenerators[colCount].getValueFromSurrogate(input.readInt());
            } else {
              input.skipBytes(4);
            }
          } else {
            input.skipBytes(4);
          }
        } else {
          if (isRequired[colCount]) {
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = input.copy(4);
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = input.readInt();
            } else {
              input.skipBytes(4);
            }
          } else {
            input.skipBytes(4);
          }
        }
      }
    }
    // complex type dimension
    for (; colCount < dimensionCount; colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = null;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        short v = input.readShort();
        if (isRequired[colCount]) {
          byte[] b = input.readBytes(v);
          if (isFilterRequired[colCount]) {
            filterValues[filterMap[colCount]] = b;
          }
          if (isProjectionRequired[colCount]) {
            outputValues[projectionMap[colCount]] = queryTypes[colCount]
                .getDataBasedOnDataType(ByteBuffer.wrap(b));
          }
        } else {
          input.skipBytes(v);
        }
      }
    }
    // measure
    DataType dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = null;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN) {
          if (isRequired[colCount]) {
            boolean v = input.readBoolean();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(1);
          }
        } else if (dataType == DataTypes.SHORT) {
          if (isRequired[colCount]) {
            short v = input.readShort();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(2);
          }
        } else if (dataType == DataTypes.INT) {
          if (isRequired[colCount]) {
            int v = input.readInt();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(4);
          }
        } else if (dataType == DataTypes.LONG) {
          if (isRequired[colCount]) {
            long v = input.readLong();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(8);
          }
        } else if (dataType == DataTypes.DOUBLE) {
          if (isRequired[colCount]) {
            double v = input.readDouble();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(8);
          }
        } else if (DataTypes.isDecimal(dataType)) {
          int len = input.readShort();
          if (isRequired[colCount]) {
            BigDecimal v = DataTypeUtil.byteToBigDecimal(input.readBytes(len));
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] =
                  DataTypeUtil.getDataTypeConverter().convertFromBigDecimalToDecimal(v);
            }
          } else {
            input.skipBytes(len);
          }
        }
      }
    }
  }

  private void readRawRowFromStream() {
    input.nextRow();
    short nullLen = input.readShort();
    BitSet nullBitSet = allNonNull;
    if (nullLen > 0) {
      nullBitSet = BitSet.valueOf(input.readBytes(nullLen));
    }
    int colCount = 0;
    // primitive type dimension
    for (; colCount < isNoDictColumn.length; colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      } else {
        if (isNoDictColumn[colCount]) {
          int v = input.readShort();
          outputValues[colCount] = input.readBytes(v);
        } else {
          outputValues[colCount] = input.readInt();
        }
      }
    }
    // complex type dimension
    for (; colCount < dimensionCount; colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = null;
      } else {
        short v = input.readShort();
        outputValues[colCount] = input.readBytes(v);
      }
    }
    // measure
    DataType dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = null;
      } else {
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN) {
          outputValues[colCount] = input.readBoolean();
        } else if (dataType == DataTypes.SHORT) {
          outputValues[colCount] = input.readShort();
        } else if (dataType == DataTypes.INT) {
          outputValues[colCount] = input.readInt();
        } else if (dataType == DataTypes.LONG) {
          outputValues[colCount] = input.readLong();
        } else if (dataType == DataTypes.DOUBLE) {
          outputValues[colCount] = input.readDouble();
        } else if (DataTypes.isDecimal(dataType)) {
          int len = input.readShort();
          outputValues[colCount] = DataTypeUtil.byteToBigDecimal(input.readBytes(len));
        }
      }
    }
  }

    private void putRowToColumnBatch(int rowId) {
        for (int i = 0; i < projection.length; i++) {
            Object value = outputValues[i];
            vectorProxy.getColumnVector(i).putRowToColumnBatch(rowId,value,i);

        }
    }

    @Override public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override public void close() throws IOException {
        if (null != input) {
            input.close();
        }
        if (null != vectorProxy) {
            vectorProxy.close();
        }
    }
}
