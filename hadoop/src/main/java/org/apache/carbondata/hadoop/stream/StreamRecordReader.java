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

package org.apache.carbondata.hadoop.stream;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Stream row record reader
 */
public class StreamRecordReader extends RecordReader<Void, Object> {

  // metadata
  protected CarbonTable carbonTable;
  private CarbonColumn[] storageColumns;
  private boolean[] isRequired;
  private boolean[] dimensionsIsVarcharTypeMap;
  private DataType[] measureDataTypes;
  private int dimensionCount;
  private int measureCount;

  // input
  private FileSplit fileSplit;
  private Configuration hadoopConf;
  protected StreamBlockletReader input;
  protected boolean isFirstRow = true;
  protected QueryModel model;

  // decode data
  private BitSet allNonNull;
  private boolean[] isNoDictColumn;
  private DirectDictionaryGenerator[] directDictionaryGenerators;
  private GenericQueryType[] queryTypes;
  private String compressorName;

  // vectorized reader
  protected boolean isFinished = false;

  // filter
  protected FilterExecuter filter;
  private boolean[] isFilterRequired;
  private Object[] filterValues;
  protected RowIntf filterRow;
  private int[] filterMap;

  // output
  protected CarbonColumn[] projection;
  private boolean[] isProjectionRequired;
  private int[] projectionMap;
  protected Object[] outputValues;

  // empty project, null filter
  protected boolean skipScanData;

  // return raw row for handoff
  private boolean useRawRow = false;

  public StreamRecordReader(QueryModel mdl, boolean useRawRow) {
    this.model = mdl;
    this.useRawRow = useRawRow;

  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException {
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    dimensionCount = dimensions.size();
    List<CarbonMeasure> measures = carbonTable.getVisibleMeasures();
    measureCount = measures.size();
    List<CarbonColumn> carbonColumnList = carbonTable.getStreamStorageOrderColumn();
    storageColumns = carbonColumnList.toArray(new CarbonColumn[carbonColumnList.size()]);
    isNoDictColumn = CarbonDataProcessorUtil.getNoDictionaryMapping(storageColumns);
    directDictionaryGenerators = new DirectDictionaryGenerator[storageColumns.length];
    for (int i = 0; i < storageColumns.length; i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      if (storageColumns[i].getDataType() == DataTypes.DATE) {
        directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(storageColumns[i].getDataType());
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3497
    dimensionsIsVarcharTypeMap = new boolean[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      dimensionsIsVarcharTypeMap[i] = storageColumns[i].getDataType() == DataTypes.VARCHAR;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    if (null != model.getIndexFilter()) {
      initializeFilter();
    } else if (projection.length == 0) {
      skipScanData = true;
    }

  }

  private void initializeFilter() {
    List<ColumnSchema> wrapperColumnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getVisibleDimensions(), carbonTable.getVisibleMeasures());

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    SegmentProperties segmentProperties = new SegmentProperties(wrapperColumnSchemaList);
    Map<Integer, GenericQueryType> complexDimensionInfoMap = new HashMap<>();

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    FilterResolverIntf resolverIntf = model.getIndexFilter().getResolver();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3611
    filter = FilterUtil.getFilterExecuterTree(
        resolverIntf, segmentProperties, complexDimensionInfoMap, true);
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
      compressorName = CompressorFactory.NativeSupportedCompressor.SNAPPY.getName();
    }
    return header.getSync_marker();
  }

  protected void initializeAtFirstRow() throws IOException {
    filterValues = new Object[carbonTable.getDimensionOrdinalMax() + measureCount];
    filterRow = new RowImpl();
    filterRow.setValues(filterValues);

    outputValues = new Object[projection.length];

    Path file = fileSplit.getPath();

    byte[] syncMarker = getSyncMarker(file.toString());

    FileSystem fs = file.getFileSystem(hadoopConf);

    int bufferSize = Integer.parseInt(hadoopConf.get(CarbonStreamInputFormat.READ_BUFFER_SIZE,
        CarbonStreamInputFormat.READ_BUFFER_SIZE_DEFAULT));

    FSDataInputStream fileIn = fs.open(file, bufferSize);
    fileIn.seek(fileSplit.getStart());
    input = new StreamBlockletReader(syncMarker, fileIn, fileSplit.getLength(),
        fileSplit.getStart() == 0, compressorName);

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    queryTypes = CarbonStreamInputFormat.getComplexDimensions(storageColumns);
  }

  /**
   * check next Row
   */
  protected boolean nextRow() throws IOException {
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

  @Override
  public boolean nextKeyValue() throws IOException {
    if (isFirstRow) {
      isFirstRow = false;
      initializeAtFirstRow();
    }
    if (isFinished) {
      return false;
    }

    return nextRow();
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public Object getCurrentValue() {
    return outputValues;
  }

  protected boolean isScanRequired(BlockletHeader header) {
    if (filter != null && header.getBlocklet_index() != null) {
      BlockletMinMaxIndex minMaxIndex = CarbonMetadataUtil
          .convertExternalMinMaxIndex(header.getBlocklet_index().getMin_max_index());
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

  protected void readRowFromStream() {
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
          int v = 0;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3497
          if (dimensionsIsVarcharTypeMap[colCount]) {
            v = input.readInt();
          } else {
            v = input.readShort();
          }
          if (isRequired[colCount]) {
            byte[] b = input.readBytes(v);
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = b;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = DataTypeUtil
                  .getDataBasedOnDataTypeForNoDictionaryColumn(b,
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
            outputValues[projectionMap[colCount]] =
                queryTypes[colCount].getDataBasedOnDataType(ByteBuffer.wrap(b));
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
          int v = 0;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3497
          if (dimensionsIsVarcharTypeMap[colCount]) {
            v = input.readInt();
          } else {
            v = input.readShort();
          }
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

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (null != input) {
      input.close();
    }
  }
}
