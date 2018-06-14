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

package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.carbondata.processing.loading.csvinput.CSVInputFormat.READ_BUFFER_SIZE;
import static org.apache.carbondata.processing.loading.csvinput.CSVInputFormat.READ_BUFFER_SIZE_DEFAULT;
import static org.apache.carbondata.processing.loading.csvinput.CSVInputFormat.extractCsvParserSettings;

/**
 * scan csv file and filter on it
 */
public class CarbonCsvRecordReader<T> extends AbstractRecordReader<T> {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      CarbonCsvRecordReader.class.getName());
  private static final int MAX_BATCH_SIZE = 32000;

  // vector reader
  private boolean isVectorReader;
  private ColumnarBatch columnarBatch;
  private StructType outputSchema;

  // metadata
  private CarbonTable carbonTable;
  private CarbonColumn[] carbonColumns;
  // input
  private QueryModel queryModel;
  FileSplit fileSplit;
  private Configuration hadoopConf;

  // filter
  private FilterExecuter filter;
  // column idx for filter values
  private int[] filterColumnIdx;
  private Object[] internalValues;
  private RowIntf internalRow;

  // output
  private CarbonColumn[] projection;
  // column idx for filter values
  private int[] projectionColumnIdx;
  private Object[] outputValues;
  private Object[] finalOutputValues;
  private InternalRow outputRow;

  // inputMetricsStats
  private InputMetricsStats inputMetricsStats;

  // scan
  private Reader reader;
  private CsvParser csvParser;

  public CarbonCsvRecordReader(QueryModel queryModel) {
    this.queryModel = queryModel;
  }

  public CarbonCsvRecordReader(QueryModel queryModel, InputMetricsStats inputMetricsStats) {
    this(queryModel);
    this.inputMetricsStats = inputMetricsStats;
  }

  public boolean isVectorReader() {
    return isVectorReader;
  }

  public void setVectorReader(boolean vectorReader) {
    isVectorReader = vectorReader;
  }

  public void setQueryModel(QueryModel queryModel) {
    this.queryModel = queryModel;
  }

  public void setInputMetricsStats(InputMetricsStats inputMetricsStats) {
    this.inputMetricsStats = inputMetricsStats;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (split instanceof CarbonInputSplit) {
      fileSplit = (CarbonInputSplit) split;
    } else if (split instanceof CarbonMultiBlockSplit) {
      fileSplit = ((CarbonMultiBlockSplit) split).getAllSplits().get(0);
    } else {
      fileSplit = (FileSplit) split;
    }

    hadoopConf = context.getConfiguration();
    if (queryModel == null) {
      CarbonTableInputFormat inputFormat = new CarbonTableInputFormat<Object>();
      queryModel = inputFormat.createQueryModel(split, context);
    }

    carbonTable = queryModel.getTable();
    // todo: here we read csv use the table schema as header, will use user specified later
    carbonColumns =
        carbonTable.getCreateOrderColumn(carbonTable.getTableName()).toArray(new CarbonColumn[0]);

    filterColumnIdx = new int[carbonColumns.length];
    int filterIdx = 0;
    for (CarbonDimension dimension : carbonTable.getDimensions()) {
      filterColumnIdx[filterIdx++] = dimension.getSchemaOrdinal();
    }
    for (CarbonMeasure measure : carbonTable.getMeasures()) {
      filterColumnIdx[filterIdx++] = measure.getSchemaOrdinal();
    }

    projection = queryModel.getProjectionColumns();
    projectionColumnIdx = new int[projection.length];

    for (int i = 0; i < projection.length; i++) {
      for (int j = 0; j < carbonColumns.length; j++) {
        if (projection[i].getColName().equals(carbonColumns[j].getColName())) {
          projectionColumnIdx[i] = j;
          break;
        }
      }
    }

    // init filter
    if (null != queryModel.getFilterExpressionResolverTree()) {
      initializeFilter();
    }

    // init reading
    initializeCsvReader();
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

    FilterResolverIntf resolverIntf = queryModel.getFilterExpressionResolverTree();
    filter = FilterUtil.getFilterExecuterTree(resolverIntf, segmentProperties,
        complexDimensionInfoMap);
    // for row filter, we need update column index
    FilterUtil.updateIndexOfColumnExpression(resolverIntf.getFilterExpression(),
        carbonTable.getDimensionOrdinalMax());
  }

  private void initializeCsvReader() throws IOException {
    internalValues = new Object[carbonColumns.length];
    internalRow = new RowImpl();
    internalRow.setValues(internalValues);

    outputValues = new Object[projection.length];
    finalOutputValues = new Object[projection.length];
    outputRow = new GenericInternalRow(outputValues);

    Path file = fileSplit.getPath();
    FileSystem fs = file.getFileSystem(hadoopConf);
    int bufferSize = Integer.parseInt(hadoopConf.get(READ_BUFFER_SIZE, READ_BUFFER_SIZE_DEFAULT));
    // note that here we read the whole file, not a split of the file
    FSDataInputStream fsStream = fs.open(file, bufferSize);
    reader = new InputStreamReader(fsStream, CarbonCommonConstants.DEFAULT_CHARSET);
    // todo: use default csv settings here, will use user specified later
    CsvParserSettings settings = extractCsvParserSettings(hadoopConf);
    csvParser = new CsvParser(settings);
    csvParser.beginParsing(reader);

    outputSchema = new StructType(convertCarbonSchemaToSparkSchema(projection));
  }

  private StructField[] convertCarbonSchemaToSparkSchema(CarbonColumn[] carbonColumns) {
    StructField[] fields = new StructField[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      CarbonColumn carbonColumn = carbonColumns[i];
      if (carbonColumn.isDimension()) {
        if (carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(carbonColumn.getDataType());
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(generator.getReturnType()), true, null);
        } else if (!carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else if (carbonColumn.isComplex()) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.INT), true, null);
        }
      } else if (carbonColumn.isMeasure()) {
        DataType dataType = carbonColumn.getDataType();
        if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(dataType), true, null);
        } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(dataType)) {
          CarbonMeasure measure = (CarbonMeasure) carbonColumn;
          fields[i] = new StructField(carbonColumn.getColName(),
              new DecimalType(measure.getPrecision(), measure.getScale()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE), true, null);
        }
      }
    }
    return fields;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isVectorReader) {
      return nextColumnarBatch();
    }

    return nextRow();
  }

  private org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType) {
    if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.STRING) {
      return DataTypes.StringType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT) {
      return DataTypes.ShortType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
      return DataTypes.IntegerType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
      return DataTypes.LongType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
      return DataTypes.DoubleType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
      return DataTypes.BooleanType;
    } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(carbonDataType)) {
      return DataTypes.createDecimalType();
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
      return DataTypes.TimestampType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      return DataTypes.DateType;
    } else {
      return null;
    }
  }

  private boolean nextColumnarBatch() throws IOException {
    return scanAndFillBatch();
  }

  private boolean scanAndFillBatch() throws IOException {
    columnarBatch = ColumnarBatch.allocate(outputSchema, MemoryMode.OFF_HEAP, MAX_BATCH_SIZE);
    int rowNum = 0;
    if (null == filter) {
      while (readRowFromFile() && rowNum < MAX_BATCH_SIZE) {
        putRowToColumnBatch(rowNum++);
      }
    } else {
      try {
        while (readRowFromFile() && rowNum < MAX_BATCH_SIZE) {
          if (filter.applyFilter(internalRow, carbonTable.getDimensionOrdinalMax())) {
            putRowToColumnBatch(rowNum++);
          }
        }
      } catch (FilterUnsupportedException e) {
        throw new IOException("Failed to filter row in CarbonCsvRecordReader", e);
      }
    }
    columnarBatch.setNumRows(rowNum);
    return rowNum > 0;
  }

  private void putRowToColumnBatch(int rowId) {
    for (int i = 0; i < projection.length; i++) {
      Object originValue = outputValues[i];
      ColumnVector col = columnarBatch.column(i);
      org.apache.spark.sql.types.DataType t = col.dataType();
      if (null == originValue) {
        col.putNull(rowId);
      } else {
        String value = String.valueOf(originValue);
        if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
          col.putBoolean(rowId, Boolean.parseBoolean(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
          col.putByte(rowId, Byte.parseByte(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
          col.putShort(rowId, Short.parseShort(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
          col.putInt(rowId, Integer.parseInt(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
          col.putLong(rowId, Long.parseLong(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
          col.putFloat(rowId, Float.parseFloat(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
          col.putDouble(rowId, Double.parseDouble(value));
        } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
          UTF8String v = UTF8String.fromString(value);
          col.putByteArray(rowId, v.getBytes());
        } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
          DecimalType dt = (DecimalType)t;
          Decimal d = Decimal.fromDecimal(value);
          if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
            col.putInt(rowId, (int)d.toUnscaledLong());
          } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
            col.putLong(rowId, d.toUnscaledLong());
          } else {
            final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
            byte[] bytes = integer.toByteArray();
            col.putByteArray(rowId, bytes, 0, bytes.length);
          }
        } else if (t instanceof CalendarIntervalType) {
          CalendarInterval c = CalendarInterval.fromString(value);
          col.getChildColumn(0).putInt(rowId, c.months);
          col.getChildColumn(1).putLong(rowId, c.microseconds);
        } else if (t instanceof org.apache.spark.sql.types.DateType) {
          col.putInt(rowId, Integer.parseInt(value));
        } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
          col.putLong(rowId, Long.parseLong(value));
        }
      }
    }
  }

  private boolean nextRow() throws IOException {
    if (csvParser == null) {
      return false;
    }

    if (!readRowFromFile()) {
      return false;
    }

    if (null == filter) {
      return true;
    } else {
      try {
        boolean scanMore;
        do {
          scanMore = !filter.applyFilter(internalRow, carbonTable.getDimensionOrdinalMax());
          if (!scanMore) {
            putRowToSparkRow();
            return true;
          }
        } while (readRowFromFile());
        // if we read the end of file and still need scanMore, it means that there is no row
        return false;
      } catch (FilterUnsupportedException e) {
        throw new IOException("Failed to filter row in CarbonCsvRecordReader", e);
      }
    }
  }

  private void putRowToSparkRow() {
    for (int i = 0; i < projection.length; i++) {
      Object originValue = outputValues[i];
      org.apache.spark.sql.types.DataType t =
          convertCarbonToSparkDataType(projection[i].getDataType());
      if (null == originValue) {
        finalOutputValues[i] = null;
      } else {
        String value = String.valueOf(originValue);
        if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
          finalOutputValues[i] =  Boolean.parseBoolean(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
          finalOutputValues[i] = Byte.parseByte(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
          finalOutputValues[i] = Short.parseShort(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
          finalOutputValues[i] = Integer.parseInt(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
          finalOutputValues[i] = Long.parseLong(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
          finalOutputValues[i] = Float.parseFloat(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
          finalOutputValues[i] = Double.parseDouble(value);
        } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
          finalOutputValues[i] = UTF8String.fromString(value);
        } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
          Decimal d = Decimal.fromDecimal(value);
          finalOutputValues[i] = d;
        } else if (t instanceof CalendarIntervalType) {
          CalendarInterval c = CalendarInterval.fromString(value);
          finalOutputValues[i] = c;
        } else if (t instanceof org.apache.spark.sql.types.DateType) {
          finalOutputValues[i] = Integer.parseInt(value);
        } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
          finalOutputValues[i] = Long.parseLong(value);
        }
      }
    }
    outputRow = new GenericInternalRow(finalOutputValues);
  }

  /**
   * read from csv file and convert to internal row
   * todo: prune with project/filter
   * @return false, if it comes to an end
   */
  private boolean readRowFromFile() {
    String[] parsedOut = csvParser.parseNext();
    if (parsedOut == null) {
      return false;
    } else {
      convertToInternalRow(parsedOut);
      convertToOutputRow(parsedOut);
      return true;
    }
  }

  /**
   * convert origin csv string row to carbondata internal row.
   * The row will be used to do filter on it. Note that the dimensions are at the head
   * while measures are at the end, so we need to adjust the values.
   */
  private void convertToInternalRow(String[] csvLine) {
    for (int i = 0; i < carbonColumns.length; i++) {
      internalValues[i] = convertOriginValue2Carbon(csvLine[filterColumnIdx[i]],
          carbonColumns[filterColumnIdx[i]].getDataType());
    }

    internalRow.setValues(internalValues);
  }

  /**
   * Since output the sequence of columns is not the same as input, we need to adjust them
   */
  private void convertToOutputRow(String[] csvLine) {
    for (int i = 0; i < projection.length; i++) {
      outputValues[i] = csvLine[projectionColumnIdx[i]];
    }
  }

  private Object convertOriginValue2Carbon(String value,
      org.apache.carbondata.core.metadata.datatype.DataType t) {
    if (null == value) {
      return null;
    } else {
      if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
        return Boolean.parseBoolean(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.BYTE) {
        return Byte.parseByte(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT) {
        return Short.parseShort(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
        return Integer.parseInt(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
        return Long.parseLong(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.FLOAT) {
        return Float.parseFloat(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
        return Double.parseDouble(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.STRING) {
        UTF8String v = UTF8String.fromString(value);
        return v.getBytes();
      } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(t)) {
        Decimal d = Decimal.fromDecimal(value);
        if (d.precision() <= Decimal.MAX_INT_DIGITS()) {
          return  (int)d.toUnscaledLong();
        } else if (d.precision() <= Decimal.MAX_LONG_DIGITS()) {
          return d.toUnscaledLong();
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          return integer.toByteArray();
        }
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
        return Integer.parseInt(value);
      } else if (t == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
        return Long.parseLong(value);
      } else {
        throw new RuntimeException("Unsupport datatype in CarbonCsvRecordReader");
      }
    }
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public T getCurrentValue() throws IOException, InterruptedException {
    if (isVectorReader) {
      int value = columnarBatch.numValidRows();
      if (inputMetricsStats != null) {
        inputMetricsStats.incrementRecordRead((long) value);
      }
      return (T) columnarBatch;
    } else {
      if (inputMetricsStats != null) {
        inputMetricsStats.incrementRecordRead(1L);
      }
      return (T) outputRow;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    try {
      if (reader != null) {
        reader.close();
      }
      if (csvParser != null) {
        csvParser.stopParsing();
      }
      if (columnarBatch != null) {
        columnarBatch.close();
      }
    } finally {
      reader = null;
      csvParser = null;
    }
  }
}
