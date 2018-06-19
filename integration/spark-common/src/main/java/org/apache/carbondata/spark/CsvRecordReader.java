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

package org.apache.carbondata.spark;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.annotations.Since;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
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
import org.apache.carbondata.core.statusmanager.FileFormatProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.InputMetricsStats;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * scan csv file and filter on it
 */
@InterfaceStability.Evolving
@InterfaceAudience.Internal
@Since("1.4.1")
public class CsvRecordReader<T> extends AbstractRecordReader<T> {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      CsvRecordReader.class.getName());
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
  private FileSplit fileSplit;
  private Configuration hadoopConf;
  // the index is schema ordinal, the value is the csv ordinal
  private int[] schema2csvIdx;

  // filter
  private FilterExecuter filter;
  // the index is the dimension ordinal, the value is the schema ordinal
  private int[] filterColumn2SchemaIdx;
  private Object[] internalValues;
  private RowIntf internalRow;

  // output
  private CarbonColumn[] projection;
  // the index is the projection column ordinal, the value is the schema ordinal
  private int[] projectionColumn2SchemaIdx;
  private Object[] outputValues;
  private Object[] finalOutputValues;
  private InternalRow outputRow;

  // inputMetricsStats
  private InputMetricsStats inputMetricsStats;

  // scan
  private Reader reader;
  private CsvParser csvParser;

  public CsvRecordReader(QueryModel queryModel) {
    this.queryModel = queryModel;
  }

  public CsvRecordReader(QueryModel queryModel, InputMetricsStats inputMetricsStats) {
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

    // since the sequence of csv header, schema, carbon internal row, projection are different,
    // we need to init the column mappings
    initializedIdxMapping();

    // init filter
    if (null != queryModel.getFilterExpressionResolverTree()) {
      initializeFilter();
    }

    // init reading
    initializeCsvReader();
  }

  private void initializedIdxMapping() {
    carbonColumns =
        carbonTable.getCreateOrderColumn(carbonTable.getTableName()).toArray(new CarbonColumn[0]);
    // for schema to csv mapping
    schema2csvIdx = new int[carbonColumns.length];
    if (!carbonTable.getTableInfo().getFormatProperties().containsKey(
        FileFormatProperties.CSV.HEADER)) {
      // if no header specified, it means that they are the same
      LOGGER.info("no header specified, will take the schema from table as header");
      for (int i = 0; i < carbonColumns.length; i++) {
        schema2csvIdx[i] = i;
      }
    } else {
      String[] csvHeader = carbonTable.getTableInfo().getFormatProperties().get(
          FileFormatProperties.CSV.HEADER).split(",");
      for (int i = 0; i < csvHeader.length; i++) {
        boolean found = false;
        for (int j = 0; j < carbonColumns.length; j++) {
          if (StringUtils.strip(csvHeader[i]).equalsIgnoreCase(carbonColumns[j].getColName())) {
            schema2csvIdx[carbonColumns[j].getSchemaOrdinal()] = i;
            found = true;
            break;
          }
        }
        if (!found) {
          throw new RuntimeException(
              String.format("Can not find csv header '%s' in table fields", csvHeader[i]));
        }
      }
    }

    // for carbon internal row to schema mapping
    filterColumn2SchemaIdx = new int[carbonColumns.length];
    int filterIdx = 0;
    for (CarbonDimension dimension : carbonTable.getDimensions()) {
      filterColumn2SchemaIdx[filterIdx++] = dimension.getSchemaOrdinal();
    }
    for (CarbonMeasure measure : carbonTable.getMeasures()) {
      filterColumn2SchemaIdx[filterIdx++] = measure.getSchemaOrdinal();
    }

    // for projects to schema mapping
    projection = queryModel.getProjectionColumns();
    projectionColumn2SchemaIdx = new int[projection.length];

    for (int i = 0; i < projection.length; i++) {
      for (int j = 0; j < carbonColumns.length; j++) {
        if (projection[i].getColName().equals(carbonColumns[j].getColName())) {
          projectionColumn2SchemaIdx[i] = projection[i].getSchemaOrdinal();
          break;
        }
      }
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
    int bufferSize = Integer.parseInt(
        hadoopConf.get(CSVInputFormat.READ_BUFFER_SIZE, CSVInputFormat.READ_BUFFER_SIZE_DEFAULT));
    // note that here we read the whole file, not a split of the file
    FSDataInputStream fsStream = fs.open(file, bufferSize);
    reader = new InputStreamReader(fsStream, CarbonCommonConstants.DEFAULT_CHARSET);
    // use default csv settings first, then update it using user specified properties later
    CsvParserSettings settings = CSVInputFormat.extractCsvParserSettings(hadoopConf);
    initCsvSettings(settings);
    csvParser = new CsvParser(settings);
    csvParser.beginParsing(reader);

    outputSchema = new StructType(convertCarbonColumnSpark(projection));
  }

  /**
   * update the settings using properties from user
   */
  private void initCsvSettings(CsvParserSettings settings) {
    Map<String, String> csvProperties = carbonTable.getTableInfo().getFormatProperties();

    if (csvProperties.containsKey(FileFormatProperties.CSV.DELIMITER)) {
      settings.getFormat().setDelimiter(
          csvProperties.get(FileFormatProperties.CSV.DELIMITER).charAt(0));
    }

    if (csvProperties.containsKey(FileFormatProperties.CSV.COMMENT)) {
      settings.getFormat().setComment(
          csvProperties.get(FileFormatProperties.CSV.COMMENT).charAt(0));
    }

    if (csvProperties.containsKey(FileFormatProperties.CSV.QUOTE)) {
      settings.getFormat().setQuote(
          csvProperties.get(FileFormatProperties.CSV.QUOTE).charAt(0));
    }

    if (csvProperties.containsKey(FileFormatProperties.CSV.ESCAPE)) {
      settings.getFormat().setQuoteEscape(
          csvProperties.get(FileFormatProperties.CSV.ESCAPE).charAt(0));
    }

    if (csvProperties.containsKey(FileFormatProperties.CSV.SKIP_EMPTY_LINE)) {
      settings.setSkipEmptyLines(
          Boolean.parseBoolean(csvProperties.get(FileFormatProperties.CSV.SKIP_EMPTY_LINE)));
    }
  }

  private StructField[] convertCarbonColumnSpark(CarbonColumn[] columns) {
    return (StructField[]) new SparkDataTypeConverterImpl().convertCarbonSchemaToSparkSchema(
        columns);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isVectorReader) {
      return nextColumnarBatch();
    }

    return nextRow();
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
      putRowToSparkRow();
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
      org.apache.spark.sql.types.DataType t = outputSchema.apply(i).dataType();
      finalOutputValues[i] = convertToSparkValue(originValue, t);
    }
    outputRow = new GenericInternalRow(finalOutputValues);
  }

  private Object convertToSparkValue(Object originValue, org.apache.spark.sql.types.DataType t) {
    if (null == originValue) {
      return null;
    } else {
      String value = String.valueOf(originValue);
      if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
        return Boolean.parseBoolean(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
        return Byte.parseByte(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
        return Short.parseShort(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
        return Integer.parseInt(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
        return Long.parseLong(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
        return Float.parseFloat(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
        return Double.parseDouble(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
        return UTF8String.fromString(value);
      } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
        return Decimal.fromDecimal(value);
      } else if (t instanceof CalendarIntervalType) {
        return CalendarInterval.fromString(value);
      } else if (t instanceof org.apache.spark.sql.types.DateType) {
        return Integer.parseInt(value);
      } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
        return Long.parseLong(value);
      } else {
        return null;
      }
    }
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
      internalValues[i] = convertOriginValue2Carbon(
          csvLine[schema2csvIdx[filterColumn2SchemaIdx[i]]],
          carbonColumns[filterColumn2SchemaIdx[i]].getDataType());
    }

    internalRow.setValues(internalValues);
  }

  /**
   * Since output the sequence of columns is not the same as input, we need to adjust them
   */
  private void convertToOutputRow(String[] csvLine) {
    for (int i = 0; i < projection.length; i++) {
      outputValues[i] = csvLine[schema2csvIdx[projectionColumn2SchemaIdx[i]]];
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
