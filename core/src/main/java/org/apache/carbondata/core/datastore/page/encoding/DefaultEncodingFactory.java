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

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.DecimalColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaFloatingCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveFloatingCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.ComplexDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.DirectDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.HighCardDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Default factory will select encoding base on column page data type and statistics
 */
public class DefaultEncodingFactory extends EncodingFactory {

  private static final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
  private static final int THREE_BYTES_MIN = -THREE_BYTES_MAX - 1;

  private static final boolean newWay = false;

  private static EncodingFactory encodingFactory = new DefaultEncodingFactory();

  public static EncodingFactory getInstance() {
    // TODO: make it configurable after added new encodingFactory
    return encodingFactory;
  }

  @Override
  public ColumnPageEncoder createEncoder(TableSpec.ColumnSpec columnSpec, ColumnPage inputPage) {
    // TODO: add log
    // choose the encoding type for measure type and no dictionary primitive type columns
    if (columnSpec instanceof TableSpec.MeasureSpec || (
        DataTypeUtil.isPrimitiveColumn(columnSpec.getSchemaDataType())
            && columnSpec.getColumnType() == ColumnType.PLAIN_VALUE)) {
      return createEncoderForMeasureOrNoDictionaryPrimitive(inputPage, columnSpec);
    } else {
      if (newWay) {
        return createEncoderForDimension((TableSpec.DimensionSpec) columnSpec, inputPage);
      } else {
        assert columnSpec instanceof TableSpec.DimensionSpec;
        return createEncoderForDimensionLegacy((TableSpec.DimensionSpec) columnSpec);
      }
    }
  }

  private ColumnPageEncoder createEncoderForDimension(TableSpec.DimensionSpec columnSpec,
      ColumnPage inputPage) {
    switch (columnSpec.getColumnType()) {
      case DIRECT_DICTIONARY:
      case PLAIN_VALUE:
        return new DirectCompressCodec(inputPage.getDataType()).createEncoder(null);
      case COMPLEX:
        return new ComplexDimensionIndexCodec(false, false).createEncoder(null);
      default:
        throw new RuntimeException("unsupported dimension type: " + columnSpec.getColumnType());
    }
  }

  private ColumnPageEncoder createEncoderForDimensionLegacy(TableSpec.DimensionSpec dimensionSpec) {
    switch (dimensionSpec.getColumnType()) {
      case DIRECT_DICTIONARY:
        return new DirectDictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex())
            .createEncoder(null);
      case PLAIN_VALUE:
        return new HighCardDictDimensionIndexCodec(dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            dimensionSpec.getSchemaDataType() == DataTypes.VARCHAR
                || dimensionSpec.getSchemaDataType() == DataTypes.BINARY).createEncoder(null);
      default:
        throw new RuntimeException("unsupported dimension type: " +
            dimensionSpec.getColumnType());
    }
  }

  private ColumnPageEncoder createEncoderForMeasureOrNoDictionaryPrimitive(ColumnPage columnPage,
      TableSpec.ColumnSpec columnSpec) {

    SimpleStatsResult stats = columnPage.getStatistics();
    DataType dataType = stats.getDataType();
    if (dataType == DataTypes.BOOLEAN
        || dataType == DataTypes.BYTE_ARRAY
        || columnPage.getDataType() == DataTypes.BINARY) {
      return new DirectCompressCodec(columnPage.getDataType()).createEncoder(null);
    } else if (dataType == DataTypes.BYTE ||
        dataType == DataTypes.SHORT ||
        dataType == DataTypes.INT ||
        dataType == DataTypes.LONG ||
        dataType == DataTypes.TIMESTAMP) {
      return selectCodecByAlgorithmForIntegral(stats, false, columnSpec).createEncoder(null);
    } else if (DataTypes.isDecimal(dataType)) {
      return createEncoderForDecimalDataTypeMeasure(columnPage, columnSpec);
    } else if (dataType == DataTypes.FLOAT || dataType == DataTypes.DOUBLE) {
      return selectCodecByAlgorithmForFloating(stats, false, columnSpec).createEncoder(null);
    } else {
      throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }

  private ColumnPageEncoder createEncoderForDecimalDataTypeMeasure(ColumnPage columnPage,
      TableSpec.ColumnSpec columnSpec) {
    DecimalConverterFactory.DecimalConverterType decimalConverterType =
        ((DecimalColumnPage) columnPage).getDecimalConverter().getDecimalConverterType();
    switch (decimalConverterType) {
      case DECIMAL_INT:
      case DECIMAL_LONG:
        return selectCodecByAlgorithmForDecimal(columnPage.getStatistics(), decimalConverterType,
            columnSpec)
            .createEncoder(null);
      default:
        return new DirectCompressCodec(columnPage.getDataType()).createEncoder(null);
    }
  }

  private static DataType fitLongMinMax(long max, long min) {
    if (max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) {
      return DataTypes.BYTE;
    } else if (max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) {
      return DataTypes.SHORT;
    } else if (max <= THREE_BYTES_MAX && min >= THREE_BYTES_MIN) {
      return DataTypes.SHORT_INT;
    } else if (max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) {
      return DataTypes.INT;
    } else {
      return DataTypes.LONG;
    }
  }

  private static DataType fitMinMax(DataType dataType, Object max, Object min) {
    if (dataType == DataTypes.BYTE || dataType == DataTypes.BOOLEAN) {
      return fitLongMinMax((byte) max, (byte) min);
    } else if (dataType == DataTypes.SHORT) {
      return fitLongMinMax((short) max, (short) min);
    } else if (dataType == DataTypes.INT) {
      return fitLongMinMax((int) max, (int) min);
    } else if ((dataType == DataTypes.LONG) || (dataType == DataTypes.TIMESTAMP)) {
      return fitLongMinMax((long) max, (long) min);
    } else if (dataType == DataTypes.DOUBLE) {
      return fitLongMinMax((long) (double) max, (long) (double) min);
    } else {
      throw new RuntimeException("internal error: " + dataType);
    }
  }

  private static DataType fitMinMaxForDecimalType(DataType dataType, Object max, Object min,
      DecimalConverterFactory.DecimalConverterType decimalConverterType) {
    long maxValue = ((BigDecimal) max).unscaledValue().longValue();
    long minValue = ((BigDecimal) min).unscaledValue().longValue();
    switch (decimalConverterType) {
      case DECIMAL_INT:
        return fitLongMinMax((int) maxValue, (int) minValue);
      case DECIMAL_LONG:
        return fitLongMinMax(maxValue, minValue);
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
  }

  private static DataType fitDeltaForDecimalType(DataType dataType, Object max, Object min,
      DecimalConverterFactory.DecimalConverterType decimalConverterType) {
    long maxValue = ((BigDecimal) max).unscaledValue().longValue();
    long minValue = ((BigDecimal) min).unscaledValue().longValue();
    switch (decimalConverterType) {
      case DECIMAL_INT:
        long value = maxValue - minValue;
        return compareMinMaxAndSelectDataType(value);
      case DECIMAL_LONG:
        return DataTypes.LONG;
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
  }

  // fit the long input value into minimum data type
  private static DataType fitDelta(DataType dataType, Object max, Object min) {
    // use long data type to calculate delta to avoid overflow
    long value;
    if (dataType == DataTypes.BYTE || dataType == DataTypes.BOOLEAN) {
      value = (long) (byte) max - (long) (byte) min;
    } else if (dataType == DataTypes.SHORT) {
      value = (long) (short) max - (long) (short) min;
    } else if (dataType == DataTypes.INT) {
      value = (long) (int) max - (long) (int) min;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      value = (long) max - (long) min;
      // The subtraction overflowed iff the operands have opposing signs
      // and the result's sign differs from the minuend.
      boolean overflow = (((long) max ^ (long) min) & ((long) max ^ value)) < 0;
      if (overflow) {
        return DataTypes.LONG;
      }
    } else if (dataType == DataTypes.DOUBLE) {
      return DataTypes.LONG;
    } else {
      throw new RuntimeException("internal error: " + dataType);
    }
    return compareMinMaxAndSelectDataType(value);
  }

  private static DataType compareMinMaxAndSelectDataType(long value) {
    if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
      return DataTypes.BYTE;
    } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
      return DataTypes.SHORT;
    } else if (value <= THREE_BYTES_MAX && value >= THREE_BYTES_MIN) {
      return DataTypes.SHORT_INT;
    } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
      return DataTypes.INT;
    } else {
      return DataTypes.LONG;
    }
  }

  /**
   * choose between adaptive encoder or delta adaptive encoder, based on whose target data type
   * size is smaller
   */
  static ColumnPageCodec selectCodecByAlgorithmForIntegral(SimpleStatsResult stats,
      boolean isComplexPrimitive, TableSpec.ColumnSpec columnSpec) {
    DataType srcDataType = stats.getDataType();
    DataType adaptiveDataType = fitMinMax(stats.getDataType(), stats.getMax(), stats.getMin());

    DataType deltaDataType = fitDelta(stats.getDataType(), stats.getMax(), stats.getMin());
    // for complex primitive, if source and destination data type is same, use adaptive encoding.
    if (!isComplexPrimitive) {
      // in case of decimal datatype, check if the decimal converter type is Int or Long and based
      // on that get size in bytes
      if (Math.min(adaptiveDataType.getSizeInBytes(), deltaDataType.getSizeInBytes()) == srcDataType
          .getSizeInBytes()) {
        // no effect to use adaptive or delta, use compression only
        return new DirectCompressCodec(stats.getDataType());
      }
    }
    boolean isInvertedIndex = isInvertedIndex(isComplexPrimitive, columnSpec);
    if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
      // choose adaptive encoding
      return new AdaptiveIntegralCodec(stats.getDataType(), adaptiveDataType, stats,
          isInvertedIndex);
    } else {
      // choose delta adaptive encoding
      return new AdaptiveDeltaIntegralCodec(stats.getDataType(), deltaDataType, stats,
          isInvertedIndex);
    }
  }

  /**
   * Check whether the column is sort column and inverted index column
   *
   * @param isComplexPrimitive
   * @param columnSpec
   * @return
   */
  private static boolean isInvertedIndex(boolean isComplexPrimitive,
      TableSpec.ColumnSpec columnSpec) {
    boolean isSort;
    boolean isInvertedIndex = false;
    if (columnSpec instanceof TableSpec.DimensionSpec && !isComplexPrimitive) {
      isSort = ((TableSpec.DimensionSpec) columnSpec).isInSortColumns();
      isInvertedIndex = isSort && ((TableSpec.DimensionSpec) columnSpec).isDoInvertedIndex();
    }
    return isInvertedIndex;
  }

  // choose between upscale adaptive encoder or upscale delta adaptive encoder,
  // based on whose target data type size is smaller
  static ColumnPageCodec selectCodecByAlgorithmForFloating(SimpleStatsResult stats,
      boolean isComplexPrimitive, TableSpec.ColumnSpec columnSpec) {
    DataType srcDataType = stats.getDataType();
    double maxValue;
    double minValue;
    if (srcDataType == DataTypes.FLOAT) {
      maxValue = (float) stats.getMax();
      minValue = (float) stats.getMin();
    } else {
      maxValue = (double) stats.getMax();
      minValue = (double) stats.getMin();
    }
    int decimalCount = stats.getDecimalCount();

    // For Complex Type primitive we should always choose adaptive path
    // as LV format will be reduced to only V format. Therefore inorder
    // to do that decimal count should be actual count instead of -1.
    if (isComplexPrimitive && decimalCount == -1 && stats instanceof PrimitivePageStatsCollector) {
      decimalCount = ((PrimitivePageStatsCollector)stats).getDecimalForComplexPrimitive();
    }

    //Here we should use the Max abs as max to getDatatype, let's say -1 and -10000000, -1 is max,
    //but we can't use -1 to getDatatype, we should use -10000000.
    double absMaxValue = Math.max(Math.abs(maxValue), Math.abs(minValue));
    if (srcDataType == DataTypes.FLOAT && decimalCount == 0) {
      return getColumnPageCodec(stats, isComplexPrimitive, columnSpec, srcDataType, maxValue,
          minValue, decimalCount, absMaxValue);
    } else if (decimalCount == 0) {
      // short, int, long
      return selectCodecByAlgorithmForIntegral(stats, false, columnSpec);
    } else if (decimalCount < 0 && !isComplexPrimitive) {
      return new DirectCompressCodec(DataTypes.DOUBLE);
    } else {
      return getColumnPageCodec(stats, isComplexPrimitive, columnSpec, srcDataType, maxValue,
          minValue, decimalCount, absMaxValue);
    }
  }

  private static ColumnPageCodec getColumnPageCodec(SimpleStatsResult stats,
      boolean isComplexPrimitive, TableSpec.ColumnSpec columnSpec, DataType srcDataType,
      double maxValue, double minValue, int decimalCount, double absMaxValue) {
    // double
    // If absMaxValue exceeds LONG.MAX_VALUE, then go for direct compression
    if ((Math.pow(10, decimalCount) * absMaxValue) > Long.MAX_VALUE) {
      return new DirectCompressCodec(DataTypes.DOUBLE);
    } else {
      long max = (long) (Math.pow(10, decimalCount) * absMaxValue);
      DataType adaptiveDataType = fitLongMinMax(max, 0);
      DataType deltaDataType = compareMinMaxAndSelectDataType(
          (long) (Math.pow(10, decimalCount) * (maxValue - minValue)));
      if (adaptiveDataType.getSizeInBytes() > deltaDataType.getSizeInBytes()) {
        return new AdaptiveDeltaFloatingCodec(srcDataType, deltaDataType, stats,
            isInvertedIndex(isComplexPrimitive, columnSpec));
      } else if (adaptiveDataType.getSizeInBytes() < DataTypes.DOUBLE.getSizeInBytes() || (
          (isComplexPrimitive) && (adaptiveDataType.getSizeInBytes() == DataTypes.DOUBLE
              .getSizeInBytes()))) {
        return new AdaptiveFloatingCodec(srcDataType, adaptiveDataType, stats,
            isInvertedIndex(isComplexPrimitive, columnSpec));
      } else {
        return new DirectCompressCodec(DataTypes.DOUBLE);
      }
    }
  }

  /**
   * choose between adaptive encoder or delta adaptive encoder, based on whose target data type
   * size is smaller for decimal data type
   */
  static ColumnPageCodec selectCodecByAlgorithmForDecimal(SimpleStatsResult stats,
      DecimalConverterFactory.DecimalConverterType decimalConverterType,
      TableSpec.ColumnSpec columnSpec) {
    DataType srcDataType = stats.getDataType();
    DataType adaptiveDataType =
        fitMinMaxForDecimalType(stats.getDataType(), stats.getMax(), stats.getMin(),
            decimalConverterType);
    DataType deltaDataType;

    if (adaptiveDataType == DataTypes.LONG) {
      deltaDataType = DataTypes.LONG;
    } else {
      deltaDataType = fitDeltaForDecimalType(stats.getDataType(), stats.getMax(), stats.getMin(),
          decimalConverterType);
    }
    // in case of decimal data type check if the decimal converter type is Int or Long and based on
    // that get size in bytes
    if (Math.min(adaptiveDataType.getSizeInBytes(), deltaDataType.getSizeInBytes()) == srcDataType
        .getSizeInBytes()) {
      // no effect to use adaptive or delta, use compression only
      return new DirectCompressCodec(stats.getDataType());
    }
    if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
      // choose adaptive encoding
      return new AdaptiveIntegralCodec(stats.getDataType(), adaptiveDataType, stats,
          isInvertedIndex(columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE, columnSpec));
    } else {
      // choose delta adaptive encoding
      return new AdaptiveDeltaIntegralCodec(stats.getDataType(), deltaDataType, stats,
          isInvertedIndex(columnSpec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE, columnSpec));
    }
  }

}
