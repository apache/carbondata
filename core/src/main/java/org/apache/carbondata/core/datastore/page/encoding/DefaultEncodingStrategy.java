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

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.ComplexDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.DictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.DirectDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.dimension.legacy.HighCardDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Default strategy will select encoding base on column page data type and statistics
 */
public class DefaultEncodingStrategy extends EncodingStrategy {

  private static final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
  private static final int THREE_BYTES_MIN = - THREE_BYTES_MAX - 1;

  private static final boolean newWay = false;

  @Override
  public ColumnPageEncoder createEncoder(TableSpec.ColumnSpec columnSpec, ColumnPage inputPage) {
    // TODO: add log
    if (columnSpec instanceof TableSpec.MeasureSpec) {
      return createEncoderForMeasure(inputPage);
    } else {
      if (newWay) {
        return createEncoderForDimension((TableSpec.DimensionSpec) columnSpec, inputPage);
      } else {
        return createEncoderForDimensionLegacy((TableSpec.DimensionSpec) columnSpec);
      }
    }
  }

  private ColumnPageEncoder createEncoderForDimension(TableSpec.DimensionSpec columnSpec,
      ColumnPage inputPage) {
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    switch (columnSpec.getDimensionType()) {
      case GLOBAL_DICTIONARY:
      case DIRECT_DICTIONARY:
      case PLAIN_VALUE:
        return new DirectCompressCodec(inputPage.getDataType()).createEncoder(null);
      case COMPLEX:
        return new ComplexDimensionIndexCodec(false, false, compressor).createEncoder(null);
      default:
        throw new RuntimeException("unsupported dimension type: " +
            columnSpec.getDimensionType());
    }
  }

  private ColumnPageEncoder createEncoderForDimensionLegacy(TableSpec.DimensionSpec columnSpec) {
    TableSpec.DimensionSpec dimensionSpec = columnSpec;
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    switch (dimensionSpec.getDimensionType()) {
      case GLOBAL_DICTIONARY:
        return new DictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor).createEncoder(null);
      case DIRECT_DICTIONARY:
        return new DirectDictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor).createEncoder(null);
      case PLAIN_VALUE:
        return new HighCardDictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor).createEncoder(null);
      default:
        throw new RuntimeException("unsupported dimension type: " +
            dimensionSpec.getDimensionType());
    }
  }

  private ColumnPageEncoder createEncoderForMeasure(ColumnPage columnPage) {
    SimpleStatsResult stats = columnPage.getStatistics();
    switch (stats.getDataType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return selectCodecByAlgorithm(stats).createEncoder(null);
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case BYTE_ARRAY:
        return new DirectCompressCodec(columnPage.getDataType()).createEncoder(null);
      default:
        throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }

  private static DataType fitLongMinMax(long max, long min) {
    if (max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) {
      return DataType.BYTE;
    } else if (max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) {
      return DataType.SHORT;
    } else if (max <= THREE_BYTES_MAX && min >= THREE_BYTES_MIN) {
      return DataType.SHORT_INT;
    } else if (max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) {
      return DataType.INT;
    } else {
      return DataType.LONG;
    }
  }

  private static DataType fitMinMax(DataType dataType, Object max, Object min) {
    switch (dataType) {
      case BYTE:
        return fitLongMinMax((byte) max, (byte) min);
      case SHORT:
        return fitLongMinMax((short) max, (short) min);
      case INT:
        return fitLongMinMax((int) max, (int) min);
      case LONG:
        return fitLongMinMax((long) max, (long) min);
      case DOUBLE:
        return DataType.DOUBLE;
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
  }

  // fit the long input value into minimum data type
  private static DataType fitDelta(DataType dataType, Object max, Object min) {
    // use long data type to calculate delta to avoid overflow
    long value;
    switch (dataType) {
      case BYTE:
        value = (long)(byte) max - (long)(byte) min;
        break;
      case SHORT:
        value = (long)(short) max - (long)(short) min;
        break;
      case INT:
        value = (long)(int) max - (long)(int) min;
        break;
      case LONG:
        // TODO: add overflow detection and return delta type
        return DataType.LONG;
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
    if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
      return DataType.BYTE;
    } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
      return DataType.SHORT;
    } else if (value <= THREE_BYTES_MAX && value >= THREE_BYTES_MIN) {
      return DataType.SHORT_INT;
    } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
      return DataType.INT;
    } else {
      return DataType.LONG;
    }
  }

  /**
   * choose between adaptive encoder or delta adaptive encoder, based on whose target data type
   * size is smaller
   */
  static ColumnPageCodec selectCodecByAlgorithm(SimpleStatsResult stats) {
    DataType srcDataType = stats.getDataType();
    DataType adaptiveDataType = fitMinMax(stats.getDataType(), stats.getMax(), stats.getMin());
    DataType deltaDataType;

    if (adaptiveDataType == DataType.LONG) {
      deltaDataType = DataType.LONG;
    } else {
      deltaDataType = fitDelta(stats.getDataType(), stats.getMax(), stats.getMin());
    }
    if (Math.min(adaptiveDataType.getSizeInBytes(), deltaDataType.getSizeInBytes()) ==
        srcDataType.getSizeInBytes()) {
      // no effect to use adaptive or delta, use compression only
      return new DirectCompressCodec(stats.getDataType());
    }
    if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
      // choose adaptive encoding
      return new AdaptiveIntegralCodec(stats.getDataType(), adaptiveDataType, stats);
    } else {
      // choose delta adaptive encoding
      return new AdaptiveDeltaIntegralCodec(stats.getDataType(), deltaDataType, stats);
    }
  }

}
