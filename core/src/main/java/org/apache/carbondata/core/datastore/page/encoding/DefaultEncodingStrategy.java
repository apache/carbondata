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

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Default strategy will select encoding base on column page data type and statistics
 */
public class DefaultEncodingStrategy extends EncodingStrategy {

  private static final Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private static final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
  private static final int THREE_BYTES_MIN = - THREE_BYTES_MAX;

  // fit the long input value into minimum data type
  private static DataType fitDataType(long value) {
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

  private DataType fitDataType(long max, long min) {
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

  // fit the input double value into minimum data type
  private DataType fitDataType(double value, int decimal) {
    DataType dataType = DataType.DOUBLE;
    if (decimal == 0) {
      if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
        dataType = DataType.BYTE;
      } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
        dataType = DataType.SHORT;
      } else if (value <= THREE_BYTES_MAX && value >= THREE_BYTES_MIN) {
        return DataType.SHORT_INT;
      } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
        dataType = DataType.INT;
      } else if (value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
        dataType = DataType.LONG;
      }
    }
    return dataType;
  }

  // choose between adaptive encoder or delta adaptive encoder, based on whose target data type
  // size is smaller
  @Override
  ColumnPageCodec newCodecForIntegerType(ColumnPageStatsVO stats) {
    DataType adaptiveDataType = fitDataType((long)stats.getMax(), (long)stats.getMin());
    DataType deltaDataType;

    // TODO: this handling is for data compatibility, change to Override check when implementing
    // encoding override feature
    if (adaptiveDataType == DataType.LONG) {
      deltaDataType = DataType.LONG;
    } else {
      deltaDataType = fitDataType((long) stats.getMax() - (long) stats.getMin());
    }
    if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
      // choose adaptive encoding
      return AdaptiveIntegerCodec.newInstance(
          stats.getDataType(), adaptiveDataType, stats, compressor);
    } else {
      // choose delta adaptive encoding
      return DeltaIntegerCodec.newInstance(stats.getDataType(), deltaDataType, stats, compressor);
    }
  }

  // choose between upscale adaptive encoder or upscale delta adaptive encoder,
  // based on whose target data type size is smaller
  @Override
  ColumnPageCodec newCodecForFloatingType(ColumnPageStatsVO stats) {
    DataType srcDataType = stats.getDataType();
    double maxValue = (double) stats.getMax();
    double minValue = (double) stats.getMin();
    int decimal = stats.getDecimal();

    //Here we should use the Max abs as max to getDatatype, let's say -1 and -10000000, -1 is max,
    //but we can't use -1 to getDatatype, we should use -10000000.
    double absMaxValue = Math.abs(maxValue) >= Math.abs(minValue) ? maxValue : minValue;

    if (decimal == 0) {
      // short, int, long
      DataType adaptiveDataType = fitDataType(absMaxValue, decimal);
      DataType deltaDataType = fitDataType(maxValue - minValue, decimal);
      if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
        // choose adaptive encoding
        return AdaptiveIntegerCodec.newInstance(srcDataType, adaptiveDataType, stats, compressor);
      } else {
        // choose delta adaptive encoding
        return DeltaIntegerCodec.newInstance(srcDataType, deltaDataType, stats, compressor);
      }
    } else {
      // double
      DataType upscaleAdaptiveDataType = fitDataType(Math.pow(10, decimal) * absMaxValue, decimal);
      DataType upscaleDiffDataType =
          fitDataType(Math.pow(10, decimal) * (maxValue - minValue), decimal);
      if (upscaleAdaptiveDataType.getSizeInBytes() <= upscaleDiffDataType.getSizeInBytes()) {
        return UpscaleFloatingCodec.newInstance(
            srcDataType, upscaleAdaptiveDataType, stats, compressor);
      } else {
        return UpscaleDeltaFloatingCodec.newInstance(
            srcDataType, upscaleDiffDataType, stats, compressor);
      }
    }
  }

  // for decimal, currently it is a very basic implementation
  @Override
  ColumnPageCodec newCodecForDecimalType(ColumnPageStatsVO stats) {
    return CompressionCodec.newInstance(stats.getDataType(), compressor);
  }

  @Override
  ColumnPageCodec newCodecForByteArrayType(ColumnPageStatsVO stats) {
    return CompressionCodec.newInstance(stats.getDataType(), compressor);
  }
}
