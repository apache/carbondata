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
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ColumnPageCodecMeta;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

/**
 * Base class for encoding strategy implementation.
 */
public abstract class EncodingStrategy {

  /**
   * create codec based on the page data type and statistics
   */
  public ColumnPageCodec newCodec(SimpleStatsResult stats) {
    switch (stats.getDataType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return newCodecForIntegralType(stats);
      case FLOAT:
      case DOUBLE:
        return newCodecForFloatingType(stats);
      case DECIMAL:
        return newCodecForDecimalType(stats);
      case BYTE_ARRAY:
        // no dictionary dimension
        return newCodecForByteArrayType(stats);
      default:
        throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }

  /**
   * create codec based on the page data type and statistics contained by ValueEncoderMeta
   */
  public ColumnPageCodec newCodec(ValueEncoderMeta meta) {
    if (meta instanceof ColumnPageCodecMeta) {
      ColumnPageCodecMeta codecMeta = (ColumnPageCodecMeta) meta;
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(codecMeta);
      switch (codecMeta.getSrcDataType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return newCodecForIntegralType(stats);
        case FLOAT:
        case DOUBLE:
          return newCodecForFloatingType(stats);
        case DECIMAL:
          return newCodecForDecimalType(stats);
        case BYTE_ARRAY:
          // no dictionary dimension
          return newCodecForByteArrayType(stats);
        default:
          throw new RuntimeException("unsupported data type: " + stats.getDataType());
      }
    } else {
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(meta);
      switch (meta.getType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return newCodecForIntegralType(stats);
        case FLOAT:
        case DOUBLE:
          return newCodecForFloatingType(stats);
        case DECIMAL:
          return newCodecForDecimalType(stats);
        case BYTE_ARRAY:
          // no dictionary dimension
          return newCodecForByteArrayType(stats);
        default:
          throw new RuntimeException("unsupported data type: " + stats.getDataType());
      }
    }
  }

  // for byte, short, int, long
  abstract ColumnPageCodec newCodecForIntegralType(SimpleStatsResult stats);

  // for float, double
  abstract ColumnPageCodec newCodecForFloatingType(SimpleStatsResult stats);

  // for decimal
  abstract ColumnPageCodec newCodecForDecimalType(SimpleStatsResult stats);

  // for byte array
  abstract ColumnPageCodec newCodecForByteArrayType(SimpleStatsResult stats);

  // for dimension column
  public abstract ColumnPageCodec newCodec(TableSpec.DimensionSpec dimensionSpec);

}
