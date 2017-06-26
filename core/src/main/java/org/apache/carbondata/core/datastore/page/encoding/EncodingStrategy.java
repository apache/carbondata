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

import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

/**
 * Base class for encoding strategy implementation.
 */
public abstract class EncodingStrategy {

  /**
   * create codec based on the page data type and statistics
   */
  public ColumnPageCodec createCodec(ColumnPageStatsVO stats) {
    switch (stats.getDataType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return newCodecForIntegerType(stats);
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
  public ColumnPageCodec createCodec(ValueEncoderMeta meta) {
    ColumnPageStatsVO stats = ColumnPageStatsVO.copyFrom(meta);
    return createCodec(stats);
  }

  // for byte, short, int, long
  abstract ColumnPageCodec newCodecForIntegerType(ColumnPageStatsVO stats);

  // for float, double
  abstract ColumnPageCodec newCodecForFloatingType(ColumnPageStatsVO stats);

  // for decimal
  abstract ColumnPageCodec newCodecForDecimalType(ColumnPageStatsVO stats);

  // for byte array
  abstract ColumnPageCodec newCodecForByteArrayType(ColumnPageStatsVO stats);

}
