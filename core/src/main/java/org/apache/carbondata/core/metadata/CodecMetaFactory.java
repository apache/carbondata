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

package org.apache.carbondata.core.metadata;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.metadata.datatype.DataType.*;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;

public class CodecMetaFactory {

  private static final ColumnarFormatVersion version =
      CarbonProperties.getInstance().getFormatVersion();

  public static ValueEncoderMeta createMeta() {
    switch (version) {
      case V1:
      case V2:
        return new ValueEncoderMeta();
      case V3:
        return ColumnPageCodecMeta.newInstance();
      default:
        throw new UnsupportedOperationException("unsupported version: " + version);
    }
  }

  public static ValueEncoderMeta createMeta(SimpleStatsResult stats, DataType targetDataType) {
    switch (version) {
      case V1:
      case V2:
        ValueEncoderMeta meta = new ValueEncoderMeta();
        switch (stats.getDataType()) {
          case SHORT:
            meta.setMaxValue((long)(short) stats.getMax());
            meta.setMinValue((long)(short) stats.getMin());
            break;
          case INT:
            meta.setMaxValue((long)(int) stats.getMax());
            meta.setMinValue((long)(int) stats.getMin());
            break;
          default:
            meta.setMaxValue(stats.getMax());
            meta.setMinValue(stats.getMin());
            break;
        }
        meta.setDecimal(stats.getDecimalPoint());
        meta.setType(converType(stats.getDataType()));
        return meta;
      case V3:
        return ColumnPageCodecMeta.newInstance(stats, targetDataType);
      default:
        throw new UnsupportedOperationException("unsupported version: " + version);
    }
  }

  public static char converType(DataType type) {
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      case DOUBLE:
        return CarbonCommonConstants.DOUBLE_MEASURE;
      case DECIMAL:
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

}
