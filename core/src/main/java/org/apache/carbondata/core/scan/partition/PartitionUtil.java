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

package org.apache.carbondata.core.scan.partition;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.BitSet;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang.StringUtils;

public class PartitionUtil {

  private static LogService LOGGER = LogServiceFactory.getLogService(PartitionUtil.class.getName());

  private static final ThreadLocal<DateFormat> timestampFormatter = new ThreadLocal<DateFormat>() {
    @Override protected DateFormat initialValue() {
      return new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    }
  };

  private static final ThreadLocal<DateFormat> dateFormatter = new ThreadLocal<DateFormat>() {
    @Override protected DateFormat initialValue() {
      return new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
              CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
    }
  };

  public static Partitioner getPartitioner(PartitionInfo partitionInfo) {
    switch (partitionInfo.getPartitionType()) {
      case HASH:
        return new HashPartitioner(partitionInfo.getNumPartitions());
      case LIST:
        return new ListPartitioner(partitionInfo);
      case RANGE:
        return new RangePartitioner(partitionInfo);
      default:
        throw new UnsupportedOperationException(
            "unsupport partition type: " + partitionInfo.getPartitionType().name());
    }
  }

  public static Object getDataBasedOnDataType(String data, DataType actualDataType) {
    if (data == null) {
      return null;
    }
    if (actualDataType != DataType.STRING && StringUtils.isEmpty(data)) {
      return null;
    }
    try {
      switch (actualDataType) {
        case STRING:
          return data;
        case INT:
          return Integer.parseInt(data);
        case SHORT:
          return Short.parseShort(data);
        case DOUBLE:
          return Double.parseDouble(data);
        case LONG:
          return Long.parseLong(data);
        case DATE:
          return PartitionUtil.dateFormatter.get().parse(data).getTime();
        case TIMESTAMP:
          return PartitionUtil.timestampFormatter.get().parse(data).getTime();
        case DECIMAL:
          return new BigDecimal(data);
        default:
          return data;
      }
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * convert the string value of partition filter to the Object
   * @param data
   * @param actualDataType
   * @return
   */
  public static Object getDataBasedOnDataTypeForFilter(String data, DataType actualDataType) {
    if (data == null) {
      return null;
    }
    if (actualDataType != DataType.STRING && StringUtils.isEmpty(data)) {
      return null;
    }
    try {
      switch (actualDataType) {
        case STRING:
          return data;
        case INT:
          return Integer.parseInt(data);
        case SHORT:
          return Short.parseShort(data);
        case DOUBLE:
          return Double.parseDouble(data);
        case LONG:
          return Long.parseLong(data);
        case DATE:
        case TIMESTAMP:
          return Long.parseLong(data) / 1000;
        case DECIMAL:
          return new BigDecimal(data);
        default:
          return data;
      }
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * generate a BitSet by size
   * @param size
   * @param initValue true: initialize all bits to true
   * @return
   */
  public static BitSet generateBitSetBySize(int size, boolean initValue) {
    BitSet bitSet = new BitSet(size);
    if (initValue) {
      bitSet.set(0, size);
    }
    return bitSet;
  }

}
