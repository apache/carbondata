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

package org.apache.carbondata.presto;

import java.math.BigDecimal;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;

import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_UNNECESSARY;
import static org.apache.carbondata.presto.CarbondataErrorCode.CARBON_INVALID_TYPE_VALUE;

public class CarbondataUtil {

  public static long shortDecimalPartitionKey(String value, DecimalType type, String name) {
    return decimalPartitionKey(value, type, name).unscaledValue().longValue();
  }

  private static BigDecimal decimalPartitionKey(String value, DecimalType type, String name) {
    try {
      BigDecimal decimal = new BigDecimal(value);
      decimal = decimal.setScale(type.getScale(), ROUND_UNNECESSARY);
      if (decimal.precision() > type.getPrecision()) {
        throw new PrestoException(CARBON_INVALID_TYPE_VALUE,
            format("Invalid type value '%s' for %s type key: %s", value, type.toString(), name));
      }
      return decimal;
    } catch (NumberFormatException e) {
      throw new PrestoException(CARBON_INVALID_TYPE_VALUE,
          format("Invalid type value '%s' for %s type key: %s", value, type.toString(), name));
    }
  }
}
