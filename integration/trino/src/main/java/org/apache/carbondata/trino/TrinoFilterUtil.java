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

package org.apache.carbondata.trino;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.toList;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Decimals;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TrinoFilterUtil create the carbonData Expression from the Trino-domain
 */
public class TrinoFilterUtil {

  /**
   * @param columnHandle
   * @return
   */
  private static DataType spi2CarbondataTypeMapper(HiveColumnHandle columnHandle) {
    HiveType colType = columnHandle.getHiveType();
    if (colType.equals(HiveType.HIVE_BOOLEAN)) {
      return DataTypes.BOOLEAN;
    } else if (colType.equals(HiveType.HIVE_BINARY)) {
      return DataTypes.BINARY;
    } else if (colType.equals(HiveType.HIVE_SHORT)) {
      return DataTypes.SHORT;
    } else if (colType.equals(HiveType.HIVE_INT)) {
      return DataTypes.INT;
    } else if (colType.equals(HiveType.HIVE_FLOAT)) {
      return DataTypes.FLOAT;
    } else if (colType.equals(HiveType.HIVE_LONG)) {
      return DataTypes.LONG;
    } else if (colType.equals(HiveType.HIVE_DOUBLE)) {
      return DataTypes.DOUBLE;
    } else if (colType.equals(HiveType.HIVE_STRING)) {
      return DataTypes.STRING;
    } else if (colType.equals(HiveType.HIVE_BYTE)) {
      return DataTypes.BYTE;
    } else if (colType.equals(HiveType.HIVE_DATE)) {
      return DataTypes.DATE;
    } else if (colType.equals(HiveType.HIVE_TIMESTAMP)) {
      return DataTypes.TIMESTAMP;
    } else if (colType.getTypeInfo() instanceof DecimalTypeInfo) {
      DecimalTypeInfo typeInfo = (DecimalTypeInfo) colType.getTypeInfo();
      return DataTypes.createDecimalType(typeInfo.getPrecision(), typeInfo.getScale());
    } else {
      return DataTypes.STRING;
    }
  }

  /**
   * Convert Trino-TupleDomain predication into Carbon scan express condition
   *
   * @param originalConstraint presto-TupleDomain
   * @return
   */
  public static Expression parseFilterExpression(TupleDomain<HiveColumnHandle> originalConstraint) {

    Domain domain;

    if (originalConstraint.isNone()) {
      return null;
    }

    // final expression for the table,
    // returned by the method after combining all the column filters
    // (colValueExpression).
    Expression finalFilters = null;

    for (HiveColumnHandle cdch : originalConstraint.getDomains().get().keySet()) {

      // Build ColumnExpression for Expression(Carbondata)
      HiveType type = cdch.getHiveType();
      DataType coltype = spi2CarbondataTypeMapper(cdch);
      Expression colExpression = new ColumnExpression(cdch.getName(), coltype);

      domain = originalConstraint.getDomains().get().get(cdch);
      checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
      List<Object> singleValues = new ArrayList<>();

      // combination of multiple rangeExpression for a single column,
      // in case of multiple range Filter on single column
      // else this is equal to rangeExpression, combined to create finalFilters
      Expression colValueExpression = null;

      for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
        if (range.isSingleValue()) {
          Object value = convertDataByType(range.getLowValue().get(), type);
          singleValues.add(value);
        } else {
          // generated for each range of column i.e. lessThan, greaterThan,
          // there can be multiple ranges for a single column. combined to
          // create colValueExpression
          Expression rangeExpression = null;
          if (range.isLowUnbounded()) {
            Object value = convertDataByType(range.getLowValue().get(), type);
            // include swith case
            if (range.isLowInclusive()) {
              rangeExpression =
                  new GreaterThanExpression(colExpression, new LiteralExpression(value, coltype));
            } else if (range.isSingleValue()) {
              rangeExpression = new GreaterThanEqualToExpression(colExpression,
                  new LiteralExpression(value, coltype));
            } else if (range.isHighInclusive()) {
              throw new IllegalArgumentException("Low marker should never use BELOW bound");
            } else {
              throw new AssertionError("Unhandled to bound");
            }
          }

          if (range.isHighUnbounded()) {
            Expression lessThanExpression;
            Object value = convertDataByType(range.getHighValue().get(), type);
            // include swith case
            if (range.isLowInclusive()) {
              throw new IllegalArgumentException("High marker should never use ABOVE bound");
            } else if (range.isSingleValue()) {
              lessThanExpression = new GreaterThanEqualToExpression(colExpression,
                  new LiteralExpression(value, coltype));
            } else if (range.isHighInclusive()) {
              lessThanExpression =
                  new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
            } else {
              throw new AssertionError("Unhandled to bound");
            }
            rangeExpression = (rangeExpression == null ? lessThanExpression :
                new AndExpression(rangeExpression, lessThanExpression));
          }
          colValueExpression = (colValueExpression == null ? rangeExpression :
              new OrExpression(colValueExpression, rangeExpression));
        }
      }

      if (singleValues.size() == 1) {
        colValueExpression = new EqualToExpression(colExpression,
            new LiteralExpression(singleValues.get(0), coltype));
      } else if (singleValues.size() > 1) {
        List<Expression> exs =
            singleValues.stream().map((a) -> new LiteralExpression(a, coltype)).collect(toList());
        colValueExpression = new InExpression(colExpression, new ListExpression(exs));
      }

      if (colValueExpression != null) {
        finalFilters = (finalFilters == null ? colValueExpression :
            new AndExpression(finalFilters, colValueExpression));
      }
    }
    return finalFilters;
  }

  private static Object convertDataByType(Object rawData, HiveType type) {
    if (type.equals(HiveType.HIVE_INT) || type.equals(HiveType.HIVE_SHORT)) {
      return Integer.valueOf(rawData.toString());
    } else if (type.equals(HiveType.HIVE_FLOAT)) {
      return Float.intBitsToFloat((int) ((Long) rawData).longValue());
    } else if (type.equals(HiveType.HIVE_LONG)) {
      return rawData;
    } else if (type.equals(HiveType.HIVE_STRING) || type.equals(HiveType.HIVE_BINARY)) {
      if (rawData instanceof Slice) {
        return ((Slice) rawData).toStringUtf8();
      } else {
        return rawData;
      }
    } else if (type.equals(HiveType.HIVE_BOOLEAN)) {
      return rawData;
    } else if (type.equals(HiveType.HIVE_DATE)) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date(0));
      c.add(Calendar.DAY_OF_YEAR, ((Long) rawData).intValue());
      Date date = c.getTime();
      return date.getTime() * 1000;
    } else if (type.getTypeInfo() instanceof DecimalTypeInfo) {
      if (rawData instanceof Double) {
        return new BigDecimal((Double) rawData);
      } else if (rawData instanceof Long) {
        return new BigDecimal(new BigInteger(String.valueOf(rawData)),
            ((DecimalTypeInfo) type.getTypeInfo()).getScale());
      } else if (rawData instanceof Slice) {
        return new BigDecimal(Decimals.decodeUnscaledValue((Slice) rawData),
            ((DecimalTypeInfo) type.getTypeInfo()).getScale());
      }
    } else if (type.equals(HiveType.HIVE_TIMESTAMP)) {
      return (Long) rawData * 1000;
    }

    return rawData;
  }
}