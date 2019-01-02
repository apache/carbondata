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
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;


/**
 * PrestoFilterUtil create the carbonData Expression from the presto-domain
 */
public class PrestoFilterUtil {

  private static Map<Integer, Expression> filterMap = new HashMap<>();

  private static final String HIVE_DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";

  /**
   * @param columnHandle
   * @return
   */
  private static DataType spi2CarbondataTypeMapper(HiveColumnHandle columnHandle) {
    HiveType colType = columnHandle.getHiveType();
    if (colType.equals(HiveType.HIVE_BOOLEAN)) {
      return DataTypes.BOOLEAN;
    } else if (colType.equals(HiveType.HIVE_SHORT)) {
      return DataTypes.SHORT;
    } else if (colType.equals(HiveType.HIVE_INT)) {
      return DataTypes.INT;
    } else if (colType.equals(HiveType.HIVE_LONG)) {
      return DataTypes.LONG;
    } else if (colType.equals(HiveType.HIVE_DOUBLE)) {
      return DataTypes.DOUBLE;
    } else if (colType.equals(HiveType.HIVE_STRING)) {
      return DataTypes.STRING;
    } else if (colType.equals(HiveType.HIVE_DATE)) {
      return DataTypes.DATE;
    } else if (colType.equals(HiveType.HIVE_TIMESTAMP)) {
      return DataTypes.TIMESTAMP;
    } else if (colType.getTypeInfo() instanceof DecimalTypeInfo) {
      DecimalTypeInfo typeInfo = (DecimalTypeInfo) colType.getTypeInfo();
      return DataTypes.createDecimalType(typeInfo.getPrecision(),typeInfo.getScale());
    } else {
      return DataTypes.STRING;
    }
  }

  /**
   * Return partition filters using domain constraints
   * @param carbonTable
   * @param originalConstraint
   * @return
   */
  public static List<String> getPartitionFilters(CarbonTable carbonTable,
      TupleDomain<HiveColumnHandle> originalConstraint) {
    List<ColumnSchema> columnSchemas = carbonTable.getPartitionInfo().getColumnSchemaList();
    List<String> filter = new ArrayList<>();
    for (HiveColumnHandle columnHandle : originalConstraint.getDomains().get().keySet()) {
      List<ColumnSchema> partitionedColumnSchema = columnSchemas.stream().filter(
          columnSchema -> columnHandle.getName()
              .equals(columnSchema.getColumnName())).collect(toList());
      if (partitionedColumnSchema.size() != 0) {
        filter.addAll(createPartitionFilters(originalConstraint, columnHandle));
      }
    }
    return filter;
  }

  /** Returns list of partition key and values using domain constraints
   * @param originalConstraint
   * @param columnHandle
   */
  private static List<String> createPartitionFilters(
      TupleDomain<HiveColumnHandle> originalConstraint, HiveColumnHandle columnHandle) {
    List<String> filter = new ArrayList<>();
    if (!originalConstraint.getDomains().isPresent()) {
      return filter;
    }
    Domain domain = originalConstraint.getDomains().get().get(columnHandle);
    if (domain != null && domain.isNullableSingleValue()) {
      Object value = domain.getNullableSingleValue();
      Type type = domain.getType();
      if (value == null) {
        filter.add(columnHandle.getName() + "=" + HIVE_DEFAULT_DYNAMIC_PARTITION);
      } else if (columnHandle.getHiveType().getTypeInfo() instanceof DecimalTypeInfo) {
        int scale = ((DecimalTypeInfo) columnHandle.getHiveType().getTypeInfo()).getScale();
        if (value instanceof Long) {
          //create decimal value from Long
          BigDecimal decimalValue = new BigDecimal(new BigInteger(String.valueOf(value)), scale);
          filter.add(columnHandle.getName() + "=" + decimalValue.toString());
        } else if (value instanceof Slice) {
          //create decimal value from Slice
          BigDecimal decimalValue =
              new BigDecimal(Decimals.decodeUnscaledValue((Slice) value), scale);
          filter.add(columnHandle.getName() + "=" + decimalValue.toString());
        }
      } else if (value instanceof Slice) {
        filter.add(columnHandle.getName() + "=" + ((Slice) value).toStringUtf8());
      } else if (value instanceof Long && columnHandle.getHiveType()
          .equals(HiveType.HIVE_DATE)) {
        Calendar c = Calendar.getInstance();
        c.setTime(new java.sql.Date(0));
        c.add(Calendar.DAY_OF_YEAR, ((Long) value).intValue());
        java.sql.Date date = new java.sql.Date(c.getTime().getTime());
        filter.add(columnHandle.getName() + "=" + date.toString());
      } else if (value instanceof Long && columnHandle.getHiveType()
          .equals(HiveType.HIVE_TIMESTAMP)) {
        String timeStamp = new Timestamp((Long) value).toString();
        filter.add(columnHandle.getName() + "=" + timeStamp
            .substring(0, timeStamp.indexOf('.')));
      } else if ((value instanceof Boolean) || (value instanceof Double)
          || (value instanceof Long)) {
        filter.add(columnHandle.getName() + "=" + value.toString());
      } else {
        throw new PrestoException(NOT_SUPPORTED,
            format("Unsupported partition key type: %s", type.getDisplayName()));
      }
    }
    return filter;
  }

  /**
   * Convert presto-TupleDomain predication into Carbon scan express condition
   *
   * @param originalConstraint presto-TupleDomain
   * @return
   */
  static Expression parseFilterExpression(TupleDomain<HiveColumnHandle> originalConstraint) {

    Domain domain;

    if (originalConstraint.isNone()) {
      return null;
    }

    // final expression for the table,
    // returned by the method after combining all the column filters (colValueExpression).
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
          Object value = convertDataByType(range.getLow().getValue(), type);
          singleValues.add(value);
        } else {
          // generated for each range of column i.e. lessThan, greaterThan,
          // there can be multiple ranges for a single column. combined to create colValueExpression
          Expression rangeExpression = null;
          if (!range.getLow().isLowerUnbounded()) {
            Object value = convertDataByType(range.getLow().getValue(), type);
            switch (range.getLow().getBound()) {
              case ABOVE:
                rangeExpression =
                    new GreaterThanExpression(colExpression, new LiteralExpression(value, coltype));
                break;
              case EXACTLY:
                rangeExpression = new GreaterThanEqualToExpression(colExpression,
                    new LiteralExpression(value, coltype));
                break;
              case BELOW:
                throw new IllegalArgumentException("Low marker should never use BELOW bound");
              default:
                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
            }
          }

          if (!range.getHigh().isUpperUnbounded()) {
            Expression lessThanExpression;
            Object value = convertDataByType(range.getHigh().getValue(), type);
            switch (range.getHigh().getBound()) {
              case ABOVE:
                throw new IllegalArgumentException("High marker should never use ABOVE bound");
              case EXACTLY:
                lessThanExpression = new LessThanEqualToExpression(colExpression,
                    new LiteralExpression(value, coltype));
                break;
              case BELOW:
                lessThanExpression =
                    new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
                break;
              default:
                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
            rangeExpression = (rangeExpression == null ?
                lessThanExpression :
                new AndExpression(rangeExpression, lessThanExpression));
          }
          colValueExpression = (colValueExpression == null ?
              rangeExpression :
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
        finalFilters = (finalFilters == null ?
            colValueExpression :
            new AndExpression(finalFilters, colValueExpression));
      }
    }
    return finalFilters;
  }

  private static Object convertDataByType(Object rawdata, HiveType type) {
    if (type.equals(HiveType.HIVE_INT) || type.equals(HiveType.HIVE_SHORT)) {
      return Integer.valueOf(rawdata.toString());
    } else if (type.equals(HiveType.HIVE_LONG)) {
      return rawdata;
    } else if (type.equals(HiveType.HIVE_STRING)) {
      if (rawdata instanceof Slice) {
        return ((Slice) rawdata).toStringUtf8();
      } else {
        return rawdata;
      }
    } else if (type.equals(HiveType.HIVE_BOOLEAN)) {
      return rawdata;
    } else if (type.equals(HiveType.HIVE_DATE)) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date(0));
      c.add(Calendar.DAY_OF_YEAR, ((Long) rawdata).intValue());
      Date date = c.getTime();
      return date.getTime() * 1000;
    }
    else if (type.getTypeInfo() instanceof DecimalTypeInfo) {
      if (rawdata instanceof Double) {
        return new BigDecimal((Double) rawdata);
      } else if (rawdata instanceof Long) {
        return new BigDecimal(new BigInteger(String.valueOf(rawdata)),
            ((DecimalTypeInfo) type.getTypeInfo()).getScale());
      } else if (rawdata instanceof Slice) {
        return new BigDecimal(Decimals.decodeUnscaledValue((Slice) rawdata),
            ((DecimalTypeInfo) type.getTypeInfo()).getScale());
      }
    }
    else if (type.equals(HiveType.HIVE_TIMESTAMP)) {
      return (Long) rawdata * 1000;
    }

    return rawdata;
  }

  /**
   * get the filters from key
   */
  static Expression getFilters(Integer key) {
    return filterMap.get(key);
  }

  static void setFilter(Integer tableId, Expression filter) {
    filterMap.put(tableId, filter);
  }
}