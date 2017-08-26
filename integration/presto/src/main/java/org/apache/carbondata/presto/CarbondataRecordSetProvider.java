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

import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.carbondata.presto.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CarbondataRecordSetProvider implements ConnectorRecordSetProvider {

  private final String connectorId;
  private final CarbonTableReader carbonTableReader;

  @Inject
  public CarbondataRecordSetProvider(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = reader;
  }

  @Override public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
    requireNonNull(split, "split is null");
    requireNonNull(columns, "columns is null");

    CarbondataSplit carbondataSplit =
        checkType(split, CarbondataSplit.class, "split is not class CarbondataSplit");
    checkArgument(carbondataSplit.getConnectorId().equals(connectorId), "split is not for this connector");

    StringBuffer targetColsBuffer = new StringBuffer();
    String targetCols = "";
    // Convert all columns handles
    ImmutableList.Builder<CarbondataColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add(checkType(handle, CarbondataColumnHandle.class, "handle"));
      targetColsBuffer.append(((CarbondataColumnHandle) handle).getColumnName()).append(",");
    }

    // Build column projection(check the column order)
    if (targetColsBuffer.length() > 0) {
      targetCols = targetColsBuffer.substring(0, targetCols.length() - 1);
    }
    else
    {
      targetCols = null;
    }
    //String cols = String.join(",", columns.stream().map(a -> ((CarbondataColumnHandle)a).getColumnName()).collect(Collectors.toList()));

    CarbonTableCacheModel tableCacheModel =
        carbonTableReader.getCarbonCache(carbondataSplit.getSchemaTableName());
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.carbonTable, "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.tableInfo, "tableCacheModel.tableInfo should not be null");

    // Build Query Model
    CarbonTable targetTable = tableCacheModel.carbonTable;
    CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(targetTable, targetCols);
    QueryModel queryModel =
        QueryModel.createModel(targetTable.getAbsoluteTableIdentifier(),
            queryPlan, targetTable, new DataTypeConverterImpl());

    // Push down filter
    fillFilter2QueryModel(queryModel, carbondataSplit.getConstraints(), targetTable);

    // Return new record set
    return new CarbondataRecordSet(targetTable, session, carbondataSplit,
        handles.build(), queryModel);
  }

  // Build filter for QueryModel
  private void fillFilter2QueryModel(QueryModel queryModel,
      TupleDomain<ColumnHandle> originalConstraint, CarbonTable carbonTable) {

    //queryModel.setFilterExpressionResolverTree(new FilterResolverIntf());

    //Build Predicate Expression
    ImmutableList.Builder<Expression> filters = ImmutableList.builder();

    Domain domain = null;

    for (ColumnHandle c : originalConstraint.getDomains().get().keySet()) {

      // Build ColumnExpresstion for Expresstion(Carbondata)
      CarbondataColumnHandle cdch = (CarbondataColumnHandle) c;
      Type type = cdch.getColumnType();

      DataType coltype = spi2CarbondataTypeMapper(cdch);
      Expression colExpression = new ColumnExpression(cdch.getColumnName(), coltype);

      domain = originalConstraint.getDomains().get().get(c);
      checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

      if (domain.getValues().isNone()) {
      }

      if (domain.getValues().isAll()) {
      }

      List<Object> singleValues = new ArrayList<>();
      List<Expression> disjuncts = new ArrayList<>();
      for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
        if (range.isSingleValue()) {
          singleValues.add(range.getLow().getValue());
        } else {
          List<Expression> rangeConjuncts = new ArrayList<>();
          if (!range.getLow().isLowerUnbounded()) {
            Object value = convertDataByType(range.getLow().getValue(), type);
            switch (range.getLow().getBound()) {
              case ABOVE:
                if (type == TimestampType.TIMESTAMP) {
                  //todo not now
                } else {
                  GreaterThanExpression greater = new GreaterThanExpression(colExpression,
                          new LiteralExpression(value, coltype));
                  rangeConjuncts.add(greater);
                }
                break;
              case EXACTLY:
                GreaterThanEqualToExpression greater =
                        new GreaterThanEqualToExpression(colExpression,
                                new LiteralExpression(value, coltype));
                rangeConjuncts.add(greater);
                break;
              case BELOW:
                throw new IllegalArgumentException("Low marker should never use BELOW bound");
              default:
                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
            }
          }
          if (!range.getHigh().isUpperUnbounded()) {
            Object value = convertDataByType(range.getHigh().getValue(), type);
            switch (range.getHigh().getBound()) {
              case ABOVE:
                throw new IllegalArgumentException("High marker should never use ABOVE bound");
              case EXACTLY:
                LessThanEqualToExpression less = new LessThanEqualToExpression(colExpression,
                        new LiteralExpression(value, coltype));
                rangeConjuncts.add(less);
                break;
              case BELOW:
                LessThanExpression less2 =
                        new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
                rangeConjuncts.add(less2);
                break;
              default:
                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
          }
          disjuncts.addAll(rangeConjuncts);
        }
      }
      if (singleValues.size() == 1) {
        Expression ex = null;
        if (coltype.equals(DataType.STRING)) {
          ex = new EqualToExpression(colExpression,
              new LiteralExpression(((Slice) singleValues.get(0)).toStringUtf8(), coltype));
        } else if (coltype.equals(DataType.TIMESTAMP) || coltype.equals(DataType.DATE)) {
          Long value = (Long) singleValues.get(0) * 1000;
          ex = new EqualToExpression(colExpression,
              new LiteralExpression(value , coltype));
        } else ex = new EqualToExpression(colExpression,
            new LiteralExpression(singleValues.get(0), coltype));
        filters.add(ex);
      } else if (singleValues.size() > 1) {
        ListExpression candidates = null;
        List<Expression> exs = singleValues.stream().map((a) -> {
          return new LiteralExpression(convertDataByType(a, type), coltype);
        }).collect(Collectors.toList());
        candidates = new ListExpression(exs);

        if (candidates != null) filters.add(new InExpression(colExpression, candidates));
      } else if (disjuncts.size() > 0) {
        if (disjuncts.size() > 1) {
          Expression finalFilters = new OrExpression(disjuncts.get(0), disjuncts.get(1));
          if (disjuncts.size() > 2) {
            for (int i = 2; i < disjuncts.size(); i++) {
              filters.add(new AndExpression(finalFilters, disjuncts.get(i)));
            }
          }
        } else if (disjuncts.size() == 1) filters.add(disjuncts.get(0));
      }
    }

    Expression finalFilters;
    List<Expression> tmp = filters.build();
    if (tmp.size() > 1) {
      finalFilters = new OrExpression(tmp.get(0), tmp.get(1));
      if (tmp.size() > 2) {
        for (int i = 2; i < tmp.size(); i++) {
          finalFilters = new OrExpression(finalFilters, tmp.get(i));
        }
      }
    } else if (tmp.size() == 1) finalFilters = tmp.get(0);
    else return;

    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // todo set into QueryModel
    CarbonInputFormatUtil.processFilterExpression(finalFilters, carbonTable);
    queryModel.setFilterExpressionResolverTree(CarbonInputFormatUtil
        .resolveFilter(finalFilters, queryModel.getAbsoluteTableIdentifier(), tableProvider));
  }

  public static DataType spi2CarbondataTypeMapper(CarbondataColumnHandle carbondataColumnHandle) {
    Type colType = carbondataColumnHandle.getColumnType();
    if (colType == BooleanType.BOOLEAN) return DataType.BOOLEAN;
    else if (colType == SmallintType.SMALLINT) return DataType.SHORT;
    else if (colType == IntegerType.INTEGER) return DataType.INT;
    else if (colType == BigintType.BIGINT) return DataType.LONG;
    else if (colType == DoubleType.DOUBLE) return DataType.DOUBLE;
    else if (colType == VarcharType.VARCHAR) return DataType.STRING;
    else if (colType == DateType.DATE) return DataType.DATE;
    else if (colType == TimestampType.TIMESTAMP) return DataType.TIMESTAMP;
    else if (colType == DecimalType.createDecimalType(carbondataColumnHandle.getPrecision(),
        carbondataColumnHandle.getScale())) return DataType.DECIMAL;
    else return DataType.STRING;
  }

  public Object convertDataByType(Object rawdata, Type type) {
    if (type.equals(IntegerType.INTEGER)) return Integer.valueOf(rawdata.toString());
    else if (type.equals(BigintType.BIGINT)) return (Long) rawdata;
    else if (type.equals(VarcharType.VARCHAR)) return ((Slice) rawdata).toStringUtf8();
    else if (type.equals(BooleanType.BOOLEAN)) return (Boolean) (rawdata);
    else if(type.equals(TimestampType.TIMESTAMP)) return (Long) rawdata * 1000;
    return rawdata;
  }
}
