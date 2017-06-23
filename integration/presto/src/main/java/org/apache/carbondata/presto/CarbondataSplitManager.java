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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
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
import org.apache.carbondata.presto.impl.CarbonLocalInputSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.carbondata.presto.Types.checkType;

/**
 * Build Carbontable splits
 * filtering irrelevant blocks
 */
public class CarbondataSplitManager implements ConnectorSplitManager {

  private final String connectorId;
  private final CarbonTableReader carbonTableReader;

  @Inject
  public CarbondataSplitManager(CarbondataConnectorId connectorId, CarbonTableReader reader) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.carbonTableReader = requireNonNull(reader, "client is null");
  }

  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableLayoutHandle layout) {
    CarbondataTableLayoutHandle layoutHandle = (CarbondataTableLayoutHandle) layout;
    CarbondataTableHandle tableHandle = layoutHandle.getTable();
    SchemaTableName key = tableHandle.getSchemaTableName();

    // Packaging presto-TupleDomain into CarbondataColumnConstraint, to decouple from presto-spi Module
    List<CarbondataColumnConstraint> rebuildConstraints =
        getColumnConstraints(layoutHandle.getConstraint());

    CarbonTableCacheModel cache = carbonTableReader.getCarbonCache(key);
    Expression filters = parseFilterExpression(layoutHandle.getConstraint(), cache.carbonTable);

    if (cache != null) {
      try {
        List<CarbonLocalInputSplit> splits = carbonTableReader.getInputSplits2(cache, filters);

        ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
        for (CarbonLocalInputSplit split : splits) {
          cSplits.add(new CarbondataSplit(connectorId, tableHandle.getSchemaTableName(),
              layoutHandle.getConstraint(), split, rebuildConstraints));
        }
        return new FixedSplitSource(cSplits.build());
      } catch (Exception ex) {
        System.out.println(ex.toString());
      }
    }
    return null;
  }

  public List<CarbondataColumnConstraint> getColumnConstraints(
      TupleDomain<ColumnHandle> constraint) {
    ImmutableList.Builder<CarbondataColumnConstraint> constraintBuilder = ImmutableList.builder();
    for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : constraint.getColumnDomains()
        .get()) {
      CarbondataColumnHandle columnHandle =
          checkType(columnDomain.getColumn(), CarbondataColumnHandle.class, "column handle");

      constraintBuilder.add(new CarbondataColumnConstraint(columnHandle.getColumnName(),
          Optional.of(columnDomain.getDomain()), columnHandle.isInvertedIndex()));
    }

    return constraintBuilder.build();
  }

  /**
   * Convert presto-TupleDomain predication into Carbon scan express condition
   * @param originalConstraint  presto-TupleDomain
   * @param carbonTable
   * @return
   */
  public Expression parseFilterExpression(TupleDomain<ColumnHandle> originalConstraint,
      CarbonTable carbonTable) {
    ImmutableList.Builder<Expression> filters = ImmutableList.builder();

    Domain domain = null;

    for (ColumnHandle c : originalConstraint.getDomains().get().keySet()) {

      CarbondataColumnHandle cdch = (CarbondataColumnHandle) c;
      Type type = cdch.getColumnType();

      List<CarbonColumn> ccols = carbonTable.getCreateOrderColumn(carbonTable.getFactTableName());
      Optional<CarbonColumn> target =
          ccols.stream().filter(a -> a.getColName().equals(cdch.getColumnName())).findFirst();

      if (target.get() == null) return null;

      DataType coltype = target.get().getDataType();
      ColumnExpression colExpression =
          new ColumnExpression(cdch.getColumnName(), target.get().getDataType());
      //colExpression.setColIndex(cs.getSchemaOrdinal());
      colExpression.setDimension(target.get().isDimension());
      colExpression.setDimension(
          carbonTable.getDimensionByName(carbonTable.getFactTableName(), cdch.getColumnName()));
      colExpression.setCarbonColumn(target.get());

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
            Object value = ConvertDataByType(range.getLow().getValue(), type);
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
            Object value = ConvertDataByType(range.getHigh().getValue(), type);
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
        } else ex = new EqualToExpression(colExpression,
            new LiteralExpression(singleValues.get(0), coltype));
        filters.add(ex);
      } else if (singleValues.size() > 1) {
        ListExpression candidates = null;
        List<Expression> exs = singleValues.stream().map((a) -> {
          return new LiteralExpression(ConvertDataByType(a, type), coltype);
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
        } else if (disjuncts.size() == 1)//only have one value
          filters.add(disjuncts.get(0));
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
    else//no filter
      return null;

    return finalFilters;
  }

  /**
   * Convert presto spi Type into Carbondata Type
   *
   * @param colType
   * @return
   */
  public static DataType Spi2CarbondataTypeMapper(Type colType) {
    if (colType == BooleanType.BOOLEAN) return DataType.BOOLEAN;
    else if (colType == SmallintType.SMALLINT) return DataType.SHORT;
    else if (colType == IntegerType.INTEGER) return DataType.INT;
    else if (colType == BigintType.BIGINT) return DataType.LONG;
    else if (colType == DoubleType.DOUBLE) return DataType.DOUBLE;
    else if (colType == DecimalType.createDecimalType()) return DataType.DECIMAL;
    else if (colType == VarcharType.VARCHAR) return DataType.STRING;
    else if (colType == DateType.DATE) return DataType.DATE;
    else if (colType == TimestampType.TIMESTAMP) return DataType.TIMESTAMP;
    else return DataType.STRING;
  }

  public Object ConvertDataByType(Object rawdata, Type type) {
    if (type.equals(IntegerType.INTEGER)) return new Integer((rawdata.toString()));
    else if (type.equals(BigintType.BIGINT)) return (Long) rawdata;
    else if (type.equals(VarcharType.VARCHAR)) return ((Slice) rawdata).toStringUtf8();
    else if (type.equals(BooleanType.BOOLEAN)) return (Boolean) (rawdata);
    else if(type.equals(TimestampType.TIMESTAMP)) return (Long) rawdata * 1000;

    return rawdata;
  }
}
