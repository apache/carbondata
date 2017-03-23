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
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.carbondata.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CarbondataRecordSetProvider implements ConnectorRecordSetProvider {

    private final String connectorId;
    private final CarbonTableReader carbonTableReader;

    @Inject
    public CarbondataRecordSetProvider(
            CarbondataConnectorId connectorId,
            CarbonTableReader reader)
    {
        //this.config = requireNonNull(config, "config is null");
        //this.connector = requireNonNull(connector, "connector is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.carbonTableReader = reader;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        // Convert split
        CarbondataSplit cdSplit = checkType(split, CarbondataSplit.class, "split is not class CarbondataSplit");
        checkArgument(cdSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        // Convert all columns handles
        ImmutableList.Builder<CarbondataColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, CarbondataColumnHandle.class, "handle"));
        }

        // Build column projection(check the column order)
        String targetCols = "";
        for(ColumnHandle col : columns){
            targetCols += ((CarbondataColumnHandle)col).getColumnName() + ",";
        }
        targetCols = targetCols.substring(0, targetCols.length() -1 );
        //String cols = String.join(",", columns.stream().map(a -> ((CarbondataColumnHandle)a).getColumnName()).collect(Collectors.toList()));

        CarbonTableCacheModel tableCacheModel = carbonTableReader.getCarbonCache(cdSplit.getSchemaTableName());
        checkNotNull(tableCacheModel, "tableCacheModel should not be null");
        checkNotNull(tableCacheModel.carbonTable, "tableCacheModel.carbonTable should not be null");
        checkNotNull(tableCacheModel.tableInfo, "tableCacheModel.tableInfo should not be null");

        // Build Query Model
        CarbonTable targetTable = tableCacheModel.carbonTable;
        CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(targetTable, targetCols);
        QueryModel queryModel = QueryModel.createModel(targetTable.getAbsoluteTableIdentifier(), queryPlan, targetTable);

        // Push down filter
        fillFilter2QueryModel(queryModel, cdSplit.getConstraints(), targetTable);

        // Return new record set
        return new CarbondataRecordSet(targetTable,/*connector,*/ session, /*config, */cdSplit, handles.build(), queryModel);
    }

    // Build filter for QueryModel (copy from CarbonInputFormat=> createRecordReader)
    private void fillFilter2QueryModel(QueryModel queryModel, TupleDomain<ColumnHandle> originalConstraint, CarbonTable carbonTable) {

        //queryModel.setFilterExpressionResolverTree(new FilterResolverIntf());

        //Build Predicate Expression
        ImmutableList.Builder<Expression> filters = ImmutableList.builder();

        Domain domain = null;

        for (ColumnHandle c : originalConstraint.getDomains().get().keySet()) {

            // Build ColumnExpresstion for Expresstion(Carbondata)
            CarbondataColumnHandle cdch = (CarbondataColumnHandle) c;
            Type type = cdch.getColumnType();

            DataType coltype = Spi2CarbondataTypeMapper(type);
            Expression colExpression = new ColumnExpression(cdch.getColumnName(), coltype);

            domain = originalConstraint.getDomains().get().get(c);
            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

            if (domain.getValues().isNone()) {
                //return QueryBuilders.filteredQuery(null, FilterBuilders.missingFilter(columnName));
                //return domain.isNullAllowed() ? columnName + " IS NULL" : "FALSE";
                //new Expression()
            }

            if (domain.getValues().isAll()) {
                //return QueryBuilders.filteredQuery(null, FilterBuilders.existsFilter(columnName));
                //return domain.isNullAllowed() ? "TRUE" : columnName + " IS NOT NULL";
            }

            List<Object> singleValues = new ArrayList<>();
            List<Expression> rangeFilter = new ArrayList<>();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else
                {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        Object value = ConvertDataByType(range.getLow().getValue(), type);
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                if (type == TimestampType.TIMESTAMP) {
                                    //todo not now
                                } else {
                                    GreaterThanExpression greater = new GreaterThanExpression(colExpression, new LiteralExpression(value, coltype));
                                    //greater.setRangeExpression(true);
                                    rangeFilter.add(greater);
                                }
                                break;
                            case EXACTLY:
                                GreaterThanEqualToExpression greater = new GreaterThanEqualToExpression(colExpression, new LiteralExpression(value, coltype));
                                //greater.setRangeExpression(true);
                                rangeFilter.add(greater);
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
                                LessThanEqualToExpression less = new LessThanEqualToExpression(colExpression, new LiteralExpression(value, coltype));
                                //less.setRangeExpression(true);
                                rangeFilter.add(less);
                                break;
                            case BELOW:
                                LessThanExpression less2 = new LessThanExpression(colExpression, new LiteralExpression(value, coltype));
                                //less2.setRangeExpression(true);
                                rangeFilter.add(less2);
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                }
            }

            if (singleValues.size() == 1) {
                Expression ex = null;
                if (coltype.equals(DataType.STRING)) {
                    ex = new EqualToExpression(colExpression, new LiteralExpression(((Slice) singleValues.get(0)).toStringUtf8(), coltype));
                } else
                    ex = new EqualToExpression(colExpression, new LiteralExpression(singleValues.get(0), coltype));
                filters.add(ex);
            }
            else if(singleValues.size() > 1) {
                ListExpression candidates = null;
                List<Expression> exs = singleValues.stream().map((a) ->
                {
                    return new LiteralExpression(ConvertDataByType(a, type), coltype);
                }).collect(Collectors.toList());
                candidates = new ListExpression(exs);

                if(candidates != null)
                    filters.add(new InExpression(colExpression, candidates));
            }
            else if(rangeFilter.size() > 0){
                if(rangeFilter.size() > 1) {
                    Expression finalFilters = new OrExpression(rangeFilter.get(0), rangeFilter.get(1));
                    if(rangeFilter.size() > 2)
                    {
                        for(int i = 2; i< rangeFilter.size(); i++)
                        {
                            filters.add(new AndExpression(finalFilters, rangeFilter.get(i)));
                        }
                    }
                }
                else if(rangeFilter.size() == 1)
                    filters.add(rangeFilter.get(0));
            }
        }

        Expression finalFilters;
        List<Expression> tmp = filters.build();
        if(tmp.size() > 1) {
            finalFilters = new AndExpression(tmp.get(0), tmp.get(1));
            if(tmp.size() > 2)
            {
                for(int i = 2; i< tmp.size(); i++)
                {
                    finalFilters = new AndExpression(finalFilters, tmp.get(i));
                }
            }
        }
        else if(tmp.size() == 1)
            finalFilters = tmp.get(0);
        else
            return;

        // todo set into QueryModel
        CarbonInputFormatUtil.processFilterExpression(finalFilters, carbonTable);
        queryModel.setFilterExpressionResolverTree(CarbonInputFormatUtil.resolveFilter(finalFilters, queryModel.getAbsoluteTableIdentifier()));
    }

    public static DataType Spi2CarbondataTypeMapper(Type colType)
    {
        if(colType == BooleanType.BOOLEAN)
            return DataType.BOOLEAN;
        else if(colType == SmallintType.SMALLINT)
            return DataType.SHORT;
        else if(colType == IntegerType.INTEGER)
            return DataType.INT;
        else if(colType == BigintType.BIGINT)
            return DataType.LONG;
        else if(colType == DoubleType.DOUBLE)
            return DataType.DOUBLE;
        else if(colType == DecimalType.createDecimalType())
            return DataType.DECIMAL;
        else if(colType == VarcharType.VARCHAR)
            return DataType.STRING;
        else if(colType == DateType.DATE)
            return DataType.DATE;
        else if(colType == TimestampType.TIMESTAMP)
            return DataType.TIMESTAMP;
        else
            return DataType.STRING;
    }


    public Object ConvertDataByType(Object rawdata, Type type)
    {
        if(type.equals(IntegerType.INTEGER))
            return new Integer((rawdata.toString()));
        else if(type.equals(BigintType.BIGINT))
            return (Long)rawdata;
        else if(type.equals(VarcharType.VARCHAR))
            return ((Slice)rawdata).toStringUtf8();
        else if(type.equals(BooleanType.BOOLEAN))
            return (Boolean)(rawdata);

        return rawdata;
    }
}
