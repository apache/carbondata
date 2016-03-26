/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.mondrian.extensions;

public class MolapSegmentLoader //extends SegmentLoader
{

    //    /**
    //     *
    //     */
    //    private MolapStatement stmt = null;
    //
    //    @Override
    //    protected SqlStatement createExecuteSql(GroupingSetsList groupingSetsList, List<StarPredicate> compoundPredicateList)
    //    {
    //        if(stmt != null)
    //        {
    //            return stmt;
    //        }
    //        final RolapStar star = groupingSetsList.getStar();
    //        BitKey levelBitKey = groupingSetsList.getDefaultLevelBitKey();
    //        BitKey measureBitKey = groupingSetsList.getDefaultMeasureBitKey();
    //        if(null!=compoundPredicateList)
    //        {
    //        Iterator<StarPredicate> iterator = compoundPredicateList.iterator();
    //        while(iterator.hasNext())
    //        {
    //        	StarPredicate next = iterator.next();
    //        	levelBitKey=levelBitKey.or(next.getConstrainedColumnBitKey());
    //        }
    //        }
    //        // Check if using aggregates is enabled.
    ////        boolean isPaginationQuery=false;
    //
    //        boolean needToFetchDataFromAggTable=true;
    //      //CHECKSTYLE:OFF
    //        if(MondrianProperties.instance().UseAggregates.get())
    //        {
    //            needToFetchDataFromAggTable = checkIfPagination(compoundPredicateList);
    //            if(!needToFetchDataFromAggTable)
    //            {
    //                final boolean[] rollup = {false};
    //                AggStar aggStar = AggregationManager.instance().findAgg(star, levelBitKey, measureBitKey, rollup);
    //
    //                if(aggStar != null)
    //                {
    //                    // Got a match, hot damn
    //
    //                    AggQuerySpec aggQuerySpec = new AggQuerySpec(aggStar, rollup[0], groupingSetsList);
    //                    // Pair<String, List<Type>> sql =
    //                    // aggQuerySpec.generateSqlQuery();
    //                    AggStar.FactTable.Measure msr = (AggStar.FactTable.Measure)aggQuerySpec.getMeasureAsColumn(0);
    //                    String aggTableAlias = msr.getExpression().getTableAlias();
    //                    long size = 0;
    //                    try
    //                    {
    //                        size = MolapExecutorFactory.getMolapExecutor(null, star.getDataSource()).executeTableCount(
    //                                aggTableAlias);
    //                    }
    //                    catch(IOException e)
    //                    {
    //                        size = 0;
    //                    }
    //                    if(size > 0)
    //                    {
    //                        // MolapStatement hbaseStatement = new
    //                        // MolapStatement(star.getDataSource(), "", null, 0, 0,
    //                        // new Locus(
    //                        // Locus.peek().execution, "Segment.load",
    //                        // "Error while loading segment"), -1, -1);
    //                        // hbaseStatement.execute(aggQuerySpec);
    //                        // return hbaseStatement;
    //                        // RolapStar star = groupingSetsList.getStar();
    //                        MolapSegmentArrayQuerySpec spec = new MolapSegmentArrayQuerySpec(groupingSetsList,
    //                                compoundPredicateList);
    //
    //                        // Pair<Scan, SqlQuery> p = spec.generateHbaseQuery();
    //
    //                        MolapStatement hbaseStatement = new MolapStatement(star.getDataSource(), "", null, 0, 0,
    //                                new Locus(Locus.peek().execution, "Segment.load", "Error while loading segment"), -1,
    //                                -1);
    //
    //                        hbaseStatement.execute(spec, aggStar.getFactTable().getName());
    //
    //                        return hbaseStatement;
    //                    }
    //                }
    //            }
    //        }
    //      //CHECKSTYLE:ON
    //        // RolapStar star = groupingSetsList.getStar();
    //        MolapSegmentArrayQuerySpec spec = new MolapSegmentArrayQuerySpec(groupingSetsList, compoundPredicateList);
    //
    //        // Pair<Scan, SqlQuery> p = spec.generateHbaseQuery();
    //
    //        MolapStatement hbaseStatement = new MolapStatement(star.getDataSource(), "", null, 0, 0, new Locus(
    //                Locus.peek().execution, "Segment.load", "Error while loading segment"), -1, -1);
    //
    //        hbaseStatement.execute(spec, null);
    //
    //        return hbaseStatement;
    //
    //    }
    //
    //    private boolean checkIfPagination(List<StarPredicate> compoundPredicateList)
    //    {
    //        boolean isPaginationQuery=false;
    //        Query query= (Query)RolapConnection.THREAD_LOCAL.get().get("QUERY_OBJ");
    //        if(query != null)
    //        {
    //            MolapQuery molapQuery = (MolapQuery)query.getAttribute("MOLAP_QUERY");
    //            MolapQueryImpl queryImpl = (MolapQueryImpl) molapQuery;
    //            if(null != queryImpl && null != queryImpl.getExtraProperties()
    //                    && null != queryImpl.getExtraProperties().get("PAGINATION_REQUIRED"))
    //            {
    //                isPaginationQuery=Boolean.parseBoolean(queryImpl.getExtraProperties().get("PAGINATION_REQUIRED").toString());
    //                if(isPaginationQuery)
    //                {
    //                   return true;
    //                }
    //            }
    //        }
    //        return false;
    //    }
    //
    //    /**
    //     * @see
    //     *      mondrian.rolap.agg.SegmentLoader#processData(mondrian.rolap.SqlStatement
    //     *      , boolean[], java.util.SortedSet<java.lang.Comparable<?>>[],
    //     *      mondrian.rolap.agg.GroupingSetsList)
    //     */
    //    public RowList processData(SqlStatement stmt1, final boolean[] axisContainsNull,
    //            final SortedSet<Comparable<?>>[] axisValueSets, final GroupingSetsList groupingSetsList)
    //            throws SQLException
    //    {
    //        //
    //        List<Segment> segments = groupingSetsList.getDefaultSegments();
    //        MolapStatement statement = (MolapStatement)stmt1;
    //        int measureCount = segments.size();
    //        MolapResultHolder rawRows = statement.getIterator();
    //        final List<SqlStatement.Type> types = statement.guessTypes();
    //        int arity = axisValueSets.length;
    //        final int groupingColumnStartIndex = arity + measureCount;
    //
    //        // If we're using grouping sets, the SQL query will have a number of
    //        // indicator columns, and we roll these into a single BitSet column in
    //        // the processed data set.
    //        final List<SqlStatement.Type> processedTypes;
    //        if(groupingSetsList.useGroupingSets())
    //        {
    //            processedTypes = new ArrayList<SqlStatement.Type>(types.subList(0, groupingColumnStartIndex));
    //            processedTypes.add(SqlStatement.Type.OBJECT);
    //        }
    //        else
    //        {
    //            processedTypes = rawRows.getDataTypes();
    //        }
    //        //
    //        final RowList processedRows = new RowList(processedTypes, 100);
    //        //
    //        while(rawRows.isNext())
    //        {
    //            ++statement.rowCount;
    //            processedRows.createRow();
    //            // get the columns
    //            int columnIndex = 0;
    //            columnIndex = getColumnsAndProcess(axisContainsNull, axisValueSets, groupingSetsList, rawRows, arity,
    //                    groupingColumnStartIndex, processedTypes, processedRows, columnIndex);
    //
    //            // pre-compute which measures are numeric
    //            final boolean[] numeric = new boolean[measureCount];
    //            getNumericMeasuresAndSetProcessedRowsObjects(groupingSetsList, segments, measureCount, rawRows,
    //                    processedTypes, processedRows, columnIndex, numeric);
    //        }
    //        return processedRows;
    //    }
    //
    //    /**
    //     *
    //     * @param axisContainsNull
    //     * @param axisValueSets
    //     * @param groupingSetsList
    //     * @param rawRows
    //     * @param arity
    //     * @param groupingColumnStartIndex
    //     * @param processedTypes
    //     * @param processedRows
    //     * @param columnIndex
    //     * @return
    //     * @throws SQLException
    //     */
    //    private int getColumnsAndProcess(final boolean[] axisContainsNull, final SortedSet<Comparable<?>>[] axisValueSets,
    //            final GroupingSetsList groupingSetsList, MolapResultHolder rawRows, int arity,
    //            final int groupingColumnStartIndex, final List<SqlStatement.Type> processedTypes,
    //            final RowList processedRows, int columnIndex) throws SQLException
    //    {
    //        for(int axisIndex = 0;axisIndex < arity;axisIndex++, columnIndex++)
    //        {
    //            final SqlStatement.Type type = processedTypes.get(columnIndex);
    //            switch(type)
    //            {
    //            //
    //            case OBJECT:
    //            case STRING:
    //                Object o = rawRows.getObject(columnIndex + 1);
    //                processRowsForString(axisContainsNull, axisValueSets, groupingSetsList, rawRows,
    //                        groupingColumnStartIndex, processedRows, columnIndex, axisIndex, o);
    //                break;
    //            //
    //            case INT:
    //                final int intValue = rawRows.getInt(columnIndex + 1);
    //                processRowsForInteger(axisContainsNull, axisValueSets, groupingSetsList, rawRows,
    //                        groupingColumnStartIndex, processedRows, columnIndex, axisIndex, intValue);
    //                break;
    //            //
    //            case LONG:
    //                final long longValue = rawRows.getLong(columnIndex + 1);
    //                processRowsForLong(axisContainsNull, axisValueSets, groupingSetsList, rawRows,
    //                        groupingColumnStartIndex, processedRows, columnIndex, axisIndex, longValue);
    //                break;
    //            //
    //            case DOUBLE:
    //                final double doubleValue = rawRows.getDouble(columnIndex + 1);
    //                processRowsForDouble(axisContainsNull, axisValueSets, groupingSetsList, rawRows,
    //                        groupingColumnStartIndex, processedRows, columnIndex, axisIndex, doubleValue);
    //                break;
    //            default:
    //                throw Util.unexpected(type);
    //            }
    //        }
    //        return columnIndex;
    //    }
    //
    //    /**
    //     *
    //     * @param groupingSetsList
    //     * @param segments
    //     * @param measureCount
    //     * @param rawRows
    //     * @param processedTypes
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param numeric
    //     * @throws SQLException
    //     */
    //    private void getNumericMeasuresAndSetProcessedRowsObjects(final GroupingSetsList groupingSetsList,
    //            List<Segment> segments, int measureCount, MolapResultHolder rawRows,
    //            final List<SqlStatement.Type> processedTypes, final RowList processedRows, int columnIndex,
    //            final boolean[] numeric) throws SQLException
    //    {
    //        int k = 0;
    //        for(Segment segment : segments)
    //        {
    //            numeric[k++] = segment.measure.getDatatype().isNumeric();
    //        }
    //
    //        // get the measure
    //        for(int i = 0;i < measureCount;i++, columnIndex++)
    //        {
    //            final SqlStatement.Type type = processedTypes.get(columnIndex);
    //            //added for source monitor fix
    //            processBasedOnType(segments, rawRows, processedRows, columnIndex, numeric, i, type);
    //        }
    //
    //        if(groupingSetsList.useGroupingSets())
    //        {
    //            processedRows.setObject(columnIndex,
    //                    getRollupBitKey(groupingSetsList.getRollupColumns().size(), rawRows, columnIndex));
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param segments
    //     * @param rawRows
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param numeric
    //     * @param i
    //     * @param type
    //     *
    //     */
    //    private void processBasedOnType(List<Segment> segments, MolapResultHolder rawRows, final RowList processedRows,
    //            int columnIndex, final boolean[] numeric, int i, final SqlStatement.Type type)
    //    {
    //        switch(type)
    //        {
    //        case OBJECT:
    //        case STRING:
    //            Object o = rawRows.getObject(columnIndex + 1);
    //            processRowsInCaseString(segments, processedRows, columnIndex, numeric, i, o);
    //            break;
    //        case INT:
    //            final int intValue = rawRows.getIntValue(columnIndex + 1);
    //            processRowsInCaseInteger(rawRows, processedRows, columnIndex, intValue);
    //            break;
    //        case LONG:
    //            final long longValue = rawRows.getLongValue(columnIndex + 1);
    //            processRowsInCaseLong(rawRows, processedRows, columnIndex, longValue);
    //            break;
    //        case DOUBLE:
    //            final double doubleValue =rawRows.getDoubleValue(columnIndex + 1);
    //            processRowsInCaseDouble(rawRows, processedRows, columnIndex, doubleValue);
    //            break;
    //        default:
    //            throw Util.unexpected(type);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param rawRows
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param doubleValue
    //     *
    //     */
    //    private void processRowsInCaseDouble(MolapResultHolder rawRows, final RowList processedRows, int columnIndex,
    //            final double doubleValue)
    //    {
    //        processedRows.setDouble(columnIndex, doubleValue);
    //        if(doubleValue == 0 && rawRows.wasNull())
    //        {
    //            processedRows.setNull(columnIndex, true);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param rawRows
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param longValue
    //     *
    //     */
    //    private void processRowsInCaseLong(MolapResultHolder rawRows, final RowList processedRows, int columnIndex,
    //            final long longValue)
    //    {
    //        processedRows.setLong(columnIndex, longValue);
    //        if(longValue == 0 && rawRows.wasNull())
    //        {
    //            processedRows.setNull(columnIndex, true);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param rawRows
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param intValue
    //     *
    //     */
    //    private void processRowsInCaseInteger(MolapResultHolder rawRows, final RowList processedRows, int columnIndex,
    //            final int intValue)
    //    {
    //        processedRows.setInt(columnIndex, intValue);
    //        if(intValue == 0 && rawRows.wasNull())
    //        {
    //            processedRows.setNull(columnIndex, true);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param segments
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param numeric
    //     * @param i
    //     * @param o
    //     *
    //     */
    //    private void processRowsInCaseString(List<Segment> segments, final RowList processedRows, int columnIndex,
    //            final boolean[] numeric, int i, Object o)
    //    {
    //        if(o == null)
    //        {
    //            o = Util.nullValue; // convert to placeholder
    //        }
    //        else
    //        {
    //                o = processObjectOrString(segments , numeric , i , o);
    //        }
    //        processedRows.setObject(columnIndex, o);
    //    }
    //
    //    /**
    //     *
    //     * @param axisContainsNull
    //     * @param axisValueSets
    //     * @param groupingSetsList
    //     * @param rawRows
    //     * @param groupingColumnStartIndex
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param axisIndex
    //     * @param doubleValue
    //     * @throws SQLException
    //     *
    //     */
    //    private void processRowsForDouble(final boolean[] axisContainsNull, final SortedSet<Comparable<?>>[] axisValueSets,
    //            final GroupingSetsList groupingSetsList, MolapResultHolder rawRows, final int groupingColumnStartIndex,
    //            final RowList processedRows, int columnIndex, int axisIndex, final double doubleValue) throws SQLException
    //    {
    //        if(doubleValue == 0 && rawRows.wasNull())
    //        {
    //            if(!groupingSetsList.useGroupingSets()
    //                    || !isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex))
    //            {
    //                axisContainsNull[axisIndex] = true;
    //            }
    //            processedRows.setNull(columnIndex, true);
    //        }
    //        axisValueSets[axisIndex].add(doubleValue);
    //        processedRows.setDouble(columnIndex, doubleValue);
    //    }
    //
    //    /**
    //     *
    //     * @param axisContainsNull
    //     * @param axisValueSets
    //     * @param groupingSetsList
    //     * @param rawRows
    //     * @param groupingColumnStartIndex
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param axisIndex
    //     * @param longValue
    //     * @throws SQLException
    //     *
    //     */
    //    private void processRowsForLong(final boolean[] axisContainsNull, final SortedSet<Comparable<?>>[] axisValueSets,
    //            final GroupingSetsList groupingSetsList, MolapResultHolder rawRows, final int groupingColumnStartIndex,
    //            final RowList processedRows, int columnIndex, int axisIndex, final long longValue) throws SQLException
    //    {
    //        if(longValue == 0 && rawRows.wasNull())
    //        {
    //            if(!groupingSetsList.useGroupingSets()
    //                    || !isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex))
    //            {
    //                axisContainsNull[axisIndex] = true;
    //            }
    //            processedRows.setNull(columnIndex, true);
    //        }
    //        else
    //        {
    //            axisValueSets[axisIndex].add(longValue);
    //            processedRows.setLong(columnIndex, longValue);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param axisContainsNull
    //     * @param axisValueSets
    //     * @param groupingSetsList
    //     * @param rawRows
    //     * @param groupingColumnStartIndex
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param axisIndex
    //     * @param intValue
    //     * @throws SQLException
    //     *
    //     */
    //    private void processRowsForInteger(final boolean[] axisContainsNull,
    //            final SortedSet<Comparable<?>>[] axisValueSets, final GroupingSetsList groupingSetsList,
    //            MolapResultHolder rawRows, final int groupingColumnStartIndex, final RowList processedRows,
    //            int columnIndex, int axisIndex, final int intValue) throws SQLException
    //    {
    //        if(intValue == 0 && rawRows.wasNull())
    //        {
    //            if(!groupingSetsList.useGroupingSets()
    //                    || !isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex))
    //            {
    //                axisContainsNull[axisIndex] = true;
    //            }
    //            processedRows.setNull(columnIndex, true);
    //        }
    //        else
    //        {
    //            axisValueSets[axisIndex].add(intValue);
    //            processedRows.setInt(columnIndex, intValue);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param axisContainsNull
    //     * @param axisValueSets
    //     * @param groupingSetsList
    //     * @param rawRows
    //     * @param groupingColumnStartIndex
    //     * @param processedRows
    //     * @param columnIndex
    //     * @param axisIndex
    //     * @param o
    //     * @throws SQLException
    //     *
    //     */
    //    private void processRowsForString(final boolean[] axisContainsNull, final SortedSet<Comparable<?>>[] axisValueSets,
    //            final GroupingSetsList groupingSetsList, MolapResultHolder rawRows, final int groupingColumnStartIndex,
    //            final RowList processedRows, int columnIndex, int axisIndex, Object o) throws SQLException
    //    {
    //        if(o == null || o == RolapUtil.sqlNullValue)
    //        {
    //            o = RolapUtil.sqlNullValue;
    //            if(!groupingSetsList.useGroupingSets()
    //                    || !isAggregateNull(rawRows, groupingColumnStartIndex, groupingSetsList, axisIndex))
    //            {
    //                axisContainsNull[axisIndex] = true;
    //            }
    //        }
    //        else
    //        {
    //            axisValueSets[axisIndex].add(Aggregation.Axis.wrap(o));
    //        }
    //        processedRows.setObject(columnIndex, o);
    //    }
    //
    //    /**
    //     * @param rowList
    //     * @param groupingColumnStartIndex
    //     * @param groupingSetsList
    //     * @param axisIndex
    //     * @return
    //     * @throws SQLException
    //     */
    //    public boolean isAggregateNull(MolapResultHolder rowList, int groupingColumnStartIndex, GroupingSetsList groupingSetsList,
    //            int axisIndex) throws SQLException
    //    {
    //        int groupingFunctionIndex = groupingSetsList.findGroupingFunctionIndex(axisIndex);
    //        if(groupingFunctionIndex == -1)
    //        {
    //            // Not a rollup column
    //            return false;
    //        }
    //        return new Long(rowList.getObject(groupingColumnStartIndex + groupingFunctionIndex + 1).toString()).intValue() == 1;
    //    }
    //
    //    BitKey getRollupBitKey(int arity, MolapResultHolder rowList, int k) throws SQLException
    //    {
    //        BitKey groupingBitKey = BitKey.Factory.makeBitKey(arity);
    //        for(int i = 0;i < arity;i++)
    //        {
    //            int o = ((Integer)rowList.getObject(k + i + 1)).intValue();
    //            if(o == 1)
    //            {
    //                groupingBitKey.set(i);
    //            }
    //        }
    //        return groupingBitKey;
    //    }
    //
    //    /**
    //     * @param groupingSets
    //     * @param pinnedSegments
    //     * @param compoundPredicateList
    //     * @param stmt
    //     */
    //    public void load(List<GroupingSet> groupingSets, RolapAggregationManager.PinSet pinnedSegments,
    //            List<StarPredicate> compoundPredicateList, MolapStatement stmt)
    //    {
    //        this.stmt = stmt;
    //        super.load(groupingSets, pinnedSegments, compoundPredicateList);
    //
    //    }

}
