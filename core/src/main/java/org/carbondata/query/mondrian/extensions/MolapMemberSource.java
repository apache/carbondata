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

public class MolapMemberSource //extends SqlMemberSource
{

    //    public MolapMemberSource(RolapHierarchy hierarchy)
    //    {
    //        super(hierarchy);
    //    }
    //
    //    /**
    //     * @see mondrian.rolap.SqlMemberSource#getMemberChildren(mondrian.rolap.RolapMember,
    //     *      java.util.List, mondrian.rolap.sql.MemberChildrenConstraint)
    //     */
    //    public void getMemberChildren(RolapMember parentMember, List<RolapMember> children,
    //            MemberChildrenConstraint constraint)
    //    {
    //        // allow parent child calculated members through
    //        // this fixes the non closure parent child hierarchy bug
    //        if(!parentMember.isAll() && parentMember.isCalculated() && !parentMember.getLevel().isParentChild())
    //        {
    //            return;
    //        }
    //        getMemberChildren2(parentMember, children, constraint);
    //    }
    //
    //    @Override
    //    public List<RolapMember> getMembersInLevel(RolapLevel level, int startOrdinal, int endOrdinal,
    //            TupleConstraint constraint)
    //    {
    //
    //        if(level.isAll())
    //        {
    //            final List<RolapMember> list = new ArrayList<RolapMember>();
    //            list.add(getHierarchy().getAllMember());
    //            return list;
    //        }
    //        return getMembersInLevel(level, constraint);
    //    }
    //
    //    private List<RolapMember> getMembersInLevel(RolapLevel level, TupleConstraint constraint)
    //    {
    //        final MolapTupleReader tupleReader = new MolapTupleReader(constraint, (RolapSchema)level.getDimension().getSchema(),level);
    //        tupleReader.addLevelMembers(level, this, null);
    //        final TupleList tupleList = tupleReader.readTuples(getHierarchy().getRolapSchema().getInternalConnection()
    //                .getDataSource(), null, null);
    //
    //        assert tupleList.getArity() == 1;
    //        return Util.cast(tupleList.slice(0));
    //    }
    //
    //    private void getMemberChildren2(RolapMember parentMember, List<RolapMember> children,
    //            MemberChildrenConstraint constraint)
    //    {
    //        boolean parentChild;
    //        final RolapLevel parentLevel = parentMember.getLevel();
    //        RolapLevel childLevel;
    //        if(parentLevel.isParentChild())
    //        {
    //            parentChild = true;
    //            childLevel = parentLevel;
    //        }
    //        else
    //        {
    //            childLevel = (RolapLevel)parentLevel.getChildLevel();
    //            if(childLevel == null)
    //            {
    //                // member is at last level, so can have no children
    //                return;
    //            }
    //            if(childLevel.isParentChild())
    //            {
    //                parentChild = true;
    //            }
    //            else
    //            {
    //                parentChild = false;
    //            }
    //        }
    //        RolapLevel level = (RolapLevel)parentMember.getLevel().getChildLevel();
    //        Map<String, MolapFilterInfo> cons = new HashMap<String, MolapFilterInfo>();
    //        //MolapFilterInfo tempContraint;
    //
    //        //Method added for source monitor fix
    //        setConstraints(parentMember, constraint, level, cons);
    //
    //        AggStar aggStar = chooseAggStar(constraint, parentMember);
    //
    //        DataSource datasource = getHierarchy().getRolapSchema().getInternalConnection().getDataSource();
    //
    //        MolapStatement statement = new MolapStatement(datasource);
    //        String hName = getHierarchy().getSubName() == null ? getHierarchy().getName() : getHierarchy().getSubName();
    //
    //        String cubeName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
    //        String schemaName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
    //        statement.execute(cons, hName, getHierarchy().getLevels(), aggStar, true, schemaName+'_'+cubeName, null,true,new RolapLevel[]{level});
    //
    //        try
    //        {
    //            //added method for source monitor fix
    //            iterateAndAddMembers(parentMember, children, parentChild, childLevel, statement);
    //        }
    //        catch(SQLException e)
    //        {
    //            throw statement.handle(e);
    //        }
    //        /*
    //         * finally { // stmt.close(); }
    //         */
    //    }
    //
    //    private void setConstraints(RolapMember parentMember, MemberChildrenConstraint constraint, RolapLevel level,
    //            Map<String, MolapFilterInfo> cons)
    //    {
    //        MolapFilterInfo tempContraint;
    //        if(constraint instanceof SqlContextConstraint)
    //        {
    //            CellRequest request = ((SqlContextConstraint)constraint).getCellRequestForConstraint(null, parentMember);
    //            //Coverity Fix added null check
    //            if( null != request)
    //            {
    //                RolapStar.Column[] columns = request.getConstrainedColumns();
    //                Object[] values = request.getSingleValues();
    //                int arity = columns.length;
    //
    //                // following code is similar to
    //                // AbstractQuerySpec#nonDistinctGenerateSQL()
    //
    //                putValuesInConsMap(cons, columns, values, arity);
    //            }
    //        }
    //        else if(constraint instanceof ChildByNameConstraint)
    //        {
    //            /*
    //             * RolapMember member = parentMember; while(!member.isAll()) {
    //             * List<String> list = new ArrayList<String>();
    //             * list.add(member.getName());
    //             * cons.put(((MondrianDef.Column)member.getLevel
    //             * ().getKeyExp()).name, list); member = member.getParentMember(); }
    //             * List<String> list = new ArrayList<String>();
    //             * list.add(((Id.Segment
    //             * )((List)((ChildByNameConstraint)constraint).getCacheKey
    //             * ()).get(1)).name); // addConstr(cons, levels,
    //             * ((MondrianDef.Column)level.getKeyExp()).name, list);
    //             * cons.put(((MondrianDef.Column)level.getKeyExp()).name, list);
    //             */
    //
    //            makeConstraintBasedOnParent(parentMember, cons);
    //            tempContraint = new MolapFilterInfo();
    //            tempContraint.addIncludedMembers('['+((Id.Segment)((List)((ChildByNameConstraint)constraint).getCacheKey())
    //                    .get(1)).name+']');
    //
    //            cons.put(((MondrianDef.Column)level.getKeyExp()).table+'_'+((MondrianDef.Column)level.getKeyExp()).name, tempContraint);
    //
    //        }
    //        else if(constraint instanceof DefaultMemberChildrenConstraint)
    //        {
    //            // Make the constraint based on parent
    //            makeConstraintBasedOnParent(parentMember, cons);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param cons
    //     * @param columns
    //     * @param values
    //     * @param arity
    //     */
    //    private void putValuesInConsMap(Map<String, MolapFilterInfo> cons, RolapStar.Column[] columns, Object[] values,
    //            int arity)
    //    {
    //        MolapFilterInfo tempContraint;
    //        for(int i = 0;i < arity;i++)
    //        {
    //            RolapStar.Column column = columns[i];
    //            tempContraint = new MolapFilterInfo();
    //            tempContraint.addIncludedMembers(String.valueOf(values[i]));
    //
    //            // addConstr(cons, levels,
    //            // ((MondrianDef.Column)column.getExpression()).name, list);
    //            cons.put(((MondrianDef.Column)column.getExpression()).table+'_'+((MondrianDef.Column)column.getExpression()).name, tempContraint);
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param parentMember
    //     * @param cons
    //     */
    //    private void makeConstraintBasedOnParent(RolapMember parentMember, Map<String, MolapFilterInfo> cons)
    //    {
    //        MolapFilterInfo tempContraint;
    //        RolapMember member = parentMember;
    //        while(member != null && !member.isAll())
    //        {
    //            tempContraint = new MolapFilterInfo();
    //            tempContraint.addIncludedMembers('['+member.getName()+']');
    //            cons.put(((MondrianDef.Column)member.getLevel().getKeyExp()).table+'_'+((MondrianDef.Column)member.getLevel().getKeyExp()).name, tempContraint);
    //            member = member.getParentMember();
    //        }
    //    }
    //
    //    /**
    //     *
    //     * @param parentMember
    //     * @param children
    //     * @param parentChild
    //     * @param childLevel
    //     * @param statement
    //     * @throws SQLException
    //     *
    //     */
    //    private void iterateAndAddMembers(RolapMember parentMember, List<RolapMember> children, boolean parentChild,
    //            RolapLevel childLevel, MolapStatement statement) throws SQLException
    //    {
    //        String resultLimit = MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_RESULT_SIZE_KEY,MolapCommonConstants.MOLAP_RESULT_SIZE_DEFAULT);
    //        int limit = Integer.parseInt(resultLimit);
    //        boolean checkCacheStatus = true;
    //
    //            MolapResultHolder resultSet = statement.getIterator();
    //            RolapMember parentMember2 = RolapUtil.strip(parentMember);
    //            final List<SqlStatement.Accessor> accessors =
    //                    statement.getAccessors();
    //            while(resultSet.isNext())
    //            {
    //                ++statement.rowCount;
    //                if(limit > 0 && limit < statement.rowCount)
    //                {
    //                    // result limit exceeded, throw an exception
    //                    throw MondrianResource.instance().MemberFetchLimitExceeded.ex(limit);
    //                }
    //
    //            Object value = accessors.get(0).get();
    //            if(value == null)
    //            {
    //                value = RolapUtil.sqlNullValue;
    //            }
    //            // Object captionValue = null;
    //            int columnOffset = 1;
    //            /*
    //             * if(!childLevel.hasCaptionColumn()) { // The columnOffset needs to
    //             * take into account // the caption column if one exists //
    //             * captionValue = accessors.get(columnOffset++).get(); } else {
    //             * captionValue = null; }
    //             */
    //            Object key = getMemberCache().makeKey(parentMember2, value);
    //            RolapMember member = getMemberCache().getMember(key, checkCacheStatus);
    //            checkCacheStatus = false; /* Only check the first time */
    //            if(member == null)
    //            {
    //                member = makeMember(parentMember2, childLevel, value, null, parentChild, statement, key,
    //                        columnOffset);
    //            }
    //            if(value.equals(RolapUtil.sqlNullValue))
    //            {
    //                children.toArray();
    //                addAsOldestSibling(children, member);
    //            }
    //            else
    //            {
    //                children.add(member);
    //            }
    //        }
    //    }
    //
    //    /**
    //     * Overridden method from SqlMemberSource tp perform member count
    //     *
    //     * @see mondrian.rolap.SqlMemberSource#getMemberCount(mondrian.rolap.RolapLevel, javax.sql.DataSource)
    //     *
    //     */
    //
    //
    //    protected int getMemberCount(RolapLevel level, DataSource datasource) {
    //
    //        MolapStatement statement = new MolapStatement(datasource);
    //
    //        String dimName = ((MondrianDef.Column)level.getKeyExp()).name;
    //
    //        String schemaName = getHierarchy().getRolapSchema().getName();
    //
    //        statement.executeCount(dimName, schemaName);
    //
    //        //Modified the return for handling Null Iterator
    //        //mantis defect : 0003151
    //        int count = 0;
    //        if(null!=statement.getIterator() && statement.getIterator().isNext())
    //        {
    //            count = ((Double)statement.getIterator().getObject(1)).intValue();
    //        }
    //        return count;
    //
    //       //return statement.getIterator().getColumnCount();
    //
    //
    //    }

}
