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

package com.huawei.unibi.molap.engine.mondrian.extensions;


/**
 * @author R00900208
 * 
 */
public class MolapTupleReader //extends SqlTupleReader
{

//    private static final String ZERO = "*ZERO";
//
//    private static final String IS_EMPTY = "IsEmpty";
//
//    private static final String NOT = "NOT";
//
//    private static final String OR = "OR";
//
//    /**
//     * 
//     */
//    final AggregationManager aggMgr = AggregationManager.instance();
//
//    /**
//     * 
//     */
//    private RolapSchema schema;
//    
//    /**
//     * whether it has been called for reading members
//     */
//    private boolean readingMembers = false;
//    
//    /**
//     * Attribute for Molap LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapTupleReader.class.getName());
//
//    public MolapTupleReader(TupleConstraint constraint, RolapSchema schema)
//    {
//        super(constraint);
//        this.schema = schema;
//    }
//    
//    public MolapTupleReader(TupleConstraint constraint, RolapSchema schema,RolapLevel rolapLevel)
//    {
//        super(constraint,rolapLevel);
//        this.schema = schema;
//        readingMembers = true;
//    }
//
//    @Override
//    protected void prepareTuples(DataSource dataSource, TupleList partialResult,
//            List<List<RolapMember>> newPartialResult)
//    {
//        try
//        {
//            
//            long startTime = System.currentTimeMillis();
//            String message = "Populating member cache with members for " + targets;
//            Evaluator evaluator = getEvaluator(constraint);
//            AggStar aggStar = chooseAggStar(constraint, evaluator);
//            boolean execQuery = (partialResult == null);
//
//            String cubeSchemaName = schema.getName();//(String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
//            String cubeName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
//            String cubeUniqueName = cubeSchemaName+'_'+cubeName;
//            Cube queryCube = MolapMetadata.getInstance().getCube(cubeUniqueName);
//            RolapCube cube = (RolapCube)schema.lookupCube(cubeName, true);
//            // }
//            MolapResultHolder hIterator = null;
//            // Map<String, List<String>> cons = new LinkedHashMap<String,
//            // List<String>>();
//            Map<String, MolapFilterInfo> constraints = new LinkedHashMap<String, MolapFilterInfo>();
//            Map<Measure, MeasureFilterModel[]> msrFilterConstraints = new LinkedHashMap<Measure, MeasureFilterModel[]>();
//            List<String> cols = new ArrayList<String>();
//            List<String> hierarchycols = new ArrayList<String>();
//            List<String> levelNames = new ArrayList<String>();
//            //added for source monitor fix
//            CellRequest request = processCellRequest(evaluator, aggStar, cube, constraints, cols,msrFilterConstraints,queryCube,hierarchycols,levelNames);
//            MolapQueryModel molapQueryModel = getMolapQueryModel(cubeSchemaName, cubeName);
//            boolean isPagination=isPaginationRequest(molapQueryModel);
////            try
////            {
//////                Map<String,Role> mapOfRoles=schema.getMapNameToRole();
//////            
//////                if(null!=mapOfRoles &&  mapOfRoles.size()>0)
//////                {
//////                    addRoleConstraintsInMolapTupleQuery(mapOfRoles,constraints,cube,cols,aggStar,request,hierarchycols,levelNames,evaluator);
//////                }
////            }
////            catch(IOException e)
////            {
////                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
////            }
//            
//            aggStar = updateAggStarIncaseOfCaculatedMemberAndPagination(aggStar, isPagination, molapQueryModel);
//            List<MolapMetadata.Measure> msrs = new ArrayList<MolapMetadata.Measure>();
//            List<RolapStar.Measure> msList = new ArrayList<RolapStar.Measure>();
//
//            // Only if from same hierarchy, then the hierarchy is returned;
//            RolapHierarchy hier = isAllColPresentInSameHierarichy(cube.getHierarchies(), cols);// :
//                                                                                               // targets.get(0).getLevel().getHierarchy();
//
//            MolapStatement statement = new MolapStatement(dataSource);
//
//            //added for source monitor fix
//            executeStatement(evaluator, aggStar, queryCube, cube, constraints, cols, msrs, msList, hier, statement,msrFilterConstraints);
//            LOGGER.info(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,"****************************** Actual Time Takes for Query Execution: "
//                            + (System.currentTimeMillis() - startTime));
//            //System.out.println("****************************** Actual Time Takes for Query Execution: "
//             //       + (System.currentTimeMillis() - startTime));
//            hIterator = statement.getIterator();
//            startTime = System.currentTimeMillis();
//            for(TargetBase target : targets)
//            {
//                target.open();
//            }
//
//            String resultLimit = MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_RESULT_SIZE_KEY,MolapCommonConstants.MOLAP_RESULT_SIZE_DEFAULT);
//            long limit = Long.parseLong(resultLimit);
//            int fetchCount = 0;
//
//            // determine how many enum targets we have
//            int enumTargetCount = getEnumTargetCount();
//            int[] srcMemberIdxes = null;
//            if(enumTargetCount > 0)
//            {
//                srcMemberIdxes = new int[enumTargetCount];
//            }
//
//            int currPartialResultIdx = 0;
//            // method added to reduce complexity
//            doOperationOnMoreRows(partialResult, newPartialResult, message, execQuery, hIterator, statement, limit,
//                    fetchCount, enumTargetCount, srcMemberIdxes, currPartialResultIdx);
//            LOGGER.info(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,"****************************** Time taken to set The targets: "
//                            + (System.currentTimeMillis() - startTime));
//           // System.out.println("****************************** Time taken to set The targets: "
//               //     + (System.currentTimeMillis() - startTime));
//            // have given this condition as i am not sure when mdx contains
//            // cross join instead of non empty cross join, we get evaluator as
//            // null or not null
//            startTime = System.currentTimeMillis();
//            if((null != request) && (evaluator != null || hier == null)) // if(evaluator == null &&
//                                                  // hier != null)
//            {
//                makeSegmentCache(cube, request, statement, msList,constraints);
//            }
//            LOGGER.info(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,"****************************** Time taken to set The segment cache: "
//                            + (System.currentTimeMillis() - startTime));
//            //System.out.println("****************************** Time taken to set The segment cache: "
//                //    + (System.currentTimeMillis() - startTime));
//        }
//        catch(ResultLimitExceededException r)
//        {
//            throw r;
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//    }
//
//    private AggStar updateAggStarIncaseOfCaculatedMemberAndPagination(AggStar aggStar, boolean isPagination,
//            MolapQueryModel molapQueryModel)
//    {
//        if(isPagination)
//        {
//            List<CalculatedMeasure> calcMsrs = molapQueryModel.getCalcMsrs();
//            if(calcMsrs.size()>0)
//            {
//                aggStar = null;
//            }
//        }
//        return aggStar;
//    }
//
//    private boolean isPaginationRequest(MolapQueryModel model) throws IOException
//    {
//        if(null!=model &&  model.isPaginationRequired())
//        {
//            return true;
//        }
//        return false;
//    }
//
//    
//    
//
//    private MolapQueryModel getMolapQueryModel(String cubeSchemaName, String cubeName) throws IOException 
//    {
//
//        Query query = (Query)RolapConnection.THREAD_LOCAL.get().get("QUERY_OBJ");
//        MolapQueryModel model =null;
//        MolapQuery molapQuery = null;
//        if(query != null)
//        {
//            molapQuery = (MolapQuery)query.getAttribute("MOLAP_QUERY");
//            if(null==molapQuery)
//            {
//                return null;
//            }
//            try
//            {
//                 model = MolapQueryParseUtil.parseMolapQuery(molapQuery, cubeSchemaName, cubeName);
//                 
//
//            }
//            catch(IOException e)
//            {
//                // TODO Auto-generated catch block
//                throw e;
//            }
//        }
//        return model;
//    }
//    
//    private MolapQuery getMolapQuery() 
//    {
//        Query query = (Query)RolapConnection.THREAD_LOCAL.get().get("QUERY_OBJ");
//        MolapQuery molapQuery = null;
//        if(query != null)
//        {
//            molapQuery = (MolapQuery)query.getAttribute("MOLAP_QUERY");
//            return molapQuery;
//        }
//       return molapQuery;
//    }
//
//    private void addRoleConstraintsInMolapTupleQuery( Map<String, Role> mapOfRoles,
//            Map<String, MolapFilterInfo> constraints, RolapCube cube, List<String> cols, AggStar aggStar, CellRequest request, List<String> hierarchycols, List<String> levelNames, Evaluator evaluator) throws IOException
//    {
//        List<String> roleNames =null;
//        if(null!=evaluator && null!=evaluator.getQuery())
//        {
//           if(null!=evaluator.getQuery().getConnection() && evaluator.getQuery().getConnection() instanceof InMemoryOlapConnection)
//           {
//               roleNames = getRoleConstraintsInMolapTupleQuery(evaluator);
//           }
//        }
//        List<RolapHierarchy> listOfCubeHierarchies=cube.getHierarchies();
//        Iterator<String> itr = mapOfRoles.keySet().iterator();
//        while(itr.hasNext())
//        {
//            String roleName = itr.next();
//            if(mapOfRoles.get(roleName) instanceof RoleImpl)
//            {
//                RoleImpl role = (RoleImpl)mapOfRoles.get(roleName);
//                if(null!=roleNames && roleNames.contains(roleName))
//                {
////                Map<Hierarchy, HierarchyAccessImpl> mapOfHierarchyRole = role.getHierarchyGrants();
////                    addRoleConstraintsIfExistInRequestCubeHierarchies(listOfCubeHierarchies, mapOfHierarchyRole,
////                            hierarchycols, constraints, cube, cols, aggStar, request, levelNames);
//                }
//
//            }
//
//        }
// 
//    }
//
//    /**
//     * @Author M00903915
//     * @Description : addRoleConstraintsInMolapTupleQuery
//     * @param evaluator
//     * @param roleNames
//     * @return
//     */
//    private List<String> getRoleConstraintsInMolapTupleQuery(Evaluator evaluator)
//    {
//        Util.PropertyList propertyList= ((InMemoryOlapConnection)evaluator.getQuery().getConnection()).getConnectInfo();
//           if(null!=propertyList)
//           {
//           String roleNameList =
//                   propertyList.get(RolapConnectionProperties.Role.name());
//               if (roleNameList != null) {
//                   return Util.parseCommaList(roleNameList);
//               }
//           }
//        return null;
//    }
//
//    private void addMemberConstraintForMolapQuery(Map<Member, Access> mapOfMemberGrant, 
//            Map<String, MolapFilterInfo> constraints, RolapCube cube, List<String> cols, AggStar aggStar, CellRequest request, List<String> hierarchycols, List<String> levelNames) throws IOException
//    {
//
//           Iterator<Member> itr = mapOfMemberGrant.keySet().iterator();
//            while(itr.hasNext())
//            {
//                Member member = itr.next();
//                if(member instanceof RolapCubeMember)
//                {
//                    
//               
//                    if(member.isAll())
//                    {
//                        continue;
//                    }
//                    
//                    if(levelNames.contains(((RolapCubeMember)member).getName()))
//                    {
//                        continue;
//                    }
//                  
//                    MolapQuery molapQuery=getMolapQuery();
//                    if(null!=molapQuery)
//                    {
//                    MolapQueryImpl queryImpl = (MolapQueryImpl) molapQuery;
//
//                    Map<String,Object> mapOfSliceProperty=new HashMap<String,Object>(15);
//                    mapOfSliceProperty.put("isSliceFilterPresent", true);
//                    if(null!=queryImpl.getExtraProperties())
//                    {
//                        queryImpl.getExtraProperties().putAll(mapOfSliceProperty);
//                    }
//                    else
//                    {
//                        molapQuery.setExtraProperties(mapOfSliceProperty);
//                    }
//                    }
//                    Access memberAccess= mapOfMemberGrant.get(member);
//                }
//            }
////                    if(memberAccess.CUSTOM==memberAccess)
////                    {
////                        continue;
////                    }
////                    addLeveMemberHbase(((RolapCubeMember)member).getLevel(),cube, aggStar, cols, request,null,null);
////                   
////                    boolean isExclude=false;//CHECKSTYLE:OFF
////                    if(memberAccess==Access.NONE)
////                    {//CHECKSTYLE:ON
////                        isExclude=true;
////                    }
////                    ((RolapCubeMember)member).setRestrictedMember(true);
////                    RolapUtil.addMolapConstraint(cube,
////                            constraints, request,
////                            isExclude, ((RolapCubeMember)member)); 
////                    }
//                    
////                    tempContraint.addIncludedMembers('[' + member.getName() + ']');
////                    ((RolapCubeMember)member).getLevel().getStarKeyColumn();
////                    constraints.put(((RolapCubeMember)member).getLevel().getStarKeyColumn().getTable().getTableName()
////                            + '_' + ((RolapCubeMember)member).getLevel().getStarKeyColumn().getName(), tempContraint);
////                }
//
//            
// 
//        }
//
//    
//
////    private void addRoleConstraintsIfExistInRequestCubeHierarchies(List<RolapHierarchy> listOfCubeHierarchies,
////            Map<Hierarchy, HierarchyAccessImpl> mapOfHierarchyRole, List<String> hierarchycols, Map<String, MolapFilterInfo> constraints, RolapCube cube, List<String> cols, AggStar aggStar, CellRequest request, List<String> levelNames) throws IOException 
////    {
////          
////        if(null!=mapOfHierarchyRole && mapOfHierarchyRole.size()>0)
////        {
////            Iterator<Hierarchy> itr=mapOfHierarchyRole.keySet().iterator();
////            while(itr.hasNext())
////            {
////                Hierarchy hierarchy=itr.next();
////                if(hierarchy.getDimension() instanceof RolapCubeDimension)
////                {
////                    RolapCubeDimension cubeDim=(RolapCubeDimension)hierarchy.getDimension();
////                    if(!cubeDim.getCube().getName().equals(cube.getName()))
////                    {
////                        continue;
////                    }
////                }
////                processRoleBasedOnHierarchies(listOfCubeHierarchies, mapOfHierarchyRole, hierarchycols, constraints,
////                        cube, cols, aggStar, request, levelNames, hierarchy);
////            }
////        }
////        
////    }
//
////    /**
////     * @Author M00903915
////     * @Description : processRoleBasedOnHierarchies
////     * @param listOfCubeHierarchies
////     * @param mapOfHierarchyRole
////     * @param hierarchycols
////     * @param constraints
////     * @param cube
////     * @param cols
////     * @param aggStar
////     * @param request
////     * @param levelNames
////     * @param hierarchy
////     * @throws IOException
////     */
////    private void processRoleBasedOnHierarchies(List<RolapHierarchy> listOfCubeHierarchies,
////            Map<Hierarchy, HierarchyAccessImpl> mapOfHierarchyRole, List<String> hierarchycols,
////            Map<String, MolapFilterInfo> constraints, RolapCube cube, List<String> cols, AggStar aggStar,
////            CellRequest request, List<String> levelNames, Hierarchy hierarchy) throws IOException
////    {
////        for(RolapHierarchy rolapHierarchy:listOfCubeHierarchies)
////        {//CHECKSTYLE:OFF
////            if(rolapHierarchy instanceof RolapCubeHierarchy
////                    && hierarchy instanceof RolapCubeHierarchy
////                    && ((RolapCubeHierarchy)rolapHierarchy).getCube().getName()
////                            .equals(((RolapCubeHierarchy)hierarchy).getCube().getName()))
////            {
////
////                if((rolapHierarchy.getName().equals(hierarchy.getName()) ) || (rolapHierarchy.getName().equals(hierarchy.getName()) && constraints.isEmpty()))
////                {
////                    HierarchyAccessImpl hierarchyWithRole = mapOfHierarchyRole.get(hierarchy);
////                    
////                    if(null != hierarchyWithRole)
////                    {
////                        Map<Member, Access> mapOfMemberGrant = hierarchyWithRole.getMemberGrants();
////                        if(null==mapOfMemberGrant || mapOfMemberGrant.size()==0)
////                        {
////                            continue;
////                        }
//////                                else if(hierarchyWithRole.getRollupPolicy() == RollupPolicy.FULL || (hierarchyWithRole.getRollupPolicy()==RollupPolicy.HIDDEN))
//////                                {
//////                                    continue;
//////                                }
////                        addMemberConstraintForMolapQuery(mapOfMemberGrant, constraints, cube, cols, aggStar,request,hierarchycols,levelNames);
////                    }
////                 
////                }
////            }//CHECKSTYLE:ON
////            else
////            {
////                if(rolapHierarchy.getName().equals(hierarchy.getName()) && !hierarchycols.contains(rolapHierarchy.getName()))
////                {
////                    HierarchyAccessImpl hierarchyWithRole = mapOfHierarchyRole.get(hierarchy);
////                    Map<Member, Access> mapOfMemberGrant = hierarchyWithRole.getMemberGrants();
////                    if(null==mapOfMemberGrant || mapOfMemberGrant.size()==0)
////                    {
////                        continue;
////                    }
////                    addMemberConstraintForMolapQuery(mapOfMemberGrant, constraints, cube, cols, aggStar, request,hierarchycols,levelNames);
////                }
////            }
////
////        }
////    }
//
//    /**
//     * 
//     * @param partialResult
//     * @param newPartialResult
//     * @param message
//     * @param execQuery
//     * @param hIterator
//     * @param statement
//     * @param limit
//     * @param fetchCount
//     * @param enumTargetCount
//     * @param srcMemberIdxes
//     * @param currPartialResultIdx
//     * @throws SQLException
//     */
//    private void doOperationOnMoreRows(TupleList partialResult, List<List<RolapMember>> newPartialResult,
//            String message, boolean execQuery, MolapResultHolder hIterator, MolapStatement statement, long limit,
//            int fetchCount, int enumTargetCount, int[] srcMemberIdxes, int currPartialResultIdx) throws SQLException
//    {
//        boolean moreRows;
//        if(execQuery)
//        {
//            moreRows = hIterator.isNext();
//            if(moreRows)
//            {
//                ++statement.rowCount;
//            }
//        }
//        else
//        {
//            moreRows = currPartialResultIdx < partialResult.size();
//        }
//        while(moreRows)
//        {
//            ++fetchCount;
////            if(limit > 0 && limit < fetchCount)
////            {
////                // result limit exceeded, throw an exception
////                throw MondrianResource.instance().MemberFetchLimitExceeded.ex(limit);
////            }
//
//            addEnumTargets(partialResult, newPartialResult, message, execQuery, statement, enumTargetCount,
//                    srcMemberIdxes, currPartialResultIdx);
//
//            if(execQuery)
//            {
//                moreRows = hIterator.isNext();
//                if(moreRows)
//                {
//                    ++statement.rowCount;
//                }
//            }
//            else
//            {
//                currPartialResultIdx++;
//                moreRows = currPartialResultIdx < partialResult.size();
//            }
//        }
//    }
//
//    private CellRequest processCellRequest(Evaluator evaluator, AggStar aggStar, RolapCube cube,
//            Map<String, MolapFilterInfo> constraints, List<String> cols,Map<Measure, MeasureFilterModel[]> msrFilterConstraints,Cube molapCube,List<String> hierarchyCols, List<String> levelNames)
//    {
//        CellRequest request = null;
//        
//        try
//        {
//            request = evaluator != null ? RolapAggregationManager.makeRequest((RolapEvaluator)evaluator)
//                : null;
//        }
//        catch (ClassCastException e)
//        {
//            if(constraint instanceof SetConstraint)
//            {
////                ((SetConstraint)constraint).addConstraintsInCaseOfSubtotal(cube, aggStar, constraints);
//            }
//            else
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG , e.getMessage());
//            }
//        }
//        catch (Exception e) 
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG , e.getMessage());
//        }
//
//        // add the selects for all levels to fetch
//        for(TargetBase target : targets)
//        {
//            // if we're going to be enumerating the values for this target,
//            // then we don't need to generate sql for it
//            if(target.getSrcMembers() == null)
//            {
//                addLeveMemberHbase(target.getLevel(), cube, aggStar, cols, request,hierarchyCols,levelNames);
//            }
//
//        }
//
//        if(request != null && constraint instanceof SetConstraint)
//        {
//            ((SetConstraint)constraint).addConstraintHbase(cube, aggStar, constraints, request);
//        }
//        else if(constraint instanceof SqlContextConstraint)
//        {
//           request = ((SqlContextConstraint)constraint).getCellRequest(null, aggStar);
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
//                putValuesInConsMap(constraints, columns, values, arity);
//            }
//        }
//        if(constraint instanceof FilterConstraint)
//        {
//            FilterConstraint filterConstraint = (FilterConstraint)constraint;
//            Exp filterExp = filterConstraint.getFilterExp();
//            if(!addMsrNonEmptyFilters(msrFilterConstraints, molapCube, filterExp))
//            {
//                msrFilterConstraints.clear();
//            }
//        }
//        
//        return request;
//    }
//
//    /**
//     * Add the non empty filters to the measures.
//     * @param msrFilterConstraints
//     * @param molapCube
//     * @param filterExp
//     */
//    private boolean addMsrNonEmptyFilters(Map<Measure, MeasureFilterModel[]> msrFilterConstraints, Cube molapCube,
//            Exp filterExp)
//    {
//        if(filterExp instanceof ResolvedFunCall)
//        {
//            ResolvedFunCall funCall = (ResolvedFunCall)filterExp;
//            if(!checkForFunOR(msrFilterConstraints, molapCube, funCall))
//            {
//                return false;
//            }
//            if(!checkForFunNOT(msrFilterConstraints, molapCube, funCall))
//            {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    /**
//     * @param msrFilterConstraints
//     * @param molapCube
//     * @param funCall
//     */
//    public boolean checkForFunNOT(Map<Measure, MeasureFilterModel[]> msrFilterConstraints, Cube molapCube,
//            ResolvedFunCall funCall)
//    {
//      //CHECKSTYLE:OFF    Approval No:Approval-368
//        if(funCall.getFunName().equals(NOT))
//        {
//            Exp[] args = funCall.getArgs();
//            Exp exp = args[0];
//            if(exp instanceof ResolvedFunCall)
//            {
//                ResolvedFunCall notEmptFun = (ResolvedFunCall)exp;
//                if(notEmptFun.getFunName().equals(IS_EMPTY))
//                {
//                    Exp arg = notEmptFun.getArg(0);
//                    if(arg instanceof MemberExpr)
//                    {
//                        MemberExpr meExpr = (MemberExpr)arg;
//                        if((meExpr.getMember() instanceof RolapBaseCubeMeasure))
//                        {                           
//                            RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)meExpr.getMember();
//                            Measure measure2 = molapCube.getMeasure(molapCube.getFactTableName(), measure.getName());
//                            MeasureFilterModel filterModel = new MeasureFilterModel(0, MeasureFilterType.NOT_EMPTY);
//                            msrFilterConstraints.put(measure2, new MeasureFilterModel[]{filterModel});
//                        }
//                        else if((meExpr.getMember() instanceof RolapCalculatedMeasure))
//                        {
//                            RolapCalculatedMeasure measure = (RolapCalculatedMeasure)meExpr.getMember();
//                            if(measure.getName().equals(ZERO))
//                            {
//                                return false;
//                            }
//                        }
//                    }
//                }
//            }
//        }
//      //CHECKSTYLE:ON
//        return true;
//    }
//
//    /**
//     * @param msrFilterConstraints
//     * @param molapCube
//     * @param funCall
//     */
//    public boolean checkForFunOR(Map<Measure, MeasureFilterModel[]> msrFilterConstraints, Cube molapCube,
//            ResolvedFunCall funCall)
//    {
//        if(funCall.getFunName().equals(OR))
//        {
//            Exp[] argsOr = funCall.getArgs();
//            for(int i = 0;i < argsOr.length;i++)
//            {
//                if(!addMsrNonEmptyFilters(msrFilterConstraints, molapCube, argsOr[i]))
//                {
//                    return false;
//                }
//            }
//        }
//        return true;
//    }
//    
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
//            tempContraint.addIncludedMembers('['+String.valueOf(values[i])+']');
//
//            // addConstr(cons, levels,
//            // ((MondrianDef.Column)column.getExpression()).name, list);
//            cons.put(((MondrianDef.Column)column.getExpression()).table+'_'+((MondrianDef.Column)column.getExpression()).name, tempContraint);
//        }
//    }
//
//    /**
//     * 
//     * @param partialResult
//     * @param newPartialResult
//     * @param message
//     * @param execQuery
//     * @param statement
//     * @param enumTargetCount
//     * @param srcMemberIdxes
//     * @param currPartialResultIdx
//     * @throws SQLException
//     * 
//     */
//    private void addEnumTargets(TupleList partialResult, List<List<RolapMember>> newPartialResult, String message,
//            boolean execQuery, MolapStatement statement, int enumTargetCount, int[] srcMemberIdxes,
//            int currPartialResultIdx) throws SQLException
//    {
//        if(enumTargetCount == 0)
//        {
//            int column = 0;
//            for(TargetBase target : targets)
//            {
//                target.setCurrMember(null);
//                column = target.addRow(statement, column);
//            }
//        }
//        else
//        {
//            // find the first enum target, then call addTargets()
//            // to form the cross product of the row from resultSet
//            // with each of the list of members corresponding to
//            // the enumerated targets
//            int firstEnumTarget = 0;
//            for(;firstEnumTarget < targets.size();firstEnumTarget++)
//            {
//                if(targets.get(firstEnumTarget).srcMembers != null)
//                {
//                    break;
//                }
//            }
//            List<RolapMember> partialRow;
//            if(execQuery)
//            {
//                partialRow = null;
//            }
//            else
//            {
//                partialRow = Util.cast(partialResult.get(currPartialResultIdx));
//            }
//            resetCurrMembers(partialRow);
//            addTargets(0, firstEnumTarget, enumTargetCount, srcMemberIdxes, statement, message);
//            if(newPartialResult != null)
//            {
//                savePartialResult(newPartialResult);
//            }
//        }
//    }
//
//    /**
//     * 
//     * @param evaluator
//     * @param aggStar
//     * @param queryCube
//     * @param cube
//     * @param constraints
//     * @param cols
//     * @param msrs
//     * @param msList
//     * @param hier
//     * @param statement
//     * 
//     */
//    private void executeStatement(Evaluator evaluator, AggStar aggStar, Cube queryCube, RolapCube cube,
//            Map<String, MolapFilterInfo> constraints, List<String> cols, List<MolapMetadata.Measure> msrs,
//            List<RolapStar.Measure> msList, RolapHierarchy hier, MolapStatement statement,Map<Measure, MeasureFilterModel[]> msrFilterConstraints)
//    {
//       
//        if(evaluator == null && hier != null)
//        {
//            // If all from same hierarchy and evaluator is not null. i.e
//            // some form of valid constraint exists
//            String hName = hier.getSubName() == null ? hier.getName() : hier.getSubName();
//            String cubeUniqueName = cube.getSchema().getName()+'_'+cube.getName();
//            statement.execute(constraints, hName, hier.getLevels(), aggStar, true, cubeUniqueName,msrFilterConstraints,readingMembers,getLevel(hier, cols));
//        }
//        else
//        {
//            List<Measure> calcMeasures = new ArrayList<MolapMetadata.Measure>();
//            // if constraint on diff hierarchy
//            Member[] queryMembers = evaluator != null ? getMeasureMembersArray(evaluator.getQuery()
//                    .getMeasuresMembers()) : null;
//
//            // If measures are available in query, consider them
//            if(queryMembers != null)
//            {
//                for(int i = 0;i < queryMembers.length;i++)
//                {
//                    if(queryMembers[i].isMeasure())
//                    {
//                        if((queryMembers[i] instanceof RolapBaseCubeMeasure))
//                        {                           
//                            RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)queryMembers[i];
//                            if(measure.getMondrianDefExpression() == null)
//                            {
//                                continue;
//                            }
//                            boolean found = false;
//                            for(int j = 0;j < msList.size();j++)
//                            {
//                                if(msrs.get(j).getName().equals(measure.getName()))
//                                {
//                                    found = true;
//                                    break;
//                                }
//                            }
//                            if(!found)
//                            {
//                                msList.add((RolapStar.Measure)measure.getStarMeasure());
//                                msrs.add(queryCube.getMeasure(queryCube.getFactTableName(), measure.getName()));
//                            }
//                        }
//                        else if(queryMembers[i] instanceof RolapHierarchy.RolapCalculatedMeasure)
//                        {
//                            RolapHierarchy.RolapCalculatedMeasure calculatedMeasure = (RolapHierarchy.RolapCalculatedMeasure)queryMembers[i];
//                            if(calculatedMeasure.getExpression() instanceof ResolvedFunCall)
//                            {
//                                getMeasuresFromCalcMeasures(msrs, msList, calculatedMeasure.getExpression(), queryCube);
////                                CalculatedMeasure calMsr = new CalculatedMeasure(calculatedMeasure.getExpression(), calculatedMeasure.getName());
//                                //calcMeasures.add(calMsr);
//                            }
//                        }
//                    }
//                }
//            }
//            // If not, get first measure from cube and use it (May be we an
//            // change to default measure if any configuration is available)
//            if(msrs.size() == 0)
//            {
//                List<RolapMember> members = cube.getMeasuresMembers();
//                // msrs=queryCube.getMeasures(queryCube.getFactTableName());
//
//                for(int i = 0;members != null && i < members.size();i++)
//                {
//                    if(members.get(i).isMeasure())
//                    {
//                        if(!(members.get(i) instanceof RolapBaseCubeMeasure))
//                        {
//                            continue;
//                        }
//                        RolapBaseCubeMeasure measure = (RolapBaseCubeMeasure)members.get(i);
//                        if(measure.getMondrianDefExpression() == null)
//                        {
//                            continue;
//                        }
//                        msList.add((RolapStar.Measure)measure.getStarMeasure());
//                        msrs.add(queryCube.getMeasure(queryCube.getFactTableName(), measure.getName()));
//
//                        // Added break just to consider one measure.
//                        break;
//                    }
//                }
//
//            }
//            
//            if(!constraints.isEmpty())
//            {
//                statement.execute(queryCube, cols, constraints, msrs, aggStar, calcMeasures, true,
//                        msrFilterConstraints, readingMembers, true);
//            }
//            else
//            {
//                statement.execute(queryCube, cols, constraints, msrs, aggStar, calcMeasures, true,
//                        msrFilterConstraints, readingMembers, false);
//            }
//        }
//    }
//
//    private void makeSegmentCache(RolapCube cube, CellRequest request, MolapStatement statement,
//            List<RolapStar.Measure> msList,Map<String, MolapFilterInfo> constraints)
//    {
//
//        statement.getIterator().reset();
//
//        // Identify new indices map from Tuple query to Segment query
//        List<Dimension> dimensionsList = statement.getDimensionsList();
//        List<Dimension> sortedDimensions = statement.getSortedDimensionsList();
//        int dimCount = dimensionsList.size();
//        int msrCount = msList.size();
//        int[] queryDimIndexMap = new int[msrCount + dimCount];
//
//        int attributeCount = 0;
//        int dimensionCount = dimensionsList.size();
//
//        for(int i = 0;i < dimensionCount;i++)
//        {
//            queryDimIndexMap[i] = attributeCount + i;
//            attributeCount += dimensionsList.get(i).getTotalAttributeCount();
//        }
//
//        for(int i = 0;i < msrCount;i++)
//        {
//            queryDimIndexMap[dimensionCount + i] = dimCount + attributeCount + i;
//        }
//
//        // For segment only dimension and measures are required. Properties are
//        // not required.
//        int segmentColumnCount = dimCount + msrCount;
//
//        // Segment expects the dimensions should be in order as per schema
//        // definition.
//        int[] schemaIndexMap = new int[segmentColumnCount];
//
//        int index = 0;
//        // Process dimension indices
//        for(Dimension dim : sortedDimensions)
//        {//CHECKSTYLE:OFF    Approval No:Approval-360
//            schemaIndexMap[index++] = queryDimIndexMap[dimensionsList.indexOf(dim)];
//            // schemaIndexMap[index++] = dimensionsList.indexOf(dim);//
//            // queryDimIndexMap//queryDimIndexMap[sortedDimensions.indexOf(dim)];
//        }
////CHECKSTYLE:ON
//        // Process measure indices
////        for(int i = 0;i < msrCount;i++)
////        {
////            schemaIndexMap[dimensionCount + i] = queryDimIndexMap[dimensionCount + i];
////        }
//        System.arraycopy(queryDimIndexMap, dimensionCount, schemaIndexMap, dimensionCount, msrCount);
//
//        // Set the calculated indices in HIterator for segment query
//        // If there is no change in indices, no need to consider.
//        if(!Arrays.equals(queryDimIndexMap, schemaIndexMap) || attributeCount > 0)
//        {
//            SegmentResultHolder iterator = new SegmentResultHolder(statement.getIterator(), schemaIndexMap);
//            statement.updateIterator(iterator);
//        }
//          //Removed null check for findbug issue
//        if(msList.size() > 0)
//        {
//            RolapStar.Measure[] measures=new RolapStar.Measure[0];
//            Column[] constrainedColumns = request.getConstrainedColumns();
//            StarColumnPredicate[] predicates = getPredicates(request,sortedDimensions,constraints);
//            if(constrainedColumns.length != predicates.length)
//            {
//                return;
//            }
//            AggregationKey aggregationKey = new AggregationKey(request);
//            Aggregation agg = cube.getStar().lookupOrCreateAggregation(aggregationKey);
//            List<GroupingSet> gsList = agg.getGroupList(constrainedColumns,
//                    msList.toArray(measures), predicates, aggMgr.createPinSet());
//            List<StarPredicate> compoundPredList = agg.getCompoundPredList();
//
//            // Load the data from statement to segment
//            new MolapSegmentLoader().load(gsList, aggMgr.createPinSet(), compoundPredList, statement);
//        }
//    }
//
//    private StarColumnPredicate[] getPredicates(CellRequest request,List<Dimension> sortedDimensions,Map<String, MolapFilterInfo> constraints)
//    {
//        int colCount = sortedDimensions.size();
//        StarColumnPredicate[] predicates = new StarColumnPredicate[colCount];
//
//        for(int i = 0;i < predicates.length;i++)
//        {
//            // Get includedMembers of constraints and set predicates accordingly 
//            Dimension dimension = sortedDimensions.get(i);
//            String name = dimension.getTableName()+'_'+dimension.getColName();
//            MolapFilterInfo  filterInfo = constraints.get(name);
//            List<String> list = filterInfo == null ? null :filterInfo.getIncludedMembers();
//            getStarPredicate(list,predicates,i);
//        }
//
//        return predicates;
//    }
//    
//
//    /**
//     * Method returns columnPredicate based on includedMembers list 
//     * @param list list of included members
//     * @return
//     */
//    private static void getStarPredicate( List<String> list,StarColumnPredicate[] predicates,int index)
//    {
//        List<String> memList = new ArrayList<String>();
//        if(list == null || list.size() == 0)
//        {
//            predicates[index] = LiteralStarPredicate.TRUE;
//        }
//        else if(list.size() == 1)
//        {
//            String[] parseMembers = QueryExecutorUtil.parseMembers(list.get(0), memList);
//            for(int i = parseMembers.length-1;i >= 0;i--,index--)
//            {
//                predicates[index] = new ValueColumnPredicate(null, parseMembers[i]);
//            }
//            
//        }
//        else
//        {
//            for(String pred : list)
//            {
//                int locIndex = index;
//                String[] parseMembers = QueryExecutorUtil.parseMembers(pred, memList);
//                for(int i = parseMembers.length-1;i >= 0;i--,locIndex--)
//                {
//                    predicates[locIndex] = new ValueColumnPredicate(null, parseMembers[i]);
//                    if(predicates[locIndex] == null)
//                    {
//                        predicates[locIndex] =  new ListColumnPredicate(null, new ArrayList<StarColumnPredicate>());
//                    }
//                    else if(predicates[locIndex] instanceof ValueColumnPredicate)
//                    {
//                        ValueColumnPredicate valueColumnPredicate = (ValueColumnPredicate)predicates[locIndex];
//                        ListColumnPredicate columnPredicate =  new ListColumnPredicate(null, new ArrayList<StarColumnPredicate>());
//                        columnPredicate.getPredicates().add(valueColumnPredicate);
//                        columnPredicate.getPredicates().add(new ValueColumnPredicate(null, parseMembers[i]));
//                        predicates[locIndex] = columnPredicate;
//                    }
//                    else
//                    {
//                        ListColumnPredicate columnPredicate = (ListColumnPredicate)predicates[locIndex];
//                        columnPredicate.getPredicates().add(new ValueColumnPredicate(null, parseMembers[i]));
//                    }
//                }
//            }
//        }
//    }
//    
//
//
//    private Member[] getMeasureMembersArray(Set<Member> memberSet)
//    {
//        if(memberSet == null || memberSet.size() == 0)
//        {
//            return null;
//        }
//        Member[] members = new Member[memberSet.size()];
//        int i = 0;
//        for(Member member : memberSet)
//        {
//            members[i++] = member;
//        }
//        return members;
//    }
//
//    /**
//     * Add level to request and the name of level's column to the list
//     * <code>cols</code>
//     * 
//     * @param level
//     * @param baseCube
//     * @param aggStar
//     * @param cols
//     * @param request
//     * @param levelNames 
//     * 
//     */
//    private void addLeveMemberHbase(RolapLevel level, RolapCube baseCube, AggStar aggStar, List<String> cols,
//            CellRequest request,List<String> hierarchyNames, List<String> levelNames)
//    {
//
//        RolapHierarchy hierarchy = level.getHierarchy();
//
//        // lookup RolapHierarchy of base cube that matches this hierarchy
//        if(hierarchy instanceof RolapCubeHierarchy)
//        {
//            RolapCubeHierarchy cubeHierarchy = (RolapCubeHierarchy)hierarchy;
//            if(baseCube != null && !cubeHierarchy.getCube().equals(baseCube))
//            {
//                // replace the hierarchy with the underlying base cube hierarchy
//                // in the case of virtual cubes
//                hierarchy = baseCube.findBaseCubeHierarchy(hierarchy);
//            }
//        }
//        //Coverity Fix added null check
//        if( null != hierarchy)
//        {
//            RolapLevel[] levels = (RolapLevel[])hierarchy.getLevels();
//            int levelDepth = level.getDepth();
//    
//            for(int i = 0;i <= levelDepth;i++)
//            {
//                RolapLevel currLevel = levels[i];
//                if(currLevel.isAll())
//                {
//                    continue;
//                }
//
//            // Determine if the aggregate table contains the collapsed level
//            // boolean levelCollapsed = (aggStar != null)
//            // && SqlMemberSource.isLevelCollapsed(aggStar,
//            // (RolapCubeLevel)currLevel);
//
//            // boolean multipleCols =
//            // SqlMemberSource.levelContainsMultipleColumns(currLevel);
//
//            // if (levelCollapsed && !multipleCols) {
//            // // if this is a single column collapsed level, there is
//            // // no need to join it with dimension tables
//            // RolapStar.Column starColumn =
//            // ((RolapCubeLevel) currLevel).getStarKeyColumn();
//            // int bitPos = starColumn.getBitPosition();
//            // AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
//            // String q = aggColumn.generateExprString(sqlQuery);
//            // sqlQuery.addSelectGroupBy(q, starColumn.getInternalType());
//            // sqlQuery.addOrderBy(q, true, false, true);
//            // aggColumn.getTable().addToFrom(sqlQuery, false, true);
//            // continue;
//            // }
//
//            MondrianDef.Expression keyExp = currLevel.getKeyExp();
//            if(null!=levelNames)
//            {
//            levelNames.add(currLevel.getName());
//            }
//            // MondrianDef.Expression ordinalExp = currLevel.getOrdinalExp();
//            // MondrianDef.Expression captionExp = currLevel.getCaptionExp();
//            // MondrianDef.Expression parentExp = currLevel.getParentExp();
//            String hierarchyName = currLevel.getHierarchy().getName();
//            if(null!=hierarchyNames)
//            {
//                hierarchyNames.add(hierarchyName);
//            }
//            if(hierarchyName.contains("."))
//            {
//                hierarchyName = hierarchyName.substring(hierarchyName.indexOf(".") + 1, hierarchyName.length());
//            } 
//            if(!cols.contains(((MondrianDef.Column)keyExp).table + '_' + ((MondrianDef.Column)keyExp).name + '#'
//                    + currLevel.getDimension().getName() + ',' + hierarchyName))
//            {
//                cols.add(((MondrianDef.Column)keyExp).table + '_' + ((MondrianDef.Column)keyExp).name + '#'
//                        + currLevel.getDimension().getName() + ',' + hierarchyName); 
//            }
//
//            if(request != null)
//            {
//                request.addConstrainedColumn(((RolapCubeLevel)currLevel).getBaseStarKeyColumn(baseCube), null);
//            }
//
//            // if (parentExp != null) {
//            // if (!levelCollapsed) {
//            // hierarchy.addToFrom(sqlQuery, parentExp);
//            // }
//            // String parentSql = parentExp.getExpression(sqlQuery);
//            // sqlQuery.addSelectGroupBy(
//            // parentSql, currLevel.getInternalType());
//            // if (whichSelect == WhichSelect.LAST
//            // || whichSelect == WhichSelect.ONLY)
//            // {
//            // sqlQuery.addOrderBy(parentSql, true, false, true, false);
//            // }
//            // }
//            //
//            // String keySql = keyExp.getExpression(sqlQuery);
//            // String ordinalSql = ordinalExp.getExpression(sqlQuery);
//            //
//            // if (!levelCollapsed) {
//            // hierarchy.addToFrom(sqlQuery, keyExp);
//            // hierarchy.addToFrom(sqlQuery, ordinalExp);
//            // }
//            // String captionSql = null;
//            // if (captionExp != null) {
//            // captionSql = captionExp.getExpression(sqlQuery);
//            // if (!levelCollapsed) {
//            // hierarchy.addToFrom(sqlQuery, captionExp);
//            // }
//            // }
//            //
//            // String alias =
//            // sqlQuery.addSelect(keySql, currLevel.getInternalType());
//            // if (needsGroupBy) {
//            // sqlQuery.addGroupBy(keySql, alias);
//            // }
//            //
//            // if (captionSql != null) {
//            // alias = sqlQuery.addSelect(captionSql, null);
//            // if (needsGroupBy) {
//            // sqlQuery.addGroupBy(captionSql, alias);
//            // }
//            // }
//            //
//            // if (!ordinalSql.equals(keySql)) {
//            // alias = sqlQuery.addSelect(ordinalSql, null);
//            // if (needsGroupBy) {
//            // sqlQuery.addGroupBy(ordinalSql, alias);
//            // }
//            // }
//            //
//            // constraint.addLevelConstraint(
//            // sqlQuery, baseCube, aggStar, currLevel);
//            //
//            // if (levelCollapsed) {
//            // // add join between key and aggstar
//            // // join to dimension tables starting
//            // // at the lowest granularity and working
//            // // towards the fact table
//            // hierarchy.addToFromInverse(sqlQuery, keyExp);
//            //
//            // RolapStar.Column starColumn =
//            // ((RolapCubeLevel) currLevel).getStarKeyColumn();
//            // int bitPos = starColumn.getBitPosition();
//            // AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
//            // RolapStar.Condition condition =
//            // new RolapStar.Condition(keyExp, aggColumn.getExpression());
//            // sqlQuery.addWhere(condition.toString(sqlQuery));
//            // }
//            //
//            // // If this is a select on a virtual cube, the query will be
//            // // a union, so the order by columns need to be numbers,
//            // // not column name strings or expressions.
//            // switch (whichSelect) {
//            // case LAST:
//            // boolean nullable = true;
//            // final Dialect dialect = sqlQuery.getDialect();
//            // if (dialect.requiresUnionOrderByExprToBeInSelectClause()
//            // || dialect.requiresUnionOrderByOrdinal())
//            // {
//            // // If the expression is nullable and the dialect
//            // // sorts NULL values first, the dialect will try to
//            // // add an expression 'Iif(expr IS NULL, 1, 0)' into
//            // // the ORDER BY clause, and that is not allowed by this
//            // // dialect. So, pretend that the expression is not
//            // // nullable. NULL values, if present, will be sorted
//            // // wrong, but that's better than generating an invalid
//            // // query.
//            // nullable = false;
//            // }
//            // sqlQuery.addOrderBy(
//            // Integer.toString(
//            // sqlQuery.getCurrentSelectListSize()),
//            // true, false, nullable);
//            //
//            // break;
//            // case ONLY:
//            // sqlQuery.addOrderBy(ordinalSql, true, false, true);
//            // break;
//            // }
//            //
//            // RolapProperty[] properties = currLevel.getProperties();
//            // for (RolapProperty property : properties) {
//            // final MondrianDef.Expression propExp = property.getExp();
//            // final String propSql;
//            // if (propExp instanceof MondrianDef.Column) {
//            // // When dealing with a column, we must use the same table
//            // // alias as the one used by the level. We also assume that
//            // // the property lives in the same table as the level.
//            // propSql =
//            // sqlQuery.getDialect().quoteIdentifier(
//            // currLevel.getTableAlias(),
//            // ((MondrianDef.Column)propExp).name);
//            // } else {
//            // propSql = property.getExp().getExpression(sqlQuery);
//            // }
//            // alias = sqlQuery.addSelect(propSql, null);
//            // if (needsGroupBy) {
//            // // Certain dialects allow us to eliminate properties
//            // // from the group by that are functionally dependent
//            // // on the level value
//            // if (!sqlQuery.getDialect().allowsSelectNotInGroupBy()
//            // || !property.dependsOnLevelValue())
//            // {
//            // sqlQuery.addGroupBy(propSql, alias);
//            // }
//            // }
//            // }
//            }
//        }
//    }
//
//    private RolapHierarchy isAllColPresentInSameHierarichy(List<RolapHierarchy> hierarchies, List<String> cols)
//    {
//        for(Iterator<RolapHierarchy> iterator = hierarchies.iterator();iterator.hasNext();)
//        {
//            RolapHierarchy rolapHierarchy = iterator.next();
//
//            RolapLevel[] levels = (RolapLevel[])rolapHierarchy.getLevels();
//            //
//            int colPresent = 0;
//            for(Iterator<String> iterator2 = cols.iterator();iterator2.hasNext();)
//            {
//                String col = iterator2.next();
//                if(isColPrsntinLevels(levels, col))
//                {
//                    colPresent++;
//                }
//                //
//            }
//            if(colPresent == cols.size())
//            {
//                return rolapHierarchy;
//            }
//        }
//
//        return null;
//    }
//
//    private boolean isColPrsntinLevels(RolapLevel[] levels, String col)
//    {
//        boolean colPrsnt = false;
//        for(int i = 0;i < levels.length;i++)
//        {
//            if(!levels[i].isAll() && !levels[i].isMeasure())
//            {
//                String hierarchyName = levels[i].getHierarchy().getName();
//                if(hierarchyName.contains("."))
//                {
//                    hierarchyName = hierarchyName.substring(hierarchyName.indexOf(".") + 1, hierarchyName.length());
//                }
//                if((((MondrianDef.Column)levels[i].getKeyExp()).table + '_'
//                        + ((MondrianDef.Column)levels[i].getKeyExp()).name + '#' + levels[i].getDimension().getName()
//                        + ',' + hierarchyName).equals(col))
//                {
//                    colPrsnt = true;
//                    break;
//                }
//            }
//        }
//        return colPrsnt;
//    }
//
//
//    private RolapLevel[] getLevel(RolapHierarchy hier, List<String> colName)
//    {
//        Level[] levels = hier.getLevels();
//
//        RolapLevel[] levels2 = new RolapLevel[colName.size()];
//        //
//        for(int j = 0;j < levels.length;j++)
//        {
//            RolapLevel level2 = (RolapLevel)levels[j];
//
//            if(!level2.isAll())
//            {
//                //
//                int i = 0;
//                for(Iterator<String> iterator = colName.iterator();iterator.hasNext();)
//                {
//                    String string = (String)iterator.next();
//                    //
//                    if(((((MondrianDef.Column)level2.getKeyExp()).table+'_'+((MondrianDef.Column)level2.getKeyExp()).name)).equals(string.split("#")[0]))
//                    {
//                        levels2[i] = level2;
//                    }
//                    i++;
//                }
//            }
//
//        }
//        return levels2;
//    }
//    
//    private void getMeasuresFromCalcMeasures(List<MolapMetadata.Measure> msrs,List<RolapStar.Measure> msList,Exp exp,Cube queryCube)
//    {
//        List<MolapMetadata.Measure> msrsLocal = new ArrayList<MolapMetadata.Measure>();
//        List<RolapStar.Measure> msListLocal = new ArrayList<RolapStar.Measure>();
//        
//        parseExpression(msrsLocal, msListLocal, exp, queryCube);
//        
//        List<MolapMetadata.Measure> msrsLocalUnq = new ArrayList<MolapMetadata.Measure>();
//        List<RolapStar.Measure> msListLocalUnq = new ArrayList<RolapStar.Measure>();
//        
//        for(int i = 0;i < msListLocal.size();i++)
//        {
//            MolapMetadata.Measure measure = msrsLocal.get(i);
//            boolean found = false;
//            for(int j = 0;j < msList.size();j++)
//            {
//                if(msrs.get(j).getName().equals(measure.getName()))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            if(!found)
//            {
//                msrsLocalUnq.add(measure);
//                msListLocalUnq.add(msListLocal.get(i));
//            }
//        }
//        
//        msrs.addAll(msrsLocalUnq);
//        msList.addAll(msListLocalUnq);
//        
//    }
//
//    private void parseExpression(List<MolapMetadata.Measure> msrs,List<RolapStar.Measure> msList,Exp exp,Cube queryCube)
//    {
//        if(exp instanceof ResolvedFunCall)
//        {
//            ResolvedFunCall funCall = (ResolvedFunCall)exp;
//            Exp[] args = funCall.getArgs();
//            for(int i = 0;i < args.length;i++)
//            {
//                parseExpression(msrs, msList, args[i],queryCube);
//            }
//        }
//        else if(exp instanceof MemberExpr)
//        {
//            MemberExpr expr = (MemberExpr)exp;
//            if(expr.getMember() instanceof RolapBaseCubeMeasure)
//            {
//                RolapBaseCubeMeasure cubeMeasure = (RolapBaseCubeMeasure)expr.getMember();
//                
//                boolean found = false;
//                for(int j = 0;j < msList.size();j++)
//                {
//                    if(msrs.get(j).getName().equals(cubeMeasure.getName()))
//                    {
//                        found = true;
//                        break;
//                    }
//                }
//                if(!found)
//                {
//                    msList.add((RolapStar.Measure)cubeMeasure.getStarMeasure());
//                    msrs.add(queryCube.getMeasure(queryCube.getFactTableName(), cubeMeasure.getName()));
//                }
//            }
//        }
//    }

}
