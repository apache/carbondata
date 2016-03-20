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

package com.huawei.unibi.molap.engine.directinterface.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.query.metadata.MolapMember;
import com.huawei.unibi.molap.query.metadata.MolapTuple;
import com.huawei.unibi.molap.query.result.impl.MolapResultChunkImpl;
import com.huawei.unibi.molap.query.result.impl.MolapResultStreamImpl;

/**
 * It converts traditional DB based iterrator to the OLAP iteration model.
 * 
 *
 */
public final class MolapResultConverter
{
  
    private MolapResultConverter()
    {
        
    }
    public static MolapResultStreamImpl convertInternal(MolapResultHolder rs, List<MolapMetadata.Dimension> queryDimsRows,
            List<MolapMetadata.Dimension> queryDimsCols, List<Measure> queryMsrs, boolean propertiesRequired, boolean useNamesFromHolder)
    {

        int[] rowsIndexes = new int[queryDimsRows.size()];
        int[] colsIndexes = new int[queryDimsCols.size()];
        int[] msrsIndexes = new int[queryMsrs.size()];
        
        int[][] rowsProps = new int[queryDimsRows.size()][];
        int[][] colsProps = new int[queryDimsCols.size()][];
        
        int msrLen = msrsIndexes.length;
        if(useNamesFromHolder && rs.getColHeaders()!=null && !rs.getColHeaders().isEmpty())
        {
            //Get the actual column index in the result based on the column headers given in result set
            List<String> namesOrder = rs.getColHeaders();
            calculateIndexesUsingNameOrder(queryDimsRows, propertiesRequired, rowsIndexes, rowsProps, namesOrder);
            calculateIndexesUsingNameOrder(queryDimsCols, propertiesRequired, colsIndexes, colsProps, namesOrder);
            
            for(int j = 0;j < msrLen;j++)
            {
                msrsIndexes[j] = namesOrder.indexOf(queryMsrs.get(j).getName())+1;
            }
        }
        else
        {
            int k =1;
            
            k = calculateIndexes(queryDimsRows, propertiesRequired, rowsIndexes, rowsProps, k);
            k = calculateIndexes(queryDimsCols, propertiesRequired, colsIndexes, colsProps, k);
            
            for(int j = 0;j < msrLen;j++)
            {
                msrsIndexes[j] = k;
                k++;
            }
        }
        

        MolapResultStreamImpl streamImpl = new MolapResultStreamImpl();
        MolapResultChunkImpl chunkImpl = new MolapResultChunkImpl();
        List<MolapTuple> molapColTuples = null;
        if(colsIndexes.length > 0)
        {
            Set<MolapTuple> colTuples = new LinkedHashSet<MolapTuple>();
            while(rs.isNext())
            {
                createColTuples(rs, propertiesRequired, colsIndexes, colsProps, colTuples);
            }
            
            molapColTuples = new ArrayList<MolapTuple>(colTuples);
            streamImpl.setTuples(getActualColTuples(molapColTuples, queryMsrs));
            rs.reset();
            
            Map<MolapTuple, Integer> colTupleMappings = new HashMap<MolapTuple, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            
            int l = 0;
            for(MolapTuple molapTuple : colTuples)
            {
                colTupleMappings.put(molapTuple, l);
                l++;
            }
            
//            List<MolapTuple> rowTuples = new ArrayList<MolapTuple>();
            Map<MolapTuple, Object[]> rowTupleMap = new LinkedHashMap<MolapTuple, Object[]>();
//            List<Object[]> cells = new ArrayList<Object[]>();
            int len = molapColTuples.size();
            while(rs.isNext())
            {
                MolapMember[] members = new MolapMember[rowsIndexes.length];
                MolapTuple rowTuple = new MolapTuple(members);
                for(int i = 0;i < rowsIndexes.length;i++)
                {
                    Object val = rs.getObject(rowsIndexes[i]);
                    members[i] = new MolapMember(rs.wasNull()?null:val, null);
                }
                
                MolapMember[] colMembers = new MolapMember[colsIndexes.length];
                MolapTuple colTuple = new MolapTuple(colMembers);
                for(int i = 0;i < colsIndexes.length;i++)
                {
                    Object val = rs.getObject(colsIndexes[i]);
                    colMembers[i] = new MolapMember(rs.wasNull()?null:val, null);
                }
                
                
                
                Object[] cell = rowTupleMap.get(rowTuple);
                if(cell == null)
                {
                    if(propertiesRequired)
                    {
                        for(int i = 0;i < rowsProps.length;i++)
                        {
                            Object[] props = new Object[rowsProps[i].length];
                            members[i].setProperties(props);
                            for(int j = 0;j < rowsProps[i].length;j++)
                            {
                                Object val = rs.getObject(rowsProps[i][j]);
                                props[j] = rs.wasNull()?null:val;
                            }
                        }
                    }

                    cell = new Object[len*msrLen];
                    rowTupleMap.put(rowTuple, cell);
                }

                int index = colTupleMappings.get(colTuple)*msrLen;
                for(int i = 0;i < msrLen;i++)
                {
                    Object val = rs.getObject(msrsIndexes[i]);
                    cell[index++] = rs.wasNull()?null:val;
                }
               
            }
            chunkImpl.setRowTuples(new ArrayList<MolapTuple>(rowTupleMap.keySet()));
            chunkImpl.setData(new ArrayList<Object[]>(rowTupleMap.values()).toArray(new Object[0][0]));
        }
        else
        {
            List<MolapTuple> rowTuples = new ArrayList<MolapTuple>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            List<Object[]> cells = new ArrayList<Object[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            
            while(rs.isNext())
            {
                MolapMember[] members = new MolapMember[rowsIndexes.length];
                MolapTuple rowTuple = new MolapTuple(members);
                for(int i = 0;i < rowsIndexes.length;i++)
                {
                    Object val = rs.getObject(rowsIndexes[i]);
                    members[i] = new MolapMember(rs.wasNull()?null:val, null);
                }
                
                
                if(propertiesRequired)
                {
                    for(int i = 0;i < rowsProps.length;i++)
                    {
                        Object[] props = new Object[rowsProps[i].length];
                        members[i].setProperties(props);
                        for(int j = 0;j < rowsProps[i].length;j++)
                        {
                            Object val = rs.getObject(rowsProps[i][j]);
                            props[j] = rs.wasNull()?null:val;
                        }
                    }
                }
                rowTuples.add(rowTuple);
                
                Object[] msrs = new Object[msrsIndexes.length];
                for(int i = 0;i < msrsIndexes.length;i++)
                {
                    Object val = rs.getObject(msrsIndexes[i]);
                    msrs[i] = rs.wasNull()?null:val;
                }
                cells.add(msrs);
            }
            
            chunkImpl.setRowTuples(rowTuples);
            
            streamImpl.setTuples(getActualColTuples(queryMsrs));
            
            chunkImpl.setData(cells.toArray(new Object[0][0]));
        }
        streamImpl.setMolapResultChunk(chunkImpl);
        return streamImpl;
    
    }
    
    /**
     * Convert molap result to the direct interface api result
     * @param rs
     * @param queryDimsRows
     * @param queryDimsCols
     * @param queryMsrs
     * @param propertiesRequired
     * @return MolapResultStreamImpl
     */
    public static MolapResultStreamImpl convert(MolapResultHolder rs, List<MolapMetadata.Dimension> queryDimsRows,
            List<MolapMetadata.Dimension> queryDimsCols, List<Measure> queryMsrs, boolean propertiesRequired)
    {
        return convertInternal(rs, queryDimsRows, queryDimsCols, queryMsrs, propertiesRequired, false);
    }
    
    /**
     * Convert molap result to the direct interface api result
     * @param rs
     * @param queryDimsRows
     * @param queryDimsCols
     * @param queryMsrs
     * @param propertiesRequired
     * @return MolapResultStreamImpl
     */
    public static MolapResultStreamImpl convertUsingNameMapping(MolapResultHolder rs, List<MolapMetadata.Dimension> queryDimsRows,
            List<MolapMetadata.Dimension> queryDimsCols, List<Measure> queryMsrs, boolean propertiesRequired)
    {
        return convertInternal(rs, queryDimsRows, queryDimsCols, queryMsrs, propertiesRequired, true);
    }




    /**
     * Create the column tuples.
     * @param rs
     * @param propertiesRequired
     * @param colsIndexes
     * @param colsProps
     * @param colTuples
     */
    private static void createColTuples(MolapResultHolder rs, boolean propertiesRequired, int[] colsIndexes,
            int[][] colsProps, Set<MolapTuple> colTuples)
    {
        MolapMember[] members = new MolapMember[colsIndexes.length];
        MolapTuple colTuple = new MolapTuple(members);
        for(int i = 0;i < colsIndexes.length;i++)
        {
            Object val = rs.getObject(colsIndexes[i]);
            members[i] = new MolapMember(rs.wasNull()?null:val, null);
        }
        if(colTuples.add(colTuple) && propertiesRequired)
        {
            for(int i = 0;i < colsProps.length;i++)
            {
                Object[] props = new Object[colsProps[i].length];
                members[i].setProperties(props);
                for(int j = 0;j < colsProps[i].length;j++)
                {
                    Object val = rs.getObject(colsProps[i][j]);
                    props[j] = rs.wasNull()?null:val;
                }
            }
        }
    }
    
    /**
     * Get the tuples with measure added.
     * @param molapTuples
     * @param queryMsrs
     * @return
     */
    private static List<MolapTuple> getActualColTuples(List<MolapTuple> molapTuples,List<Measure> queryMsrs)
    {
        List<MolapTuple> tuples = new ArrayList<MolapTuple>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        for(MolapTuple molapTuple : molapTuples)
        {
            MolapMember[] members = molapTuple.getTuple();
            for(Measure measure : queryMsrs)
            {
                MolapMember[] localMem = new MolapMember[members.length+1];
                System.arraycopy(members, 0, localMem, 0, members.length);
                localMem[members.length] = new MolapMember(measure.getName(), null);
                MolapTuple locTuple = new MolapTuple(localMem);
                tuples.add(locTuple);
            }
        }
        return tuples;
    }

    /**
     * create the tuples with measure added.
     * @param queryMsrs
     * @return
     */
    private static List<MolapTuple> getActualColTuples(List<Measure> queryMsrs)
    {
        List<MolapTuple> tuples = new ArrayList<MolapTuple>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for(Measure measure : queryMsrs)
        {
            MolapMember[] localMem = new MolapMember[1];
            localMem[0] = new MolapMember(measure.getName(), null);
            MolapTuple locTuple = new MolapTuple(localMem);
            tuples.add(locTuple);
        }
        return tuples;
    }
    
    /**
     * Claculate the indexes of rows , columns as per the MOlap iterator.
     * @param queryDimsRows
     * @param propertiesRequired
     * @param rowsIndexes
     * @param rowsProps
     * @param k
     * @return
     */
    private static int calculateIndexes(List<MolapMetadata.Dimension> queryDimsRows, boolean propertiesRequired,
            int[] rowsIndexes, int[][] rowsProps, int k)
    {
        for(int i = 0;i < queryDimsRows.size();i++)
        {
            Dimension dimension = queryDimsRows.get(i);
            if(propertiesRequired&&dimension.isHasNameColumn())
            {
                k++;
            }
            rowsIndexes[i] = k;
            k++;
            int[] props = new int[0];
            if(propertiesRequired)
            {
                if(dimension.getPropertyCount()>0)
                {
                    props = new int[dimension.isHasNameColumn()?(dimension.getPropertyCount()-1):dimension.getPropertyCount()];
                    for(int j = 0;j < props.length;j++)
                    {
                        props[j] = k;
                        k++;
                    }
                }
            }
            rowsProps[i] = props;
        }
        return k;
    }
    
    private static void calculateIndexesUsingNameOrder(List<MolapMetadata.Dimension> queryDimsRows, boolean propertiesRequired,
            int[] rowsIndexes, int[][] rowsProps, List<String> namesOrder)
    {
        for(int i = 0;i < queryDimsRows.size();i++)
        {
            Dimension dimension = queryDimsRows.get(i);
            
            rowsIndexes[i] = namesOrder.indexOf(dimension.getName());
            
            int propsIndex =  rowsIndexes[i]++;
            if(propertiesRequired&&dimension.isHasNameColumn())
            {
                propsIndex++;
            }
            
            int[] props = new int[0];
            if(propertiesRequired)
            {
                if(dimension.getPropertyCount()>0)
                {
                    props = new int[dimension.isHasNameColumn()?(dimension.getPropertyCount()-1):dimension.getPropertyCount()];
                    for(int j = 0;j < props.length;j++)
                    {
                        props[j] = propsIndex;
                        propsIndex++;
                    }
                }
            }
            rowsProps[i] = props;
        }
    }
    
}
