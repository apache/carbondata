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

package com.huawei.unibi.molap.engine.holders;

import java.util.List;

/**
 * This class can be used to re index the existing data array to read in
 * different column order. Ex:- For tuple query, data order is
 * dimensions order in query. For Segment query the order as per
 * dimensions in cube.
 * If we re-index the same data with required order, we can reuse the
 * same data array for both purposes.
 * 
 * Version 1.0
 */
public class SegmentResultHolder extends MolapResultHolder
{

    public SegmentResultHolder(List<com.huawei.unibi.molap.olap.SqlStatement.Type> dataTypes)
    {
        super(dataTypes);
        // TODO Auto-generated constructor stub
    }
//    /**
//     * Actual Iterator on data
//     */
//    private MolapResultHolder actual;
//
//    /**
//     * Index mapping for each dimension.
//     */
//    private int[] dimIndexMap;
//
//    /**
//     * Data type
//     */
//    private List<Type> dataTypes;
//
//    /**
//     * 
//     * 
//     * @param actual
//     * @param dimIndexMap
//     *
//     */
//    public SegmentResultHolder(MolapResultHolder actual, int[] dimIndexMap)
//    {
//        super(null);
//
//        this.actual = actual;
//        this.dimIndexMap = dimIndexMap;
//
//        dataTypes = new ArrayList<SqlStatement.Type>(100);
//
//        for(Integer dimension : this.dimIndexMap)
//        {
//            dataTypes.add(actual.getDataType(dimension));
//        }
//    }
//
//    
//    /**
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#getDataType(int)
//     */
//    public Type getDataType(int column)
//    {
//        return dataTypes.get(column);
//    }
//
//    /**
//     * Return the Sql data type,  Wrapped methods to ignore properties.
//     * 
//     * @return Sql data type
//     * 
//     */
//    public List<Type> getDataTypes()
//    {
//        return dataTypes;
//    }
//
//    /**
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#getObject(int)
//     */
//    public Object getObject(int column)
//    {
//        return actual.getObject(dimIndexMap[column - 1] + 1);
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#isNext()
//     */
//    public boolean isNext()
//    {
//        return actual.isNext();
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#reset()
//     */
//    public void reset()
//    {
//        actual.reset();
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#setObject(java.lang
//     *      .Object[][])
//     */
//    public void setObject(Object[][] data)
//    {
//        actual.setObject(data);
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#getColumnCount()
//     */
//    public int getColumnCount()
//    {
//        return actual.getColumnCount();
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#getInt(int)
//     */
//    public int getInt(int column)
//    {
//        return actual.getInt(dimIndexMap[column - 1] + 1);
//    }
//
//    /**
//     * 
//     * @see com.huawei.unibi.molap.engine.util.MolapResultHolder#getLong(int)
//     */
//    public long getLong(int column)
//    {
//        return actual.getLong(dimIndexMap[column - 1] + 1);
//    }
//
//    /**
//     * Return the double value for column
//     * 
//     * @param column
//     *          column index
//     * @return double value
//     *          
//     *
//     */
//    public double getDouble(int column)
//    {
//        return actual.getDouble(dimIndexMap[column - 1] + 1);
//    }
//
//    /**
//     * Check whether last value is null or not
//     * 
//     * @return last value was null or not
//     *
//     */
//    public boolean wasNull()
//    {
//        return actual.wasNull();
//    }
//
//    /**
//     * Set the sql data type, Wrapped methods to ignore properties.
//     * 
//     * @param types
//     *            sql data type
//     * 
//     */
//    public void setDataTypes(List<Type> types)
//    {
//        actual.setDataTypes(types);
//    }

}
