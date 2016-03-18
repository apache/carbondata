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

package com.huawei.unibi.molap.engine.executer.impl;

import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.vo.HybridStoreModel;

public class QueryExecuterProperties
{
    /**
     * schemaName
     */
    protected String schemaName;

    /**
     * cubeName
     */
    protected String cubeName;

    /**
     * cube unique name
     */
    protected String cubeUniqueName;

    /**
     * dimension table array
     */
    protected Dimension[] dimTables;

    /**
     * dimension table array
     */
    protected Map<String,GenericQueryType> complexDimensionsMap;
    
    /**
     * list of active slices present for execution
     */
    protected List<InMemoryCube> slices;

    /**
     * global key generator basically it is the last slice updated keygenerator
     */
    protected KeyGenerator globalKeyGenerator;
    
    /**
     * uniqueValue
     */
    protected double[] uniqueValue;
    
    /**
     * mask bytes ranges
     */
    protected int[] maskByteRanges;
    
    /**
     * masked bytes
     */
    protected int[] maskedBytes; 
    
    /**
     * max key for query execution
     */
    protected byte[] maxKey;
    
    /**
     * byteCount
     */
    protected int byteCount;
    
    /**
     * isCountMsrExistInCurrTable
     */
    protected boolean isCountMsrExistInCurrTable;
    
    /**
     * count msr index in current table
     */
    protected int countMsrIndex = -1;
    
    /**
     * average msr indexes
     */
    protected List<Integer> avgIndexes;
    
    /**
     * sort order of dimension
     */
    protected byte[] dimSortOrder;
    
    /**
     * measureStartIndex
     */
    protected int measureStartIndex;
    
    /**
     * aggTypes
     */
    protected String[] aggTypes;
    
    /**
     * msrMinValue
     */
    protected double[] msrMinValue;

    /**
     * isFunctionQuery
     */
    protected boolean isFunctionQuery;
    

    /**
     * aggExpDimension
     */
    protected List<Dimension> aggExpDimensions;
    
    /**
     * aggExpMeasure
     */
    protected List<Measure> aggExpMeasures;
    
    /**
     * aggExpressionStartIndex
     */
    protected int aggExpressionStartIndex;
    
    /**
     * sortDimIndexex
     */
    protected byte[] sortDimIndexes;

    protected boolean[] isHighCardinality;
    
    
    /**
     * Hybrid store model, it will have detail about columnar and row stores
     */
    protected HybridStoreModel hybridStoreModel;

    
}
