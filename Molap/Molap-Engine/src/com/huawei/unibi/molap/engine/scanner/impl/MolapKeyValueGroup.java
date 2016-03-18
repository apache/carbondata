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

/**
 * 
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapKeyValueGroup extends MolapValue
{

    /**
     * 
     */
    private static final long serialVersionUID = -3777098129647737853L;
    
    private List<MolapKey> keys = new ArrayList<MolapKey>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    
    private List<MolapValue> values = new ArrayList<MolapValue>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    
    public MolapKeyValueGroup(MeasureAggregator[] values)
    {
        super(values);
    }
    
    
    public void addGroup(MolapKey key,MolapValue value)
    {
        keys.add(key);
        values.add(value);
    }
    
    public MolapValue mergeKeyVal(MolapValue another)
    {
       merge(another);
       MolapKeyValueGroup group = (MolapKeyValueGroup)another;
       keys.addAll(group.keys);
       values.addAll(group.values);
       return this;
    }
    
    public List<MolapKey> getKeys()
    {
        return keys;
    }
    
    public List<MolapValue> getAllValues()
    {
        return values;
    }
    
   
    
}
