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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d0/3+mkJ6cIeSiU8ZpdJlfRNSGLseKB6XHiPq7/zhfHfGNAVuqRy9E2EklWbLVl7/e9a
Iw9MjrpjJu75S8mQfGVpFVB3+0BSCpB6IuyyIJiqR+w64KGqkrtPFJGRGchjcQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class Page
{
    private List<byte[]> keys;
    
    private List<MeasureAggregator[]> msrs;
    
    

    public Page(List<byte[]> keys, List<MeasureAggregator[]> msrs)
    {
        super();
        this.keys = keys;
        this.msrs = msrs;
    }

    /**
     * @return the keys
     */
    public List<byte[]> getKeys()
    {
        return keys;
    }

    /**
     * @return the msrs
     */
    public List<MeasureAggregator[]> getMsrs()
    {
        return msrs;
    }
    
    
}
