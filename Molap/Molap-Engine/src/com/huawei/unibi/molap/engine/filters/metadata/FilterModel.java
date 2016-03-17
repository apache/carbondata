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
TTt3d7RXgq62sVvnXIp1Blu2u/F8oTodIH8N2LUdbYT5c3O9BmTczv85MGnRCcX/cbITigJh
hpTSyRd2fJHgX5scVoHgpzwKTSU0cybKgR+GdEGZ+DxJt6TLb97dqlhIYvlrHA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.engine.filters.metadata;

import java.io.Serializable;

/**
 * @author R00900208
 * 
 */
public class FilterModel implements Serializable
{

    /**
	 * 
	 */
    private static final long serialVersionUID = 2779048394090554034L;

    /**
     * Dimensions,filter index, the filter values
     */
    private byte[][][] filter;

    /**
     * 
     */
    private byte[][] maxKey;

    /**
     * 
     */
    private int maxSize;

    public FilterModel(byte[][][] filter, byte[][] maxKey, int maxSize)
    {
        this.filter = filter;
        this.maxKey = maxKey;
        this.maxSize = maxSize;
    }

    /**
     * @return the filter
     */
    public byte[][][] getFilter()
    {
        return filter;
    }

    /**
     * @param filter
     *            the filter to set
     */
    public void setFilter(byte[][][] filter)
    {
        this.filter = filter;
    }

    /**
     * @return the maxKey
     */
    public byte[][] getMaxKey()
    {
        return maxKey;
    }

    /**
     * @param maxKey
     *            the maxKey to set
     */
    public void setMaxKey(byte[][] maxKey)
    {
        this.maxKey = maxKey;
    }

    /**
     * @return the maxSize
     */
    public int getMaxSize()
    {
        return maxSize;
    }

    /**
     * @param maxSize
     *            the maxSize to set
     */
    public void setMaxSize(int maxSize)
    {
        this.maxSize = maxSize;
    }

}
