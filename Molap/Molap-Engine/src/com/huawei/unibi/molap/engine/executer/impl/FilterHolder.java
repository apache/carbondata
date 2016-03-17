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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7HyuasgFkAs610bSK/ld3q3O8fOXtc4/yW8M36tmlp+88jHdLoPgbKbrUDUHa2N9qI5B
VoQzihhWWDlItUJZTGdnSfi/qFITyUFJXvqxJtduS/g3zYou9BQF04u28OIcbg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.impl;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.filter.MolapFilterInfo;

/**
 * FilterHolder
 * @author R00900208
 *
 */
public class FilterHolder
{
    /**
     * dimension
     */
    private Dimension dimension;
    /**
     * filterInfo
     */
    private MolapFilterInfo filterInfo;
    /**
     * @return the dimension
     */
    public Dimension getDimension()
    {
        return dimension;
    }
    /**
     * @param dimension the dimension to set
     */
    public void setDimension(Dimension dimension)
    {
        this.dimension = dimension;
    }
    /**
     * @return the filterInfo
     */
    public MolapFilterInfo getFilterInfo()
    {
        return filterInfo;
    }
    /**
     * @param filterInfo the filterInfo to set
     */
    public void setFilterInfo(MolapFilterInfo filterInfo)
    {
        this.filterInfo = filterInfo;
    }
    
    
}

