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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.factreader;


/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapSliceTupleIterator.java
 * Class Description : holder class to hold the tuple
 * Class Version 1.0
 */
public class MolapSurrogateTupleHolder
{
    /**
     * surrogateKey
     */
    private byte[] mdKey;
    
    /**
     * measures
     */
    private Object[] measures;

    /**
     * @return the surrogateKey
     */
    public byte[] getMdKey()
    {
        return mdKey;
    }

    /**
     * @param surrogateKey the surrogateKey to set
     */
    public void setSurrogateKey(byte[] mdKey)
    {
        this.mdKey = mdKey;
    }

    /**
     * @return the measure
     */
    public Object[] getMeasures()
    {
        return measures;
    }

    /**
     * @param measure the measure to set
     */
    public void setMeasures(Object[] measures)
    {
        this.measures = measures;
    }

}
