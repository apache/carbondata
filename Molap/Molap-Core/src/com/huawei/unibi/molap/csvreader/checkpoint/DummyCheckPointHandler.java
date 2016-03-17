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
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.csvreader.checkpoint;

import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.csvreader.checkpoint.exception.CheckPointException;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :DummyCheckPointHandler.java 
 * Class Description : DummyCheckPointHandler class 
 * Version 1.0
 */
public class DummyCheckPointHandler implements CheckPointInterface
{

    @Override
    public Map<String, Long> getCheckPointCache() throws CheckPointException
    {
        return new HashMap<String, Long>(0);
    }

    @Override
    public void saveCheckPointCache(Map<String, Long> checkPointCache)
            throws CheckPointException
    {
        
    }

    @Override
    public int getCheckPointInfoFieldCount()
    {
        return 0;
    }

    @Override
    public void updateInfoFields(Object[] inputRow, Object[] outputRow)
    {
        
    }
    
}
