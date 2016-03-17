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
 * Copyright (c) 2013
 * =====================================
 *
 */

package com.huawei.unibi.molap.datastorage.store.impl.key.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Commons 
 * Author K00900841
 * Created Date :21-May-2013 7:21:49 PM
 * FileName : SingleArrayKeyInMemoryStore.java
 * Class Description : 
 * 
 * Version 1.0
 */
public class SingleArrayKeyInMemoryStore extends AbstractSingleArrayKeyStore
{

    /**
     * 
     * 
     * @param size
     * @param elementSize
     *
     */
    public SingleArrayKeyInMemoryStore(int size, int elementSize)
    {
        super(size, elementSize);
    }

    /**
     * 
     * 
     * @param size
     * @param elementSize
     * @param offset
     * @param filePath
     * @param fileHolder
     * @param length
     *
     */
    public SingleArrayKeyInMemoryStore(int size, int elementSize, long offset, String filePath, FileHolder fileHolder, int length)
    {
        this(size, elementSize);
        datastore=fileHolder.readByteArray(filePath, offset, length);
    }
    
}
