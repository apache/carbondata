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
 * Copyright (c) 2012
 * =====================================
 *
 */
package org.carbondata.processing.engine.datastorage;


/**
 *
 * @author K00900841
 *
 */
public class CubeSliceLoader
{

    private static CubeSliceLoader instance = new CubeSliceLoader();

    /**
     * Create instance for calling of action sequence.
     *
     * @return added by liupeng 00204190
     */
    public static CubeSliceLoader getInstance()
    {
        return instance;
    }

    public void loadSliceFromFiles(String filesLocaton)
    {
    }
    public void updateSlices(String newSlicePath,String[] slicePathsToDelete)
    {

    }
}
