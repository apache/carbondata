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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbUlhnhxHLIdwdWZ/obpNJDuf53dPf4FIuUwljMwwbRw8/Kgbm3515ax/2gwyzTeejkC6
zfk/sk8ffvX4E6KlLVV07E0LEJIcSDsGdnjA3vxIyC1RrcYZS4Z2UZ5WAkc+Sw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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
package com.huawei.unibi.molap.util;

import java.io.File;

/** 
* Project Name NSE V3R7C00 
* Module Name : Molap Data Processor
* Author K00900841
* Created Date :21-May-2013 6:42:29 PM
* FileName : SliceAndFiles.java
* Class Description :  Below class will be use for holding slice path and its fact files
* Version 1.0
*/
public class SliceAndFiles
{
    /**
     * slice path
     */
    private String path;

    /**
     * slice fact files 
     */
    private File[] sliceFactFilesList;

    /**
     * This method will return the slice path
     * 
     * @return slice path
     *
     */
    public String getPath()
    {
        return path;
    }

    /**
     * This method will be used to set the slice path 
     * 
     * @param path
     *
     */
    public void setPath(String path)
    {
        this.path = path;
    }

    /**
     * This method will be used get the slice fact files 
     * 
     * @return slice fact files 
     *
     */
    public File[] getSliceFactFilesList()
    {
        return sliceFactFilesList;
    }

    /**
     * This method  will be used to set the slice fact files 
     * 
     * @param sliceFactFilesList
     *
     */
    public void setSliceFactFilesList(File[] sliceFactFilesList)
    {
        this.sliceFactFilesList = sliceFactFilesList;
    }
}
