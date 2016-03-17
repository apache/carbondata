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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaWtQwKKBpns/1ErtydbNem/maSpseAVK/I0KJ9rs97B12LeCv7sUdkf/ThyL5Pdesal3
D6R1u2wxk/+wSJtVwjBxROV7SXY6FDOfUDSv7fZ0Oga/65mX+/Pm+xvd6p2hZA==*/
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
package com.huawei.unibi.molap.merger.Util;

import java.io.File;
import java.util.List;

import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : SliceInfo.java
 * Class Description : This class is responsible for holding the slice information 
 * Version 1.0
 */
public class SliceInfo
{
	private File tableFolder;
	
    /**
     * getTableFolder
     * @return File
     */
    public File getTableFolder() {
		return tableFolder;
	}

	/**
	 * setTableFolder
	 * @param tableFolder void
	 */
	public void setTableFolder(File tableFolder) {
		this.tableFolder = tableFolder;
	}

	/**
     * leafNodeInfoList
     */
    private List<LeafNodeInfo> leafNodeInfoList;
    
    /**
     * compressionModel
     *  
     */
    private ValueCompressionModel compressionModel;
    
    /**
     * sliceLocation
     */
    private String sliceLocation;
    
    /**
     * measureCount
     */
    private int measureCount;
    
    /**
     * Will return list of leaf meta data 
     * 
     * @return leafNodeInfoList
     *          leafNodeInfoList
     *
     */
    public List<LeafNodeInfo> getLeafNodeInfoList()
    {
        return leafNodeInfoList;
    }

    /**
     * This method will set the leafNodeinfo list
     * 
     * @param leafNodeInfoList
     *          leafNodeInfoList
     *
     */
    public void setLeafNodeInfoList(List<LeafNodeInfo> leafNodeInfoList)
    {
        this.leafNodeInfoList = leafNodeInfoList;
    }

    /**
     * This method will be used to get the compression model 
     * 
     * @return compressionModel
     *             compressionModel
     *
     */
    public ValueCompressionModel getCompressionModel()
    {
        return compressionModel;
    }

    /**
     * This method will be used to set the compression model 
     * 
     * @param compressionModel
     *          compressionModel
     *
     */
    public void setCompressionModel(ValueCompressionModel compressionModel)
    {
        this.compressionModel = compressionModel;
    }

    /**
     * This method will be used to get the slice location
     * 
     * @return slice location
     *
     */
    public String getSliceLocation()
    {
        return sliceLocation;
    }

    /**
     * This method will be used to set the slice location 
     * 
     * @param sliceLocation
     *          sliceLocation
     *
     */
    public void setSliceLocation(String sliceLocation)
    {
        this.sliceLocation = sliceLocation;
    }

    public int getMeasureCount()
    {
        return measureCount;
    }

    public void setMeasureCount(int measureCount)
    {
        this.measureCount = measureCount;
    }
}
