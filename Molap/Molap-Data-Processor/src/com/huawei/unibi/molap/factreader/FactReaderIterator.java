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

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.util.MolapSliceAndFiles;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : FactReaderIterator.java
 * Class Description : Iterator class to iterate over leaf node and return the tuple
 * Class Version 1.0
 */
public class FactReaderIterator implements MolapIterator<Object[]>
{
    /**
     * mdkeyLength
     */
    private int mdkeyLength;
    
    /**
     * measureCount
     */
    private int measureCount;

    /**
     * keyGenerator
     */
    private KeyGenerator keyGenerator;
    
    /**
     * slice model list
     */
    private List<MolapSliceTupleIterator> sliceHolderList;
    
    /**
     * slice holder
     */
    private MolapIterator<MolapSurrogateTupleHolder> molapSliceTupleIterator;
    
    /**
     * output record size
     */
    private int outRecordSize;
    
    /**
     * counter
     */
    private int counter;
    
    /**
     * totalSize
     */
    private int totalSize;
    
    
    /**
     * FactReaderIterator iterator
     * @param measureCount
     * @param dimLens
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param isReadOnInProgress
     */
    public FactReaderIterator(int measureCount, int[] dimLens,
            String schemaName, String cubeName, String tableName,
            boolean isReadOnInProgress,
            List<MolapSliceAndFiles> sliceFactFilesList, String[] aggType)
    {
        this.measureCount = measureCount;
        this.keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimLens);
        this.mdkeyLength=keyGenerator.getKeySizeInBytes();
        this.outRecordSize=measureCount+1;
        this.sliceHolderList = new ArrayList<MolapSliceTupleIterator>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        MolapSliceTupleIterator molapSliceTupleIterator = null;
        //CHECKSTYLE:OFF    Approval No:Approval-367
        for(MolapSliceAndFiles  sliceFactFile: sliceFactFilesList)
        {//CHECKSTYLE:ON
            molapSliceTupleIterator = new MolapSliceTupleIterator(sliceFactFile
                    .getPath(), sliceFactFile.getSliceFactFilesList(), tableName,
                    mdkeyLength, this.measureCount,aggType);
            this.sliceHolderList.add(molapSliceTupleIterator);
        }
        totalSize=this.sliceHolderList.size();
        if(totalSize>0)
        {
            this.molapSliceTupleIterator= this.sliceHolderList.get(counter++);
        }
    }
    
    /**
     * below method will be used to get the tuple
     */
    @Override
    public Object[] next()
    {
        Object[] rowObj = new Object[this.outRecordSize];
        MolapSurrogateTupleHolder next=molapSliceTupleIterator.next();
        Object[] measures = next.getMeasures();
        System.arraycopy(measures, 0, rowObj, 0, measures.length);
        rowObj[measures.length]=next.getMdKey();
        return rowObj;
    }

    /**
     * check whether more tuples are present
     */
    @Override
    public boolean hasNext()
    {
        if(null!=molapSliceTupleIterator && molapSliceTupleIterator.hasNext())
        {
            return true;
        }
        else
        {
            if(counter<totalSize)
            {
                molapSliceTupleIterator=sliceHolderList.get(counter++);
                return true;
            }
        }
        return false;
    }

   
}
