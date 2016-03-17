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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBN+2z9M3qDOdUlfZW7sWyHxsb1yLilJa511JHUgUkngXlMbQ8aNmX7rQbBr4nanQ6fw2Z
epnnrZvZm9VRVk7nLtSs49sk6J0VmkNhidVdaZePgFeUY0lXAUgtFsQV0pYBBw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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
package com.huawei.unibi.molap.dataprocessor.queue.impl;

import java.util.Comparator;

import com.huawei.unibi.molap.dataprocessor.record.holder.DataProcessorRecordHolder;

/**
 * 
 * @author V00900840
 *
 */
public class RecordComparator implements Comparator<DataProcessorRecordHolder>
{
    
    /**
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     * 
     */
    @Override
    public int compare(DataProcessorRecordHolder o1,
            DataProcessorRecordHolder o2)
    {

        if(o1 == null)
        {
            return -1;
        }
        if(o2 == null)
        {
            return 1;
        }

        if(o1.getSeqNumber() < o2.getSeqNumber())
        {
            return -1;
        }
        else if(o1.getSeqNumber() > o2.getSeqNumber())
        {
            return 1;
        }
        return 0;
    }

}
