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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBsDLVq/YKyh6HtHHa1yn3o0WSvKoE2AYc3ABFIxK4P/du2tStgLT2mLgioNW9/3BKOFV
7/8cebfCrnQDbYT5LwLufN6xf3VrmVW9VlGpoBNDNvsgdgsjtkkn4G/y3cH6nA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.datastorage.streams;

import java.util.List;

import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :25-Jun-2013
 * FileName : DataInputStream.java
 * Class Description : 
 * Version 1.0
 */
public interface DataInputStream
{
    void initInput();

    void closeInput();

    ValueCompressionModel getValueCompressionMode();
    
    List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar();
    
    List<LeafNodeInfo> getLeafNodeInfo();
    
    Pair getNextHierTuple();
    
    byte[] getStartKey();
}
