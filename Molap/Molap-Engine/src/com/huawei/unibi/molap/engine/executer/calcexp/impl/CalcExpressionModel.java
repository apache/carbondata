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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOaMehuhS0lcz9dAho9/ck5SUPTL0ArmWpOcQLU27wWRVO1feo8jNSDfodMSlXLn1oWIb
+1IikiVn9uRFb4LsI8TGCTzITOcl14ru/4I23+kdytOgyxqP/vh9zq3xZvjCxQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import java.util.List;

import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * @author R00900208
 *
 */
public class CalcExpressionModel
{
    /**
     * 
     */
    private List<Measure> msrsList;

    /**
     * @return the msrsList
     */
    public List<Measure> getMsrsList()
    {
        return msrsList;
    }

    /**
     * @param msrsList the msrsList to set
     */
    public void setMsrsList(List<Measure> msrsList)
    {
        this.msrsList = msrsList;
    }
    
    
}
