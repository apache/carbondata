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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnPhfbxNTG9fxb2HSA0kk2XIbOSyB2Es+vd8aIBNzy2zw6r42bC6vrYXTgCzt4v8xQVm2
vCiD+MUBy5sK6pmSa5YCJgXswRkefBQElATSZlZBIBFDpQauppQer/nQFx8c1Q==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.dimension.load.command.invoker;

import com.huawei.unibi.molap.dimension.load.command.DimensionLoadCommand;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :14-Nov-2013 6:13:57 PM
 * FileName : DimensionLoadActionInvoker.java
 * Class Description :
 * Version 1.0
 */
public class DimensionLoadActionInvoker
{
    /**
     * Dimension Load
     */
    private DimensionLoadCommand dimensionLoadCommand;
    /**
     * 
     * 
     */
    public DimensionLoadActionInvoker(DimensionLoadCommand loadCommand)
    {
        this.dimensionLoadCommand = loadCommand;
    }
    
    /**
     * Executes the command
     * @throws Exception 
     * 
     *
     */
    public void execute() throws Exception
    {
        dimensionLoadCommand.execute();
    }

}

