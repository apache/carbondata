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


package com.huawei.unibi.molap.dimension.load.command.invoker;

import com.huawei.unibi.molap.dimension.load.command.DimensionLoadCommand;

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

