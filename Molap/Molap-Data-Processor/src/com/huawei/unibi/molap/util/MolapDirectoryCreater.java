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

package com.huawei.unibi.molap.util;

import java.io.File;

import com.huawei.unibi.molap.exception.MolapDataProcessorException;

public final class MolapDirectoryCreater
{
    
    /**
     * will be used to get the lock on put row method
     */
    private static final Object GETRSLOCK = new Object();
    
    private MolapDirectoryCreater()
    {
    	
    }
    
    /**
     * This method will be used to create the RS folder 
     * 
     * @param baseStorePath
     *          base path
     * @return created path
     * @throws MolapDataProcessorException
     *          MolapDataProcessorException
     *
     */
    public static String createRSDirectory(String baseStorePath) throws MolapDataProcessorException
    {
        synchronized(GETRSLOCK)
        {
            File file = null;
            file = new File(baseStorePath);
            if(!file.exists() && !file.mkdirs())
            {
				throw new MolapDataProcessorException(
						"Problem while creating the RS Directory: "
								+ baseStorePath);
            }
            return file.getAbsolutePath();
        }
    }
}
