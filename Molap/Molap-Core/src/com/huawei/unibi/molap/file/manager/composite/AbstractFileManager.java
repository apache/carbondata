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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.file.manager.composite;

import java.util.ArrayList;
import java.util.List;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :02-Aug-2013 9:50:19 PM
 * FileName : AbstractFileManager.java
 * Class Description : 
 * Version 1.0
 */
public abstract class AbstractFileManager implements IFileManagerComposite
{
    /**
     * listOfFileData, composite parent which holds the different objects
     */
    protected List<IFileManagerComposite> listOfFileData = new ArrayList<IFileManagerComposite>(10);

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite#add(com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite)
     * 
     */
    @Override
    public void add(IFileManagerComposite customData)
    {
       listOfFileData.add(customData);
        
        
    }

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite#remove(com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite)
     * 
     */
    @Override
    public void remove(IFileManagerComposite customData)
    {
        listOfFileData.remove(customData);
        
    }

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite#get()
     * 
     */
    @Override
    public IFileManagerComposite get(int i)
    {
        return listOfFileData.get(i);
    }
    
    /**
     * Renames the File/Folders
     * 
     * @param composite
     * @return
     *
     */
    public abstract boolean rename(IFileManagerComposite composite);
    
    /**
     * Return the size
     * 
     * @return
     *
     */
    public int size()
    {
        return listOfFileData.size();
    }
    
}

