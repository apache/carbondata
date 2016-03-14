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


/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :02-Aug-2013 9:45:46 PM
 * FileName : IFileManagerComposite.java
 * Class Description :
 * Version 1.0
 */
public interface IFileManagerComposite
{
    /**
     * Add the data which can be either row Folder(Composite) or File 
     * 
     * @param customData
     */
    void add(IFileManagerComposite customData);

    /**
     * Remove the CustomData type object from the IFileManagerComposite object hierarchy.
     * @param customData
     */
    void remove(IFileManagerComposite customData);

    /**
     * get the CustomData type object name
     * @param name
     * @return CustomDataIntf type
     */
    IFileManagerComposite get(int i);

    /**
     * set the CustomData type object name
     * @param name
     */
    void setName(String name);
    
    /**
     * Get the size
     * @return
     *
     */
    int size();


}

