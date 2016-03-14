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
 * Created Date :02-Aug-2013 9:54:43 PM
 * FileName : LoadFolderData.java
 * Class Description :
 * Version 1.0
 */
public class LoadFolderData extends AbstractFileManager
{
    /**
     * folder name
     */
    private String folderName;

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.AbstractFileManager#add(com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite)
     * 
     */
    @Override
    public void add(IFileManagerComposite customData)
    {
        super.add(customData);
    }

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.AbstractFileManager#rename(com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite)
     * 
     */
    @Override
    public boolean rename(IFileManagerComposite composite)
    {
        // TODO Auto-generated method stub
        
        return false;
    }

    /**
     * 
     * @see com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite#setName(java.lang.String)
     * 
     */
    @Override
    public void setName(String name)
    {
        this.folderName = name;
        
        
    }

    /**
     * 
     * @return Returns the folderName.
     * 
     */
    public String getFolderName()
    {
        return folderName;
    }
    
}

