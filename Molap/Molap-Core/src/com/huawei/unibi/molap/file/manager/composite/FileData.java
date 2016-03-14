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

import com.huawei.unibi.molap.writer.HierarchyValueWriterForCSV;
import com.huawei.unibi.molap.writer.LevelValueWriter;


/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :02-Aug-2013 9:55:14 PM
 * FileName : FileData.java
 * Class Description :
 * Version 1.0
 */
public class FileData extends AbstractFileManager
{
    /**
     * File Name
     */
    private String fileName;
    
    /**
     * Store Path
     */
    private String storePath;
    
    /**
     * levelValueWriter
     */
    private LevelValueWriter levelValueWriter;
    
    /**
     * hierarchyValueWriter
     */
    private HierarchyValueWriterForCSV hierarchyValueWriter;
    
    public FileData(String fileName , String storePath)
    {
        this.fileName = fileName;
        this.storePath = storePath;
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
        this.fileName = name;
    }

    /**
     * 
     * @return Returns the fileName.
     * 
     */
    public String getFileName()
    {
        return fileName;
    }

    /**
     * 
     * @return Returns the storePath.
     * 
     */
    public String getStorePath()
    {
        return storePath;
    }

    /**
     * get LevelValueWriter
     * 
     * @return
     *
     */
    public LevelValueWriter getLevelValueWriter()
    {
        return levelValueWriter;
    }

    /**
     * set level ValueWriter
     * 
     * @param levelValueWriter
     *
     */
    public void setLevelValueWriter(LevelValueWriter levelValueWriter)
    {
        this.levelValueWriter = levelValueWriter;
    }

    /**
     * get Hierarchy Value writer
     * 
     * @return
     *
     */
    public HierarchyValueWriterForCSV getHierarchyValueWriter()
    {
        return hierarchyValueWriter;
    }

    /**
     * Set Hierarchy Value Writer.
     * 
     * @param hierarchyValueWriter
     *
     */
    public void setHierarchyValueWriter(
            HierarchyValueWriterForCSV hierarchyValueWriter)
    {
        this.hierarchyValueWriter = hierarchyValueWriter;
    }

}

