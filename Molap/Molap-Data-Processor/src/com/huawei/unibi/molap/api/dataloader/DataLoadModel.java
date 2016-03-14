/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.api.dataloader;


/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap
 * Author V00900840
 * Created Date :10-Jul-2013 7:00:03 PM
 * FileName : DataLoadModel.java
 * Class Description : This is model class to hold the information about the parameters required for 
 * creating graph file.
 * Version 1.0
 */
public class DataLoadModel
{
    /**
     * Schema Info
     */
    private SchemaInfo schemaInfo;
    
    /**
     * table table
     */
    private String tableName;
    
    /**
     * is CSV load
     */
    private boolean isCsvLoad;
    
    
    /**
     * Modified Dimension
     */
    private String[] modifiedDimesion;

    /**
     * 
     * @return Returns the schemaInfo.
     * 
     */
    public SchemaInfo getSchemaInfo()
    {
        return schemaInfo;
    }

    /**
     * 
     * @param schemaInfo The schemaInfo to set.
     * 
     */
    public void setSchemaInfo(SchemaInfo schemaInfo)
    {
        this.schemaInfo = schemaInfo;
    }

    /**
     * 
     * @return Returns the tableName.
     * 
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * 
     * @param tableName The tableName to set.
     * 
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * 
     * @return Returns the isCsvLoad.
     * 
     */
    public boolean isCsvLoad()
    {
        return isCsvLoad;
    }

    /**
     * 
     * @param isCsvLoad The isCsvLoad to set.
     * 
     */
    public void setCsvLoad(boolean isCsvLoad)
    {
        this.isCsvLoad = isCsvLoad;
    }

    /**
     * 
     * @return Returns the modifiedDimesion.
     * 
     */
    public String[] getModifiedDimesion()
    {
        return modifiedDimesion;
    }

    /**
     * 
     * @param modifiedDimesion The modifiedDimesion to set.
     * 
     */
    public void setModifiedDimesion(String[] modifiedDimesion)
    {
        this.modifiedDimesion = modifiedDimesion;
    }
    

}

