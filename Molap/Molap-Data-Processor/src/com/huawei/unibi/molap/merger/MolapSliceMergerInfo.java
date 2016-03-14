package com.huawei.unibi.molap.merger;

import java.util.List;

import com.huawei.unibi.molap.olap.MolapDef.Schema;

public class MolapSliceMergerInfo
{
    private String schemaName;
    
    private String cubeName;
    
    private String tableName;
    
    private String partitionID;
    
    private Schema schema;
    
    private String schemaPath;
    
    private String metadataPath;
    
    private List<String> loadsToBeMerged;
    
    private String mergedLoadName;

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }
    
    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }
    
   /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * @return the partitionID
     */
    public String getPartitionID()
    {
        return partitionID;
    }

    /**
     * @param partitionID the partitionID to set
     */
    public void setPartitionID(String partitionID)
    {
        this.partitionID = partitionID;
    }

    /**
     * @return the schema
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }

    /**
     * @return the schemaPath
     */
    public String getSchemaPath()
    {
        return schemaPath;
    }

    /**
     * @param schemaPath the schemaPath to set
     */
    public void setSchemaPath(String schemaPath)
    {
        this.schemaPath = schemaPath;
    }

    /**
     * @return the metadataPath
     */
    public String getMetadataPath()
    {
        return metadataPath;
    }

    /**
     * @param metadataPath the metadataPath to set
     */
    public void setMetadataPath(String metadataPath)
    {
        this.metadataPath = metadataPath;
    }

    /**
     * @return the loadsToBeMerged
     */
    public List<String> getLoadsToBeMerged()
    {
        return loadsToBeMerged;
    }

    /**
     * @param loadsToMerge the loadsToBeMerged to set
     */
    public void setLoadsToBeMerged(List<String> loadsToMerge)
    {
        this.loadsToBeMerged = loadsToMerge;
    }

    /**
     * @return the mergedLoadName
     */
    public String getMergedLoadName()
    {
        return mergedLoadName;
    }

    /**
     * @param mergedLoadName the mergedLoadName to set
     */
    public void setMergedLoadName(String mergedLoadName)
    {
        this.mergedLoadName = mergedLoadName;
    }
    
    
}
