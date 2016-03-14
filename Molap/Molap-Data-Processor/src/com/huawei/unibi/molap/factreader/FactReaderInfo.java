package com.huawei.unibi.molap.factreader;


public class FactReaderInfo
{
    private int measureCount;

    private int[] dimLens;

    private String schemaName;

    private String cubeName;

    private String tableName;

    private int[] blockIndex;
    
    private boolean isUpdateMeasureRequired;
    
    public int getMeasureCount()
    {
        return measureCount;
    }

    public void setMeasureCount(int measureCount)
    {
        this.measureCount = measureCount;
    }

    public int[] getDimLens()
    {
        return dimLens;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
    
    public void setDimLens(int[] dimLens)
    {
        this.dimLens = dimLens;
    }

    public String getSchemaName()
    {
        return schemaName;
    }
    
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    } 

    public String getCubeName()
    {
        return cubeName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

//    public String[] getAggType()
//    {
//        return aggType;
//    }

//    public void setAggType(String[] aggType)
//    {
//        this.aggType = aggType;
//    }

    public int[] getBlockIndex()
    {
        return blockIndex;
    }

    public void setBlockIndex(int[] blockIndex)
    {
        this.blockIndex = blockIndex;
    }

	public boolean isUpdateMeasureRequired() {
		return isUpdateMeasureRequired;
	}

	public void setUpdateMeasureRequired(boolean isUpdateMeasureRequired) {
		this.isUpdateMeasureRequired = isUpdateMeasureRequired;
	}
}
