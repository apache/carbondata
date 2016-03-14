package com.huawei.datasight.molap.datastats.model;

import java.util.List;

import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.olap.MolapDef;

public class LoadModel
{
	private MolapDef.Schema schema;

	private MolapDef.Cube cube;
	
	private String partitionId="0";

	private String tableName;

	private Cube metaCube;

	private String schemaName;

	private String cubeName;
	
	private String dataPath;
	
	private String metaDataPath;
	
	private List<String> validSlices;
	
	private List<String> validUpdateSlices;
	
	private List<String> calculatedLoads;
	
	private int restructureNo;
	
	private long cubeCreationtime;
	
	private long schemaLastUpdatedTime;

	/**
	 * this will have all load of store
	 */
	private List<String> allLoads;

	

	public String getTableName()
	{
		return tableName;
	}

	public void setCube(MolapDef.Cube cube)
	{
		this.cube=cube;
	}
	
	public void setSchema(MolapDef.Schema schema)
	{
		this.schema = schema;

	}

	public void setPartitionId(String partitionId)
	{
		this.partitionId = partitionId;

	}

	public MolapDef.Schema getSchema()
	{
		return schema;
	}

	public String getPartitionId()
	{
		return partitionId;
	}

	public void setTableName(String tableName)
	{
		this.tableName = tableName;

	}

	public MolapDef.Cube getCube()
	{
		return cube;
	}

	public void setMetaCube(Cube metaCube)
	{
		this.metaCube=metaCube;
		
	}
	
	public Cube getMetaCube()
	{
		return this.metaCube;
	}

	public void setSchemaName(String schemaName)
	{
		this.schemaName=schemaName;
		
	}
	public String getSchemaName()
	{
		return this.schemaName;
	}
	
	public void setCubeName(String cubeName)
	{
		this.cubeName=cubeName;
	}
	public String getCubeName()
	{
		return this.cubeName;
	}

	public String getDataPath()
	{
		return dataPath;
	}

	public void setDataPath(String dataPath)
	{
		this.dataPath = dataPath;
	}

	public String getMetaDataPath()
	{
		return metaDataPath;
	}

	public void setMetaDataPath(String metaDataPath)
	{
		this.metaDataPath = metaDataPath;
	}

	public List<String> getValidSlices()
	{
		return validSlices;
	}

	public void setValidSlices(List<String> validSlices)
	{
		this.validSlices = validSlices;
	}

	public List<String> getValidUpdateSlices()
	{
		return validUpdateSlices;
	}

	public void setValidUpdateSlices(List<String> validUpdateSlices)
	{
		this.validUpdateSlices = validUpdateSlices;
	}
	
	public void setRestructureNo(int restructureNo)
	{
		this.restructureNo=restructureNo;
	}
	
	public int getRestructureNo()
	{
		return this.restructureNo;
	}

	public List<String> getCalculatedLoads() 
	{
		return calculatedLoads;
	}

	public void setCalculatedLoads(List<String> calculatedLoads) 
	{
		this.calculatedLoads = calculatedLoads;
	}

    /**
     * 
     * @return Returns the cubeCreationtime.
     * 
     */
    public long getCubeCreationtime()
    {
        return cubeCreationtime;
    }

    /**
     * 
     * @param cubeCreationtime The cubeCreationtime to set.
     * 
     */
    public void setCubeCreationtime(long cubeCreationtime)
    {
        this.cubeCreationtime = cubeCreationtime;
    }

	public void setAllLoads(List<String> allLoads)
	{
		this.allLoads=allLoads;
		
	}
	
	public List<String> getAllLoads()
	{
		return this.allLoads;
	}

    /**
     * 
     * @return Returns the schemaLastUpdatedTime.
     * 
     */
    public long getSchemaLastUpdatedTime()
    {
        return schemaLastUpdatedTime;
    }

    /**
     * 
     * @param schemaLastUpdatedTime The schemaLastUpdatedTime to set.
     * 
     */
    public void setSchemaLastUpdatedTime(long schemaLastUpdatedTime)
    {
        this.schemaLastUpdatedTime = schemaLastUpdatedTime;
    }
	

}
