package com.huawei.unibi.molap.dataprocessor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;



public class DataProcessTaskStatus implements IDataProcessStatus, Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * DataLoader Status Identifier.
     */
    private int dataloadstatusid;

    /**
     * 
     */
    private Timestamp createdTime;

    /**
     * Status Identifier.
     */
    private String key;

    /**
     * Status .
     */
    private String status;

    /**
     * description for the task
     */
    private String desc;

    /**
     * task type
     */
    private int taskType;

    /**
     * 
     */
    private String schemaName;

    /**
     * 
     */
    private String cubeName;

    /**
     * 
     */
    private String tableName;

    /**
     * 
     */
    private String newSchemaFilePath;

    /**
     * 
     */
    private String oldSchemaFilePath;

    /**
     * 
     */
    private String csvFilePath;
    
    /**
     *dimCSVDirLoc 
     */
    private String dimCSVDirLoc;

    /**
     * 
     */
    private String dimTables;
    
    private boolean isDirectLoad;
    private List<String> filesToProcess;
    private String csvHeader;
    private String csvDelimiter;

    public boolean isDirectLoad() {
		return isDirectLoad;
	}

	public void setDirectLoad(boolean isDirectLoad) {
		this.isDirectLoad = isDirectLoad;
	}

	public List<String> getFilesToProcess() {
		return filesToProcess;
	}

	public void setFilesToProcess(List<String> filesToProcess) {
		this.filesToProcess = filesToProcess;
	}

	public String getCsvHeader() {
		return csvHeader;
	}

	public void setCsvHeader(String csvHeader) {
		this.csvHeader = csvHeader;
	}

	public String getCsvDelimiter() {
		return csvDelimiter;
	}

	public void setCsvDelimiter(String csvDelimiter) {
		this.csvDelimiter = csvDelimiter;
	}


    /**
     * Set if the call to restructre from path or by upload
     */
    private boolean isFromPathApi;

    public DataProcessTaskStatus(String schemaName, String cubeName, String tableName)
    {
        this.schemaName = schemaName;
        this.cubeName = cubeName;
        this.tableName = tableName;
        this.desc = "";
        this.setNewSchemaFilePath("");
        this.setOldSchemaFilePath("");
    }

    public DataProcessTaskStatus()
    {
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param cubeName
     *            the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }
    
    /**
     * @param schemaName
     *            the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }
    /**
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getDesc()
    {
        return desc;
    }

    public void setDesc(String desc)
    {
        this.desc = desc;
    }

    /**
     * 
     * @see com.huawei.unibi.platform.api.dataloader.IDataLoaderStatus#getId()
     * 
     */
    @Override
    public String getKey()
    {
        return key;
    }

    /**
     * 
     * @see com.huawei.unibi.platform.api.dataloader.IDataLoaderStatus#getStatus()
     * 
     */
    @Override
    public String getStatus()
    {
        return status;
    }

    /**
     * 
     * @param key
     *            The key to set.
     * 
     */
    public void setKey(String key)
    {
        this.key = key;
    }

    /**
     * 
     * @param status
     *            The status to set.
     * 
     */
    public void setStatus(String status)
    {
        this.status = status;
    }

    public int getDataloadstatusid()
    {
        return dataloadstatusid;
    }

    public void setDataloadstatusid(int dataloadstatusid)
    {
        this.dataloadstatusid = dataloadstatusid;
    }


    /**
     * @return the createdTime
     */
    public Timestamp getCreatedTime()
    {
        return createdTime;
    }

    /**
     * @param createdTime
     *            the createdTime to set
     */
    public void setCreatedTime(Timestamp createdTime)
    {
        this.createdTime = createdTime;
    }

    /**
     * 
     * @return Returns the taskType.
     * 
     */
    public int getTaskType()
    {
        return taskType;
    }

    public void setTaskType(int taskType)
    {
        this.taskType = taskType;
    }

    /**
     * @return the oldSchemaFilePath
     */
    public String getOldSchemaFilePath()
    {
        return oldSchemaFilePath;
    }

    /**
     * @param oldSchemaFilePath
     *            the oldSchemaFilePath to set
     */
    public void setOldSchemaFilePath(String oldSchemaFilePath)
    {
        this.oldSchemaFilePath = oldSchemaFilePath;
    }

    /**
     * @return the newSchemaFilePath
     */
    public String getNewSchemaFilePath()
    {
        return newSchemaFilePath;
    }

    /**
     * @param newSchemaFilePath
     *            the newSchemaFilePath to set
     */
    public void setNewSchemaFilePath(String newSchemaFilePath)
    {
        this.newSchemaFilePath = newSchemaFilePath;
    }

    /**
     * @return the csvFilePath
     */
    public String getCsvFilePath()
    {
        return csvFilePath;
    }

    /**
     * @param csvFilePath
     *            the csvFilePath to set
     */
    public void setCsvFilePath(String csvFilePath)
    {
        this.csvFilePath = csvFilePath;
    }

    /**
     * @return the dimTables
     */
    public String getDimTables()
    {
        return dimTables;
    }

    /**
     * @param dimTables
     *            the dimTables to set
     */
    public void setDimTables(String dimTables)
    {
        this.dimTables = dimTables;
    }

    /**
     * @return the isFromPathApi
     */
    public boolean isFromPathApi()
    {
        return isFromPathApi;
    }

    /**
     * @param isFromPathApi the isFromPathApi to set
     */
    public void setFromPathApi(boolean isFromPathApi)
    {
        this.isFromPathApi = isFromPathApi;
    }
    
    /**
     * to make a copy
     * @return
     */
    public IDataProcessStatus makeCopy()
    {
        IDataProcessStatus copy = new DataProcessTaskStatus();
        copy.setCubeName(this.cubeName);
        copy.setDataloadstatusid(this.dataloadstatusid);
        copy.setDesc(this.desc);
        copy.setKey(this.key);
        copy.setSchemaName(schemaName);
        copy.setStatus(status);
        return copy;
    }

    public String getDimCSVDirLoc() {
        return dimCSVDirLoc;
    }

    public void setDimCSVDirLoc(String dimCSVDirLoc) {
        this.dimCSVDirLoc = dimCSVDirLoc;
    }
    
}
