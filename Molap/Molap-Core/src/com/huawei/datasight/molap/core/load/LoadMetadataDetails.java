package com.huawei.datasight.molap.core.load;

import java.io.Serializable;

//import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapVersion;

public class LoadMetadataDetails implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1106104914918491724L;
	private String timestamp;
	
	private String loadStatus;
	private String loadName;
	private String partitionCount;
	private String deletionTimestamp;
    public final String  versionNumber= MolapVersion.getDataVersion();
    private String loadStartTime;
    
    private String mergedLoadName;
    /**
     * visibility is used to determine whether to the load is visible or not.
     */
    private String visibility = "true";
	
	public String getPartitionCount() {
		return partitionCount;
	}

	public void setPartitionCount(String partitionCount) {
		this.partitionCount = partitionCount;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getLoadStatus() {
		return loadStatus;
	}

	public void setLoadStatus(String loadStatus) {
		this.loadStatus = loadStatus;
	}

	public String getLoadName() {
		return loadName;
	}

	public void setLoadName(String loadName) {
		this.loadName = loadName;
	}
	
	/**
     * @return the deletionTimestamp
     */
    public String getDeletionTimestamp()
    {
        return deletionTimestamp;
    }

    /**
     * @param deletionTimestamp the deletionTimestamp to set
     */
    public void setDeletionTimestamp(String deletionTimestamp)
    {
        this.deletionTimestamp = deletionTimestamp;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((loadName == null) ? 0 : loadName.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
		if (obj == null)
		{
			return false;

		}
		if (!(obj instanceof LoadMetadataDetails)) 
		{
			return false;
		}
		LoadMetadataDetails other = (LoadMetadataDetails) obj;
		if (loadName == null) 
		{
			if (other.loadName != null)
			{
				return false;
			}
		} 
		else if (!loadName.equals(other.loadName))
		{
			return false;
		}
		return true;
	}

    /**
     * 
     * @param loadStartTime
     */
    public void setLoadStartTime(String loadStartTime)
    {
        this.loadStartTime = loadStartTime;
        
    }

    /**
     * @return the startLoadTime
     */
    public String getLoadStartTime()
    {
        return loadStartTime;
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
    /**
     * @return the visibility
     */
    public String getVisibility()
    {
        return visibility;
    }

    /**
     * @param visibility the visibility to set
     */
    public void setVisibility(String visibility)
    {
        this.visibility = visibility;
    }

}
