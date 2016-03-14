package com.huawei.datasight.molap.load;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;

public class DeletedLoadMetadata implements Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 7083059404172117208L;
	private Map<String,String> deletedLoadMetadataMap = new HashMap<String,String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

	public void addDeletedLoadMetadata(String loadId,String status) {
		 deletedLoadMetadataMap.put(loadId, status);
	}

	public  List<String >getDeletedLoadMetadataIds(){
		return new  ArrayList<String>(deletedLoadMetadataMap.keySet());
	}
	public String getDeletedLoadMetadataStatus(String loadId)
	{
		if(deletedLoadMetadataMap.containsKey(loadId)){
			return deletedLoadMetadataMap.get(loadId);
		}
		else {
			return null;
		}
			
	}
	
}
