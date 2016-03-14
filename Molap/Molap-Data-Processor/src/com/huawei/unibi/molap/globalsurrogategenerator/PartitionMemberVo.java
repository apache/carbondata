package com.huawei.unibi.molap.globalsurrogategenerator;

import java.util.Map;

public class PartitionMemberVo
{
	private String path;
	
	private Map<String,Integer> membersMap;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Map<String, Integer> getMembersMap() {
		return membersMap;
	}

	public void setMembersMap(Map<String, Integer> membersMap) {
		this.membersMap = membersMap;
	}
	
	
}
