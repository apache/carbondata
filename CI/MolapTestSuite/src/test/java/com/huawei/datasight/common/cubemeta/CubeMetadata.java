package com.huawei.datasight.common.cubemeta;

import org.apache.spark.sql.cubemodel.Partitioner;

public class CubeMetadata {
	private String schema;
	private String schemaName;
	private String cubeName;
	private String dataPath;
	private Partitioner partitioner;
	
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
	public String getCubeName() {
		return cubeName;
	}
	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}
	
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	
	public Partitioner getPartitioner() {
		return partitioner;
	}
	public void setPartitioner(Partitioner partitioner) {
		this.partitioner = partitioner;
	}
}
