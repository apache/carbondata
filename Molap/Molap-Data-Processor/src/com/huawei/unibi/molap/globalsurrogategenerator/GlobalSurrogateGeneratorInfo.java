package com.huawei.unibi.molap.globalsurrogategenerator;

import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Schema;

public class GlobalSurrogateGeneratorInfo {

	private String cubeName;

	private String tableName;

	private int numberOfPartition;

	private Schema schema;

	private String storeLocation;

	private CubeDimension[] cubeDimensions;

	/**
	 * 
	 */
	private String partiontionColumnName;

	public String getCubeName() {
		return cubeName;
	}

	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getNumberOfPartition() {
		return numberOfPartition;
	}

	public void setNumberOfPartition(int numberOfPartition) {
		this.numberOfPartition = numberOfPartition;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public String getStoreLocation() {
		return storeLocation;
	}

	public void setStoreLocation(String storeLocation) {
		this.storeLocation = storeLocation;
	}

	public String getPartiontionColumnName() {
		return partiontionColumnName;
	}

	public void setPartiontionColumnName(String partiontionColumnName) {
		this.partiontionColumnName = partiontionColumnName;
	}

	public CubeDimension[] getCubeDimensions() {
		return cubeDimensions;
	}

	public void setCubeDimensions(CubeDimension[] cubeDimensions) {
		this.cubeDimensions = cubeDimensions;
	}
}