/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
