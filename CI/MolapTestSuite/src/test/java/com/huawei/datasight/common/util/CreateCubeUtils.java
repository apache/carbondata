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

package com.huawei.datasight.common.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.spark.sql.cubemodel.Partitioner;
import org.eigenbase.xom.XOMException;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapVersion;

public class CreateCubeUtils {
	public static void createCube(String schemaName, String cubeName,
			String partitionColumn) throws IOException, XOMException
	{
	  String storePath = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION);
	  String schemaXMLPath =  MolapProperties.getInstance().getProperty("molap.testdata.path") + 
			  "/"+cubeName + ".xml";
	  String schemaXML = CommonUtils.parseXMLPathToSchemaString(schemaXMLPath);
	  MolapMetadata.getInstance().removeCube(schemaName+"_"+cubeName);
	  MolapMetadata.getInstance().loadSchema(CommonUtils.createSchemaObjectFromXMLString(schemaXML));
	  
	  String cubeMetaDataPath = storePath + "/schemas/" + schemaName + "/" + cubeName;
	  
	  FileType fileType = FileFactory.getFileType(storePath);
	  
	  if(!FileFactory.isFileExist(cubeMetaDataPath, fileType))
	  {
	    FileFactory.mkdirs(cubeMetaDataPath, fileType);
	  }
	  DataOutputStream out = FileFactory.getDataOutputStream(cubeMetaDataPath + "/"+"metadata", fileType);
	  
	    byte[] schemaNameBytes = schemaName.getBytes();
	    byte[] cubeNameBytes = cubeName.getBytes();
	    byte[] dataPathBytes = (storePath+"/store").getBytes();
	    byte[] schemaArray = schemaXML.getBytes();
	    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
	    ObjectOutputStream objStream = new ObjectOutputStream(outStream);
	    objStream.writeObject(new Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl",
	    		new String[]{partitionColumn}, 1, new String[]{"localhost"}));
	    objStream.close();
	    byte[] partitionArray = outStream.toByteArray();
	    byte[] versionNoBytes = MolapVersion.getCubeVersion().getBytes();
	    out.writeInt(schemaNameBytes.length);
	    out.write(schemaNameBytes);
	    out.writeInt(cubeNameBytes.length);
	    out.write(cubeNameBytes);
	    out.writeInt(dataPathBytes.length);
	    out.write(dataPathBytes);
	    out.writeInt(versionNoBytes.length);
	    out.write(versionNoBytes);
	    out.writeInt(schemaArray.length);
	    out.write(schemaArray);
	    out.writeInt(partitionArray.length);
	    out.write(partitionArray);
	    out.close();
	    
	    System.out.println("Cube "+cubeName+" for schema "+schemaName+" created successfully.");
	}
}
