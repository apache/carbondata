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

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public class DropCubeUtils {
	public static void dropCube(String schemaName, String cubeName) throws Exception 
	{
		String storePath = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION_HDFS);
	    Cube cube = MolapMetadata.getInstance().getCube(schemaName + '_' + cubeName);
	    if(cube == null)
	    {
	    	throw new Exception("Cube "+cubeName +" of "+schemaName+" does not exist.");
	    }
	    String metaDataPath = cube.getMetaDataFilepath();
    	FileType fileType = FileFactory.getFileType(metaDataPath);
    
    	if(FileFactory.isFileExist(metaDataPath, fileType))
    	{
    	    MolapFile file = FileFactory.getMolapFile(metaDataPath, fileType);
    	    MolapUtil.deleteFoldersAndFilesSilent(file);
    	    MolapUtil.deleteFoldersAndFilesSilent(FileFactory.getMolapFile(storePath+"/"+schemaName+"_0/"+cubeName +"_0", fileType));
    	}
    	
	    MolapMetadata.getInstance().removeCube(schemaName + '_' + cubeName);
	    System.out.println("Cube "+cubeName +" of "+schemaName+" schema dropped syccessfully.");
	    
	  }
}
