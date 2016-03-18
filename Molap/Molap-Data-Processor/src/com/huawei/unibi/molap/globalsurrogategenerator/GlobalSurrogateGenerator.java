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

package com.huawei.unibi.molap.globalsurrogategenerator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Schema;




import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;

public class GlobalSurrogateGenerator 
{
	private GlobalSurrogateGeneratorInfo generatorInfo;
	
	private static final LogService LOGGER = LogServiceFactory.getLogService(GlobalSurrogateGenerator.class.getName());
	/**
     * molap schema object
     */
    private Schema schema;

    /**
     * molap cube object
     */
    private Cube cube;
    
 /*   public static void main(String[] args)
    {
    	GlobalSurrogateGeneratorInfo generatorInfo = new GlobalSurrogateGeneratorInfo();
    	generatorInfo.setCubeName("upgrade_behavior_cube");
    	generatorInfo.setTableName("Upgrade_behavior_FACT");
    	generatorInfo.setNumberOfPartition(3);
    	generatorInfo.setSchema(MolapSchemaParser.loadXML("G:/naresh/upgrade_schema_en.xml"));
    	generatorInfo.setStoreLocation("G:/naresh/store/store");
    	GlobalSurrogateGenerator globalSurrogateGenerator= new GlobalSurrogateGenerator(generatorInfo);
    	globalSurrogateGenerator.generateGlobalSurrogates();
	}*/
	
	public GlobalSurrogateGenerator(GlobalSurrogateGeneratorInfo generatorInfo) 
	{
		this.generatorInfo = generatorInfo;
		schema = generatorInfo.getSchema();
		cube = MolapSchemaParser.getMondrianCube(schema,
				generatorInfo.getCubeName());
	}
	
	public void generateGlobalSurrogates(int currentRestructNumber)
	{
		 String hdfsLocation = generatorInfo.getStoreLocation();
//		 System.out.println("HDFS Location: "+ hdfsLocation);
		 LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,"HDFS Location: "+ hdfsLocation);
		 int numberOfPartition = generatorInfo.getNumberOfPartition();
		 String[][] partitionLocation = new String[numberOfPartition][];
//		 String storeLocation= null;
		// StringBuffer storeLocation= null;
		 for (int i = 0; i < numberOfPartition; i++) 
		 {
//			storeLocation=hdfsLocation+ '/' + schema.name+'_'+i + '/' + cube.name+'_'+i;
			 StringBuffer storeLocation= new StringBuffer();
			 storeLocation.append(hdfsLocation);
			 storeLocation.append('/');
			 storeLocation.append(schema.name);
			 storeLocation.append('_');
			 storeLocation.append(i);
			 storeLocation.append('/');
			 storeLocation.append(cube.name);
			 storeLocation.append('_');
			 storeLocation.append(i);
			 
			int restrctFolderCount = currentRestructNumber/*MolapUtil
					.checkAndReturnNextRestructFolderNumber(hdfsLocation,"RS_")*/;
			if (restrctFolderCount == -1) {
				restrctFolderCount = 0;
			}
//			storeLocation = storeLocation + '/'
//					+ MolapCommonConstants.RESTRUCTRE_FOLDER
//					+ restrctFolderCount + '/' + generatorInfo.getTableName();
			
			 storeLocation.append('/');
			 storeLocation.append(MolapCommonConstants.RESTRUCTRE_FOLDER);
			 storeLocation.append(restrctFolderCount);
			 storeLocation.append('/');
			 storeLocation.append(generatorInfo.getTableName());
			
//			MolapUtil.getMolapLoadFromStore(MolapUtil.getSlicesFromHDFS(storeLocation,generatorInfo.getTableName()));
			partitionLocation[i]=MolapUtil.getSlices(storeLocation.toString(),generatorInfo.getTableName(),FileFactory.getFileType(storeLocation.toString()));
		 }
		 ExecutorService writerExecutorService = Executors.newFixedThreadPool(20);
		 LevelGlobalSurrogateGeneratorThread generatorThread = null;
		 CubeDimension[] cubeDims=generatorInfo.getCubeDimensions();
		 for (int i = 0; i < cubeDims.length; i++) 
		 {
			 generatorThread = new LevelGlobalSurrogateGeneratorThread(partitionLocation, cubeDims[i], schema, generatorInfo.getTableName(),generatorInfo.getPartiontionColumnName());
			 writerExecutorService.submit(generatorThread);
		 }
		 writerExecutorService.shutdown();
		 try 
		 {
				writerExecutorService.awaitTermination(2, TimeUnit.DAYS);
		 } 
		 catch (InterruptedException e) 
		 {
//			e.printStackTrace();
			 LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,e,e.getMessage());
		 }
	}
}
