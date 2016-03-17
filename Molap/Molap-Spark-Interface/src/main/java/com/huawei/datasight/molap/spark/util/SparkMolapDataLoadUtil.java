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

/**
 * 
 */
package com.huawei.datasight.molap.spark.util;

//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.Properties;

//import com.huawei.datasight.molap.load.MolapLoadModel;
//import com.huawei.datasight.spark.SparkQueryExecutor;
//import com.huawei.iweb.platform.logging.LogService;
//import com.huawei.iweb.platform.logging.LogServiceFactory;
//import com.huawei.unibi.molap.util.MolapCoreLogEvent;
//import com.huawei.unibi.molap.util.MolapProperties;


/**
 * 
 * @author K00900207
 *
 */
public class SparkMolapDataLoadUtil {
	
//	  private static final LogService LOGGER = LogServiceFactory.getLogService(SparkMolapDataLoadUtil.class.getName());

	/**
	 * @param args
	 */
	/*public static void main(String[] args) {
		loadPCC();
	}*/

	
	/*private static void loadPCC() {
	    
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://9.91.8.157:7077","G:/spark-1.0.0-rc3","/home/smu/PCC_Java.xml","PCC","ODM","/home/smu/store"); //9.91.8.157
//		SparkQueryExecutor exec = new SparkQueryExecutor("local","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3","/opt/ravi/SchemaYM.xml","Schema5D","Cube5D","/opt/ravi/store");
		
		Properties properties = loadProperties();
		
		String schemaName = properties.getProperty("dataloader.schema.name", "PCC");
		String cubeName = properties.getProperty("dataloader.cube.name", "ODM");
		String schemaPath = properties.getProperty("dataloader.schema.path", "/opt/ravi/PCC_Java.xml");
		String dimFolderPah = properties.getProperty("dataloader.dimsdata.path", "/opt/smartpccsim/NewDims");
		String factPath = properties.getProperty("dataloader.factdata.path", "/opt/smartpccsim/demodata");
		String factTable = properties.getProperty("dataloader.facttable.name", "SDR_PCC_USER_ALL_INFO_1DAY_16085");
		String storepath = properties.getProperty("dataloader.molap.storelocation.temp", "/opt/ravi/demostore/");
		String storepathHDFS = properties.getProperty("dataloader.molap.storelocation.hdfs");
		String sparkURL = properties.getProperty("spark.url", "spark://master:7077");
		String sparkDir = properties.getProperty("spark.home", "/opt/spark-1.0.0-rc3");
		
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"********** Properties Used ***************");
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.schema.name :" + schemaName);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.cube.name :" + cubeName);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.schema.path :" + schemaPath);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.dimsdata.path :" + dimFolderPah);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.factdata.path :" + factPath);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.facttable.name :" + factTable);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.molap.storelocation.temp :" + storepath);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"dataloader.molap.storelocation.hdfs :" + storepathHDFS);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"spark.url" + sparkURL);
		LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"spark.home" + sparkDir);
		System.out.println("********** Properties Used ***************");
		System.out.println("dataloader.schema.name :" + schemaName);
		System.out.println("dataloader.cube.name :" + cubeName);
		System.out.println("dataloader.schema.path :" + schemaPath);
		System.out.println("dataloader.dimsdata.path :" + dimFolderPah);
		System.out.println("dataloader.factdata.path :" + factPath);
		System.out.println("dataloader.facttable.name :" + factTable);
		System.out.println("dataloader.molap.storelocation.temp :" + storepath);
		System.out.println("dataloader.molap.storelocation.hdfs :" + storepathHDFS);
		System.out.println("spark.url" + sparkURL);
		System.out.println("spark.home" + sparkDir);
		
		MolapProperties.getInstance().addProperty("molap.storelocation", storepath);
		
//		SparkQueryExecutor exec = new SparkQueryExecutor("spark://master:7077","/opt/spark-1.0.0-rc3","/opt/ravi/PCC_Java.xml","PCC","ODM",storepathHDFS);
		SparkQueryExecutor exec = new SparkQueryExecutor(sparkURL,sparkDir,schemaPath,storepath);
		  
		MolapLoadModel model = new MolapLoadModel();
		model.setSchemaPath(schemaPath);
		model.setSchemaName(schemaName);
		model.setCubeName(cubeName);
		if(dimFolderPah !=null)
		{
		    model.setDimFolderPath(dimFolderPah);
		}
		
		model.setFactFilePath(factPath);
		model.setTableName(factTable);
		
		exec.initLoader(model, null,null, storepath);
	}*/
	
	 /**
     * 
     * Read the properties from CSVFilePartitioner.properties
     */
   /* private static Properties loadProperties()
    {
        Properties properties = new Properties();

        File file = new File("DataLoader.properties");
        FileInputStream fis = null;
        try
        {
            if(file.exists())
            {
                fis = new FileInputStream(file);

                properties.load(fis);
            }
            else
            {
//               System.out.println("DataLoader.properties file not found @" + file.getAbsolutePath());
            	LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"DataLoader.properties file not found @" + file.getAbsolutePath());
            }
            
        }
        catch(Exception e)
        {
//            e.printStackTrace();
        	   LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "error in reading DataLoader.properties file", e, e.getMessage());
        }
        finally
        {
            if(null != fis)
            {
                try
                {
                    fis.close();
                }
                catch(IOException e)
                {
//                    e.printStackTrace();
                	   LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "error while closing stream", e, e.getMessage());
                }
            }
        }
        
        return properties;

    }*/

}
