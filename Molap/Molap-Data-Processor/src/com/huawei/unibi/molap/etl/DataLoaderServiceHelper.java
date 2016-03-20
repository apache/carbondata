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


package com.huawei.unibi.molap.etl;


/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author V00900840
 * Created Date :21-May-2013 9:15:15 PM
 * FileName : DataLoaderServiceHelper.java
 * Class Description : Helper class for the data loading APIs
 * Version 1.0
 */
public final class DataLoaderServiceHelper //implements IDataLoader
{
//    
//    /**
//     * 
//     */
//    public static final String GENRATE_RESPONSE_MSG_TEMPLATE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+
//			"<Response status='%s' message='%s' />";
//    /**
//     * 
//     * Comment for <code>LOGGER</code>
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(DataLoaderServiceHelper.class.getName());
//    
//    /**
//     * load controller for task pool
//     */
//    private static LoadController loadcontroller;
//    
//
//    /**
//     * Data Loader Service instance
//     */
////    private static volatile DataLoaderServiceHelper instance; 
//    
//    /**
//	 * Holds all the references for background tasks.
//	 */
//	private Map<String, FutureWrapper> futureTaskMap = new ConcurrentHashMap<String, FutureWrapper>(10);
//
//
//    /**
//     * instanace of DataLoaderServiceHelper
//     */
//	private IDataLoaderStatusService dataLoaderStatusService;
//
//    /**
//     * for making singleton object
//     */
//    private DataLoaderServiceHelper()
//    {
//        dataLoaderStatusService = PentahoSystem
//                .get(IDataLoaderStatusService.class);
//        
//        try 
//        {
//            loadcontroller = LoadController.getInstance();
//            //Register Dataloader task with Load controller
//            loadcontroller.registerTaskType(100,
//                    5,
//                    "DATALOADER");  
//        } 
//        catch (LoadControlException ex) 
//        {
//            //
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex, ex.getMessage());
//        }
//    }
//
//    /**
//     * 
//     * @see org.pentaho.platform.engine.core.system.PentahoBase#getLogger()
//     * 
//     */
//    
//
//    public String handleStartDataLoading(SchemaInfo schemaInfo,
//            String cubeName, String tableName, String graphFilePath)
//    {
//        String returnMsg =null;
//        // Here schemaInfo will not be null
//        String schemaName = schemaInfo.getSchemaName();
//        // create key
//        String key = schemaName.trim() +'_'+ cubeName.trim()+'_'+ tableName.trim();        
//
//        DataLoaderStatus status = new DataLoaderStatus(schemaName, cubeName, tableName);
//        IDataLoaderStatus existingStatus = null;
//
//
//        if(checkGraphExist(schemaName, cubeName, tableName))
//        {
//            boolean deleted = deleteGraph(schemaName,cubeName,tableName);
//            
//            if(!deleted)
//            {
//                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Unabel to delete graph File");
//                return "INTERNAL SYSTEM ERROR";
//            }
//        }
//    
//            String failureMsg = generateGraph(schemaInfo, tableName);
//
//            if(failureMsg != null)
//            {
//                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Unabel to generate graph File");
//                return "INTERNAL SYSTEM ERROR";
//            }
//      
//        //TODO ::to have the mechanism to avoid locking the parallel threads for diff. key.
//        // synch with "this" will block all the threads irrespective of keys.
//       synchronized(this)
//       {
//	        try{
//	        	existingStatus = dataLoaderStatusService.checkStatus(key);
//	        } catch(RepositoryException ex)
//	        {
//                LOGGER.error(
//                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                        "Unable to get data load status from DB "
//                                + ex.getMessage()); 
//	        	
//                returnMsg = "INTERNAL SYSTEM ERROR";
//	        	
//	        	return returnMsg;
//	        }
//	        
//	        if(null != existingStatus && DataLoadStatus.INPROGRESS.getType().equals(existingStatus.getStatus()))
//	        {
//                LOGGER.info(
//                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                        "Unable to submit the request. Dataloading is already in-progress for requested table "
//                                + key);
//	        	 
//	        	returnMsg = "Unable to submit the request. Dataloading is already in-progress for requested table.";
//	        	
//	        	return returnMsg;
//	        }
//	        
//            if(null != existingStatus && DataLoadStatus.WAITING.getType().equals(existingStatus.getStatus()))
//            {
//                LOGGER.info(
//                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                        "Unable to submit the request. Dataloading is already in Waiting for requested table"
//                                + key);
//
//                returnMsg = "Unable to submit the request. Dataloading is already in Waiting for requested table";
//
//                return returnMsg;
//            }
//	        
//	        //Update the DB before us submit the task, in case of failure of db op do not submit task.
//            try
//            {
//                if(null == existingStatus || null == existingStatus.getStatus())
//                {
//
//                    status.setStatus(DataLoadStatus.WAITING.getType());
//                    status.setTaskType(DataLoadTaskType.DATALOAD.getType());
//                    dataLoaderStatusService.addStatus(key, status);
//                    LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "status is Null so adding status to DB as WAITING");
//                }
//
//            }
//            catch(RepositoryException ex)
//            {
//                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex, ex.getMessage());
//                returnMsg = "INTERNAL SYSTEM ERROR";
//                return returnMsg;
//            }
//       }
//        
//        DataloaderTask task = new DataloaderTask(schemaName, cubeName,tableName);
//        task.setTaskId(key);
//        task.setGraphFilePath(graphFilePath);
//        task.setTargetSchemaName(schemaInfo.getSchemaName());
//        task.setTargetCubeName(cubeName);
//        task.addListener(new DataLoaderTaskListener());
//        
//        //TODO ::to have the mechanism to avoid Blocking the parallel threads for diff. keys.
//        // synch with this will block all the threads irrespective of keys. 
//       synchronized(this)
//       {
//            
//              //ONE more check 
//           if(futureTaskMap.containsKey(key)) 
//           {
//              LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Dataloading is already in-progress for requested table");
//              returnMsg ="Dataloading is already in-progress for requested table";
//              
//              //Need to update the status in DB as Failed
//              //DTS2013090508399-start             
//              dataLoaderStatusService.updateStatus(key, DataLoadStatus.FAILURE.getType(), returnMsg);
//              //DTS2013090508399 -end
//              return returnMsg; 
//           }
//             
//		FutureWrapper wrapper=null;
//            try
//            {
//                wrapper = loadcontroller.submit(task);
//                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Data loader task is submitted for "
//                        + graphFilePath);
//            }
//            catch(LoadControlException ex)
//            {
//                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex.getMessage());
//                dataLoaderStatusService.updateStatus(key,
//                        DataLoadStatus.FAILURE.getType(), "INTERNAL_ERROR");
//                returnMsg = "INTERNAL SYSTEM ERROR";
//                return returnMsg;
//            }
//		futureTaskMap.put(key, wrapper);
//       }		
//		return  null;
//    }
//
//    /**
//     * This method delete the graph File before starting data load
//     * @param schemaName
//     * @param cubeName
//     * @param tableName
//     * @return
//     * 
//     */
//    private boolean deleteGraph(String schemaName, String cubeName,
//            String tableName)
//    {
//        String graphPath = PentahoSystem.getApplicationContext()
//                .getSolutionPath("")
//                + File.separator
//                + "system"
//                + File.separator
//                + "molap"
//                + File.separator
//                + "etl"
//                + File.separator
//                + schemaName
//                + File.separator
//                + cubeName
//                + File.separator
//                + tableName + ".ktr";
//        File graphFile = new File(graphPath);
//        return graphFile.delete();
//    }
//
//    /**
//     * This method checks the graph existence.
//     * 
//     * @param schemaName
//     * @param cubeName
//     * @param tableName
//     * @return - true if graph exists, false otherwise.
//     * 
//     */
//    private boolean checkGraphExist(String schemaName, String cubeName,
//            String tableName)
//    {
//
//        String graphPath = PentahoSystem.getApplicationContext()
//                .getSolutionPath("")
//                + File.separator
//                + "system"
//                + File.separator
//                + "molap"
//                + File.separator
//                + "etl"
//                + File.separator
//                + schemaName
//                + File.separator
//                + cubeName
//                + File.separator
//                + tableName + ".ktr";
//        File graphFile = new File(graphPath);
//        return graphFile.exists();
//    }
//    /**
//     * implementaion for the handleDataLoadingStatus.
//     * 
//     * @param schemaName
//     * @param cubeName
//     * @param tableName
//     * @return status string
//     */
//    public String handleDataLoadingStatus(String schemaName, String cubeName, String tableName)
//    {
//    	IDataLoaderStatus status  = null;
//        String statusMsg = null;
//        // String desc = "";
//        // create the key
//        String key = schemaName.trim() +'_'+ cubeName.trim()+'_'+ tableName.trim();
//        String message = "";
//        try{
//        	status = dataLoaderStatusService.checkStatus(key);
//        	if( null != status)
//        	{
//        		if(status.getDesc()!=null && status.getStatus()!=null)
//        		{
//        			statusMsg = status.getStatus()+"@@"+status.getDesc();
//        		}
//        		else
//        		{
//        			statusMsg = status.getStatus();
//        		}
//                // desc = status.getDesc();
//        	}/*else
//        	{
//                message = "Status does not exist.";
//        		return message;
//        	}*/
//        }catch (RepositoryException ex)
//        {
//        	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex.getMessage());
//            message = "INTERNAL SYSTEM ERROR";
//        	dataLoaderStatusService.updateStatus(key,DataLoadStatus.FAILURE.getType(),"INTERNAL_ERROR");
//        	return message;
//        }
//        return statusMsg;
//    }
//    
//
//    
//    /**
//     * Return the instance of the Data loader helper.
//     * 
//     * @return instance
//     * @throws ObjectFactoryException
//     *
//     */
//    public static DataLoaderServiceHelper getInstance()
//            throws ObjectFactoryException
//    {
////        if(null == instance)
////        {
////            synchronized(DataLoaderServiceHelper.class)
////            {
////                if(null == instance)
////                {
////                    //
////                    instance = new DataLoaderServiceHelper();
////                    try 
////            		{
////                        loadcontroller = LoadController.getInstance();
////            		    //Register Dataloader task with Load controller
////                    	loadcontroller.registerTaskType(100,
////            					5,
////            					"DATALOADER");	
////            		} 
////            		catch (LoadControlException ex) 
////                    {
////                        //
////            			LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex, ex.getMessage());
////            		}
////                }
////            }
////        }
//        return SingletonHolder.instance;
//    }
//    
//    private static class SingletonHolder
//    {
//        private static volatile DataLoaderServiceHelper instance = new DataLoaderServiceHelper();
//    }
//
//
//   
//    
//    /**
//     * @param tableName
//     * @see org.pentaho.platform.api.dataloader.IDataLoader#generateGraph(org.pentaho.platform.api.dataloader.SchemaInfo)
//     */
//    public String generateGraph(SchemaInfo info, String tableName)
//    {
//    	String failureMsg = null;
//        DataLoadModel model = new DataLoadModel();
//        model.setCsvLoad(false);
//        model.setSchemaInfo(info);
//        model.setTableName(tableName);
//        GraphGenerator generator = new GraphGenerator(model, false, null, null);
//    	try{
//    		generator.generateGraph();
//    	}catch(GraphGeneratorException ex)
//    	{
//            LOGGER.error(
//                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                    "INTERNAL ERROR: Unable to generate the graph"
//                            + ex.getMessage());
//    		failureMsg = "INTERNAL ERROR";
//    	}
//    	return failureMsg;
//    }
//
//    public void removeTaskFromMap(String taskId)
//    {
//        if(futureTaskMap.containsKey(taskId))
//        {
//            futureTaskMap.remove(taskId);
//        }
//    }
    
    

	}
