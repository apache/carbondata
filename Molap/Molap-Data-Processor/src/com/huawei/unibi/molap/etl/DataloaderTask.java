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
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : DataloaderTask.java
 * Class Description : 
 * Version 1.0
 */
public class DataloaderTask // extends DataProcessTask{
{
//    /**
//     * 
//     */
//    private String schmeaName;
//    
//    /**
//     * 
//     */
//    private String cubeName;
//    
//    /**
//     * 
//     */
//    private String tableName;
//    
//    public DataloaderTask(String taskId, IDataProcessStatus dataProcessStatus)
//    {
//        super(taskId, dataProcessStatus);
//    }
//
//    public DataloaderTask(String schmeaName, String cubeName, String tableName)
//    {
//        this.schmeaName = schmeaName;
//        this.cubeName= cubeName;
//        this.tableName=tableName;
//        
//    }
//
//    /**
//     * Holds background listener.
//     */
//	private List<DataLoaderTaskListener> listeners = new ArrayList<DataLoaderTaskListener>(10);
//	
//	/**
//	 * LOGGER 
//	 */
//	private static final LogService LOGGER = LogServiceFactory.getLogService(DataloaderTask.class.getName());
//
//    /**
//     * instance of IDataLoaderStatusService
//     */
//	private IDataLoaderStatusService dataLoaderStatusService;
//	 
//    /**
//     * taks id or Key
//     */
//	private String taskId = null;
//	
//    /**
//     * path of the graph file
//     */
//	private String graphFilePath = null;
//	
//	
//	/**
//	 *  graph executer instance
//	 */
//    private  ETLGraphExecuter graphExecuter = new ETLGraphExecuter();
//    /**
//     * 
//     * @return path of the graph file
//     */
//	public String getGraphFilePath() {
//		return graphFilePath;
//	}
//
//    /**
//     * setGraphFilePath
//     * 
//     * @param graphFilePath
//     */
//	public void setGraphFilePath(String graphFilePath) {
//		this.graphFilePath = graphFilePath;
//	}
//
//    /**
//     * 
//     * @return List<DataLoaderTaskListener>
//     */
//	public List<DataLoaderTaskListener> getListeners() {
//		return listeners;
//	}
//
//    /**
//     * 
//     * @return key
//     */
//	public String getTaskId() {
//		return taskId;
//	}
//
//    /**
//     * set key
//     * 
//     * @param taskId
//     */
//	public void setTaskId(String taskId) {
//		this.taskId = taskId;
//	}
//
//    /**
//     * @see com.huawei.unibi.loadcontrol.UniBIInterruptableTask#getTaskDescription()
//     */
//	@Override
//	public String getTaskDescription() {
//        return taskId;
//	}
//
//
//    /**
//     * @see java.util.concurrent.Callable#call()
//     */
//	@Override
//    public Object call() throws Exception
//    {
//		try
//		{
//			execute();
//		}
//		finally
//		{
//			//null referencing because overload not leaving reference
//			graphExecuter = null;
//			dataLoaderStatusService = null;
//		}
//        return null;
//    }
//
//    /**
//     * @see com.huawei.unibi.loadcontrol.task.AbstractUniBIInterruptableTask#getTaskType()
//     */
//	@Override
//	public String getTaskType() {
//		return "DATALOADER";
//	}
//
//    /**
//     * @see com.huawei.unibi.loadcontrol.task.AbstractUniBIInterruptableTask#interrupt()
//     */
//	@Override
//	public void interrupt() {
//        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask interrupted ");
//        graphExecuter.interruptGraphExecution();
//        fireTaskFailedEvent("INTERNAL_FAILURE: DATALOADING PROCESS INTRRUPTED");
//	}
//	
//    /**
//	 * 
//	 */
//	private void execute()
//	{
//		LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask execute started");
//        //
//        try
//        {
//            dataLoaderStatusService = PentahoSystem
//                    .get(IDataLoaderStatusService.class);
//            //
//            dataLoaderStatusService.updateStatus(taskId,
//                    DataLoadStatus.INPROGRESS.getType(), "");
//            LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "status is Null so adding status to DB as WAITING");
//        }
//        catch(RepositoryException ex)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex, ex.getMessage());
//            fireTaskFailedEvent("INTERNAL_FAILURE: DATALOADING PROCESS FAILURE");
//        }
//        //
//		try {
//			
//			graphExecuter.executeGraph(graphFilePath);
//			
//		} catch (DataLoadingException e) {
//            //
//			LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask execute : "+e.getMessage());
//			fireTaskFailedEvent("INTERNAL_FAILURE: DATALOADING PROCESS FAILURE");
//			return;
//		}
//		fireTaskSuccessEvent("");
//		try
//        {
//		    String isBackgroundMergingType = MolapProperties.getInstance()
//	                        .getProperty(
//	                                MolapCommonConstants.BACKGROUND_MERGER_TYPE);
//            if(!MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE
//                    .equalsIgnoreCase(isBackgroundMergingType)
//                    && !MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE
//                            .equalsIgnoreCase(isBackgroundMergingType))
//		    {
//                OfflineMerger merger = new OfflineMerger(this.schmeaName,
//                        this.cubeName, this.tableName);
//                merger.startMerge();
//		    }
//        }
//        catch (SliceMergerException e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Offline merging failed for : "
//                    + this.schmeaName + ' ' + this.cubeName + ' ' + this.tableName + e.getMessage());
//        }
//		LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask execute end");
//	}
//	
//	/**
//	 * Adds background listener.
//	 * 
//	 * @param listener  background listener.
//	 */
//	public void addListener(DataLoaderTaskListener listener)
//	{
//		listeners.add(listener);		
//	}
//	
//	private void fireTaskSuccessEvent(String message)
//	{
//		LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask fireTaskSuccessEvent "+message);
//		for(DataLoaderTaskListener listener : listeners)
//		{
//			listener.taskSuccessful(this);
//		}
//	}
//	
//	private void fireTaskFailedEvent(String message)
//	{
//		LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "DataloaderTask fireTaskFailedEvent "+message);
//		for(DataLoaderTaskListener listener : listeners)
//		{
//			listener.taskFailed(this,message);
//		}
//	}

	

}
