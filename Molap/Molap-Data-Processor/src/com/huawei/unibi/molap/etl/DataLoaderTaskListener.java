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

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :01-Jul-2013
 * FileName : DataLoaderTaskListener.java
 * Class Description : 
 * Version 1.0
 */
public class DataLoaderTaskListener //implements IDataProcessTaskListener
{
//    /**
//     * instance of alarm service which used to raise alarm when task fails
//     */
//    private UniBIAlarmService alarmService = (UniBIAlarmService)PentahoSystem
//            .get(UniBIAlarmService.class, null);
//
//    /**
//     * 
//     * Comment for <code>LOGGER</code>
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(DataLoaderTaskListener.class.getName());
//    
//    /**
//     * instanace of DataLoaderServiceHelper
//     */
//    private IDataLoaderStatusService dataLoaderStatusService = PentahoSystem
//            .get(IDataLoaderStatusService.class);
//
//    /**
//     * if task is successful
//     * 
//     * @param taskId
//     * @param message
//     */
//    public void taskSuccessful(DataProcessTask dataProcessTask)
//    {
//        try
//        {
//            dataLoaderStatusService.updateStatus(dataProcessTask.getTaskId(),
//                    DataLoadStatus.SUCCESS.getType(), "");
//        }
//        catch(RepositoryException ex)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex.getMessage());
//        }
//        // if(futureTaskMap.containsKey(taskId))
//        // {
//        // futureTaskMap.remove(taskId);
//        // }
//        try
//        {
//            DataLoaderServiceHelper.getInstance().removeTaskFromMap(dataProcessTask.getTaskId());
//        }
//        catch(ObjectFactoryException e)
//        {
//            LOGGER.error(
//                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                    "Error while getting instance of DataLoaderServiceHelper"
//                            + e.getMessage());
//        }
//
//    }
//
//    /**
//     * if task is Failed
//     * 
//     * @param taskId
//     * @param message
//     */
//    public void taskFailed(DataProcessTask dataProcessTask, String message)
//    {
//        try
//        {
//            dataLoaderStatusService.updateStatus(dataProcessTask.getTaskId(),
//                    DataLoadStatus.FAILURE.getType(), message);
//        }
//        catch(RepositoryException ex)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex.getMessage());
//        }
//        // sendDataLoaderTaskFailedAlarm("","");
//        // if(futureTaskMap.containsKey(taskId))
//        // {
//        // futureTaskMap.remove(taskId);
//        // }
//        try
//        {
//            DataLoaderServiceHelper.getInstance().removeTaskFromMap(dataProcessTask.getTaskId());
//        }
//        catch(ObjectFactoryException e)
//        {
//            LOGGER.error(
//                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                    "Error while getting instance of DataLoaderServiceHelper"
//                            + e.getMessage());
//        }
//        // sends alarm for report generation failure.
//        sendDataLoaderTaskFailedAlarm(dataProcessTask.getTaskId() + '_' + message,
//                AlarmConstants.MESSGE_KEY_DATALOAD_TASK_FAILED);
//    }
//
//    /**
//     * @param message
//     * @param alarmMsgKey
//     */
//    private void sendDataLoaderTaskFailedAlarm(String message,
//            String alarmMsgKey)
//    {
//
//        UniBIInternalAlarmEvent unibiAlarmEvent = new UniBIInternalAlarmEvent(
//                AlarmConstants.ALARM_ID_DATALOAD_TASK_FAILED, alarmMsgKey);
//        unibiAlarmEvent.setRectifiable(false);
//        //
//        try
//        {
//            alarmService.sendAlarm(unibiAlarmEvent);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
//					"Error while Sending alarm from sendDataLoaderTaskFailedAlarm"
//                    + e.getMessage());
//        }
//        //
//        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
//					AlarmConstants.ALARM_BROADCASTED_MSG + "Alarm ["
//                + AlarmConstants.ALARM_ID_DATALOAD_TASK_FAILED + ','
//                + message + ",INFO,Data Loader Task failure] is raised");
//    }

}
