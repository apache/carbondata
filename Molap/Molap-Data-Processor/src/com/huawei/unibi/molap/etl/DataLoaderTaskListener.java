/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnAFEaiqGK6TaF7xcTTdnPiMzeQLyCcAhoNE2+9QmSq0xfubXqFLaypCsh9+8VK7DxQn1
fK3sphOXfLX/VSXtBa63/j/v5d70W2Fka34eK32GRlMuHEx3S0ocxIGjFXJRSA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
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
