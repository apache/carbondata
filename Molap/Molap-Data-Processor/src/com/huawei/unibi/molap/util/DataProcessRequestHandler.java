/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbbIz5SuiZ8edolM+jg9rPCmuC+wUQbmLljTfAFiqvOMDhQQsSs1rJ/uzlTr3NncbV3Ot
n4FXswPXmZJWPJJ/DDECNmLiPknCTm6hmd/5/zgpYBYAuOBmurGkb5U+sd6Vgg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.util;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :r70299
 * Created Date :Sep 13, 2013
 * FileName : DataProcessRequestHandler.java
 * Class Description : 
 * Version 1.0
 */
public final class DataProcessRequestHandler {

//    private static ArrayBlockingQueue<DataProcessTask> dataProcessTaskQ = new ArrayBlockingQueue<DataProcessTask>(
//            100);
//
//    private static LoadController loadController = null;
//
//    private static volatile boolean threadInterrupted = false;
//    
//    private static volatile boolean initialized = false;
//
//    private static Thread t = null;
//
////    private static volatile DataProcessRequestHandler dataProcessRequestHandler = new DataProcessRequestHandler();
//
//    /**
//     * 
//     * Comment for <code>LOGGER</code>
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(DataProcessRequestHandler.class.getName());
//
//    /**
//     * dataload restructure service
//     */
//    private static ILoaderStatusService dataLoaderStatusService;
//    
//    private DataProcessRequestHandler()
//    {
//        dataLoaderStatusService = PentahoSystem.get(ILoaderStatusService.class);
//    }
//    
//
//    /**
//     * getInstance
//     * @return DataProcessRequestHandler
//     */
//    public static DataProcessRequestHandler getInstance()
//    {
//        synchronized (DataProcessRequestHandler.class)
//        {
//            if (!initialized)
//            {
//                MolapProperties properties = MolapProperties.getInstance();
//                int queueSize = Integer.parseInt(properties.getProperty(
//                        "molap.dataload.queuesize", "100"));
//                int concurExecsSize = Integer.parseInt(properties.getProperty(
//                        "molap.dataload.concurrent.execution.size", "1"));
//                try
//                {
//                    loadController = LoadController.getInstance();
//                    // Register Dataloader task with Load controller
//                    loadController.registerTaskType(queueSize, concurExecsSize,
//                            DataProcessorConstants.DATARESTRUCT);
//                }
//                catch (LoadControlException ex)
//                {
//                    LOGGER.error(
//                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                            ex, ex.getMessage());
//                }
//
//                initialized = true;
//                t = new Thread(new DataProcessRequestHandlerThread());
//                t.start();
//            }
//        }
//
//        return SingletonHolder.dataProcessRequestHandler;
//    }
//    
//    private static class SingletonHolder
//    {
//        private static volatile DataProcessRequestHandler dataProcessRequestHandler = new DataProcessRequestHandler();
//    }
//
//    /**
//     * submit
//     * @param dataProcessTask
//     * @return boolean
//     */
//    public boolean submit(DataProcessTask dataProcessTask)
//    {
//        if(null != t && !threadInterrupted && null != dataProcessTaskQ)
//        {
//            return dataProcessTaskQ.offer(dataProcessTask);
//        }
//        return false;
//    }
//
//    private static class DataProcessRequestHandlerThread implements Runnable
//    {
//
//        @Override
//        public void run()
//        {
////CHECKSTYLE:OFF    Approval No:Approval-393
//            do//CHECKSTYLE:ON
//            {
//                DataProcessTask task = null;//CHECKSTYLE:OFF    Approval No:Approval-372
//                try
//                {//CHECKSTYLE:ON
//                    task = dataProcessTaskQ.take();
//                    if(checkIfEligibleToExecute(task))
//                    {
//                        loadController.submit(task);
//                    }
//                    else
//                    {
//                        dataProcessTaskQ.put(task);
//                    }
//                    Thread.sleep(5000);
//                }
//                catch(InterruptedException e)
//                {
//                    threadInterrupted = true;
//                    cleanup();
//                }
//                catch(LoadControlException ex)
//                {
//                    LOGGER.error(
//                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                            ex, "Unable to submit to load controller");
//                }
//            }
//            while(!threadInterrupted);
//
//        }
//
//        private boolean checkIfEligibleToExecute(DataProcessTask task)
//        {
//            String targetSchemaName = task.getTargetSchemaName();
//            String targetCubeName = task.getTargetCubeName();
//            return dataLoaderStatusService.isDataLoadInProgressOrWait(
//                    targetSchemaName, targetCubeName);
//        }
//
//        private void cleanup()
//        {
//
//            dataProcessTaskQ = null;
//        }
//    }
}

