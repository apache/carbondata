/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3dwEzWVvJe3Ge1Y2xYe0YHOWHFFTWw58u+6lR7pw+LsUPQZI/tKojrRWte3+a4JNR7dec
crRaaDPnbsArSjxZ00Kq7vFY3x/Nf9OdQzOFbVjobZX+mVk7wtvrnHmSLasMUA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.overload;


/**
 * @author R00900208
 *
 */
public final class MolapQueryExecutorHelper
{
    
//    /**
//     * load controller for task pool
//     */
//    private LoadController loadcontroller;
//    
//    private static MolapQueryExecutorHelper executorHelper;
//    
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapQueryExecutorHelper.class.getName());
//    
//    /**
//     * private constructor
//     */
//    private MolapQueryExecutorHelper()
//    {
//        loadcontroller = LoadController.getInstance();
//        
//        MolapProperties properties = MolapProperties.getInstance();
//        int queueSize = Integer.parseInt(properties.getProperty("molap.queryexecutor.queuesize", "200"));
//        int concurExecsSize = Integer.parseInt(properties.getProperty("molap.queryexecutor.concurrent.execution.size", "3"));
//        //Register task with Load controller
//        try
//        {
//            loadcontroller.registerTaskType(queueSize,
//                    concurExecsSize,
//                    MolapQueryExecutorTask.MOLAP_QUERY_EXECUTOR);
//        }
//        catch (LoadControlException ex) 
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, ex, ex.getMessage());
//        }
//    }
//    
//    /**
//     * Singleton instance getting
//     * @return MolapQueryExecutorHelper
//     */
//    public synchronized static MolapQueryExecutorHelper getInstance()
//    {
//        if(executorHelper == null)
//        {
//            executorHelper = new MolapQueryExecutorHelper();
//        }
//        
//        return executorHelper;
//    }
//    
//    /**
//     * Execute the task with overload control.
//     * @param executorTask
//     * @throws LoadControlException
//     * @throws ExecutionException 
//     * @throws InterruptedException 
//     */
//    public void executeQueryTask(MolapQueryExecutorTask executorTask) throws LoadControlException, InterruptedException, ExecutionException
//    {
//        FutureWrapper future = loadcontroller.submit(executorTask);
//        future.get();
//    }
    
    

}
