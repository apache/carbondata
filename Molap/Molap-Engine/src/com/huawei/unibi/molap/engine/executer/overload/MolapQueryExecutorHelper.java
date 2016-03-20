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

package com.huawei.unibi.molap.engine.executer.overload;


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
