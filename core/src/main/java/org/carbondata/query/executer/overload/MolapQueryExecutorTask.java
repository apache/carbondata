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

package org.carbondata.query.executer.overload;

/**
 * This is overload control thread task
 */
public class MolapQueryExecutorTask //extends AbstractUniBIInterruptableTask
{

    //    /**
    //     * MOLAP_QUERY_EXECUTOR
    //     */
    //    public static final String MOLAP_QUERY_EXECUTOR = "MOLAP_QUERY_EXECUTOR";
    //
    //    private MolapExecutor executor;
    //
    //    private MolapQueryExecutorModel executorModel;
    //
    //
    //    public MolapQueryExecutorTask(MolapExecutor executor,MolapQueryExecutorModel executorModel)
    //    {
    //        this.executor = executor;
    //        this.executorModel = executorModel;
    //    }
    //
    //
    //    @Override
    //    public String getTaskDescription()
    //    {
    //        return "Molap query execution";
    //    }
    //
    //    @Override
    //    public Object call() throws Exception
    //    {
    //        executor.execute(executorModel);
    //        this.executor = null;
    //        this.executorModel = null;
    //        return null;
    //    }
    //
    //    @Override
    //    public String getTaskType()
    //    {
    //        return MOLAP_QUERY_EXECUTOR;
    //    }
    //
    //    @Override
    //    public void interrupt()
    //    {
    //        if(executor != null)
    //        {
    //            executor.interruptExecutor();
    //        }
    //    }

}
