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

package com.huawei.unibi.molap.engine.mondrian.extensions;

public class InMemoryOlapConnection //extends MolapConnection
{

//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(InMemoryOlapConnection.class.getName());
//
//    public InMemoryOlapConnection(MondrianServer server, PropertyList connectInfo, DataSource dataSource)
//    {
//        super(server, connectInfo, dataSource);
//    }
//
//    public InMemoryOlapConnection(MondrianServer server, Util.PropertyList connectInfo, RolapSchema schema,
//            DataSource dataSource)
//    {
//        super(server, connectInfo, schema, dataSource);
//    }
//
//    protected DataSource createMolapDatasource(Util.PropertyList connectInfo)
//    {
//        return new MolapDataSourceImpl(connectInfo);
//
//    }
//
//    protected void queryStart(String schema, String cubeName, long queryId)
//    {
//        /**
//         * While restructuring of data is in progess, the query should be
//         * waiting. But for restructuring of schema case, the query will be
//         * blocked. By Sojer z00218041 2012-8-9
//         */
//        String cubeUniqueName= schema+'_'+cubeName;
//        try
//        {
//            while(InMemoryCubeStore.getInstance().isQueryWaiting(cubeUniqueName)
//                    || InMemoryCubeStore.getInstance().isQueryBlock(cubeUniqueName))
//            {
//                if(InMemoryCubeStore.getInstance().isQueryBlock(cubeUniqueName))
//                {
//                    throw Util.newInternal("Restructuring of schema is in progess!");
//                }
//
//                Thread.sleep(10);
//
//            }
//        }
//        catch(InterruptedException e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//        super.queryStart(schema, cubeName, queryId);
//        QueryMapper.queryStart(cubeUniqueName, queryId);
//    }
//
//    protected void queryEnd(String schema, String cubeName, long queryId)
//    {
//        String cubeUniqueName= schema+'_'+cubeName;
//        super.queryEnd(schema, cubeName, queryId);
//        QueryMapper.queryEnd(cubeUniqueName, queryId);
//    }
//
//    /**
//     * @author N71324
//     * 
//     */
//    private class CallableExecution implements Callable<Result>
//    {
//        /**
//         * 
//         */
//        private Execution execution = null;
//
//        /**
//         * 
//         */
//        private String schemaName;
//
//        /**
//         * 
//         */
//        private String cubeName;
//        
//        /**
//         * 
//         */
//        private String cubeUniqueName;
//
//        /**
//         * 
//         */
//        private long queryId;
//
//        /**
//         * 
//         */
//        private long parentThreadId;
//
//        protected CallableExecution(Execution execution, String schemaName, String cubeName, long queryId)
//        {
//            super();
//            this.parentThreadId = Thread.currentThread().getId();
//            this.execution = execution;
//            this.cubeName = cubeName;
//            this.queryId = queryId;
//            this.schemaName = schemaName;
//            this.cubeUniqueName=schemaName+'_'+cubeName;
//        }
//
//        @Override
//        public Result call() throws Exception
//        {
//            RolapConnection.THREAD_LOCAL.get().put(RolapConnection.QUERY_ID, "" + queryId);
//            InMemoryOlapConnection.super.queryStart(schemaName, cubeName, queryId);
//            
//            QueryMapper.queryStart(cubeUniqueName, queryId, parentThreadId);
//            try
//            {
//                return executeInternal(execution);
//            }
//            finally
//            {
//                QueryMapper.queryEnd(cubeUniqueName, queryId, false);
//                InMemoryOlapConnection.super.queryEnd(schemaName, cubeName, queryId);
//            }
//
//        }
//    }
//
//    /**
//     * Added to provide additional behavior for the separate thread to have the
//     * slice information which thread was having to execute for the specific
//     * cube.
//     * 
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.MolapConnection#getcallableExecution(mondrian.server.Execution,
//     *      java.lang.String, long)
//     * 
//     */
//    protected Callable<Result> getcallableExecution(final Execution execution, String schemaName, String cubeName,
//            long queryId)
//    {
//        return new CallableExecution(execution, schemaName, cubeName, queryId);
//    }

}
