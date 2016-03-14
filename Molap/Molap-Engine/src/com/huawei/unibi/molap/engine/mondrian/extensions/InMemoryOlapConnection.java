/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3SNVSjPbjAftoyy20Dzcz4GOrlZ2kN/bg4NZVoRrCQ4RvC3p3k8OWjlZjzximTI+LFOO
ZbZ96I907z6eUrOUVRsZKuCgGePFYX2BvVF2TDbI7RJMVJwE2AD8I/Ke2wbX3A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.mondrian.extensions;

//import org.apache.log4j.Logger;

/**
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :25-Jun-2013 FileName : InMemoryOlapConnection.java Class Description :
 * Version 1.0
 */
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
