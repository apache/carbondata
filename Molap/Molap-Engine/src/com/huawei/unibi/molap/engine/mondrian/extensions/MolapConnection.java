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


//import org.apache.log4j.Logger;

/**
 * This is a generic Molap connection which is equivalent to RolapConnection but
 * is responsible for handling Molap datasource instead of ROLAP
 * 
 */
public class MolapConnection //extends RolapConnection
{
//    /**
//     * 
//     */
////    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapConnection.class.getName());
//
//    /**
//     * 
//     */
//    public static final ThreadLocal<HashMap<String, Object>> THREAD_LOCAL = new ThreadLocal<HashMap<String, Object>>()
//    {
//        protected HashMap<String, Object> initialValue()
//        {
//            return new HashMap<String, Object>();
//        };
//    };
//
//    /**
//     * 
//     */
//    public static final String QUERY_TIME = "QUERY_TIME";
//
//    public MolapConnection(MondrianServer server, PropertyList connectInfo, DataSource dataSource)
//    {
//        super(server, connectInfo, dataSource);
//    }
//
//    public MolapConnection(MondrianServer server, Util.PropertyList connectInfo, RolapSchema schema,
//            DataSource dataSource)
//    {
//        super(server, connectInfo, schema, dataSource);
//    }
//
////    @Override
////    protected void validateRolapCredentials(StringBuilder buf)
////    {
////        // TODO Need to ensure this method is called only in case of
////        // ROLAPConnection
////        // do nothing
////    }
//
//    /**
//     * Creates a JDBC data source from the JDBC credentials contained within a
//     * set of mondrian connection properties.
//     * 
//     * <p>
//     * This method is package-level so that it can be called from the
//     * RolapConnectionTest unit test.
//     * 
//     * @param dataSource
//     *            Anonymous data source from user, or null
//     * @param connectInfo
//     *            Mondrian connection properties
//     * @param buf
//     *            Into which method writes a description of the JDBC credentials
//     * @return Data source
//     */
//    protected DataSource createDataSource(DataSource dataSource, Util.PropertyList connectInfo, StringBuilder buf)
//    {
//        DataSource dsTobeReturned = null;
//        assert buf != null;
//        final String jdbcConnectString = connectInfo.get(RolapConnectionProperties.Jdbc.name());
//        final String jdbcUser = connectInfo.get(RolapConnectionProperties.JdbcUser.name());
//        final String jdbcPassword = connectInfo.get(RolapConnectionProperties.JdbcPassword.name());
//        //
//        if(dataSource != null)
//        {
//            appendKeyValue(buf, "Anonymous data source", dataSource);
//            appendKeyValue(buf, RolapConnectionProperties.JdbcUser.name(), jdbcUser);
//            appendKeyValue(buf, RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);
//            //
//            return dataSource;
//
//        }
//        else if(jdbcConnectString != null)
//        {
//            //
//            appendKeyValue(buf, RolapConnectionProperties.Jdbc.name(), jdbcConnectString);
//            appendKeyValue(buf, RolapConnectionProperties.JdbcUser.name(), jdbcUser);
//            appendKeyValue(buf, RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);
//            String jdbcDrivers = connectInfo.get(RolapConnectionProperties.JdbcDrivers.name());
//            if(jdbcDrivers != null)
//            {
//                //
//                DataSource molapDataSource = createMolapDatasource(connectInfo);
//                if(molapDataSource != null)
//                {
//                    dsTobeReturned = molapDataSource;
//                }
//                else
//                {
//                    throw Util.newInternal("Connect string '" + connectInfo.toString()
//                            + "' must contain either InMemory or Hbase Driver'");
//                }
//            }
//        }
//        else
//        {
//            throw Util.newInternal("Connect string '" + connectInfo.toString()
//                    + "' must contain either InMemory or Hbase Driver'");
//        }
//        return dsTobeReturned;
//    }
//
//    protected DataSource createMolapDatasource(Util.PropertyList connectInfo)
//    {
//        // / return new HbaseDataSource(connectInfo);
//        return null;
//
//    }
//
//    protected void queryStart(String schema, String cubeName, long queryId)
//    {
//        // Start logic
//        THREAD_LOCAL.get().put(QUERY_TIME, System.currentTimeMillis());
//        RolapConnection.THREAD_LOCAL.get().put(RolapConnection.SCHEMA_NAME, schema);
//        RolapConnection.THREAD_LOCAL.get().put(RolapConnection.CUBE_NAME, cubeName);
//    }
//
//    protected void queryEnd(String schema, String cubeName, long queryId)
//    {
//        RolapConnection.THREAD_LOCAL.get().remove(RolapConnection.SCHEMA_NAME);
//        RolapConnection.THREAD_LOCAL.get().remove(RolapConnection.CUBE_NAME);
//        RolapConnection.THREAD_LOCAL.get().remove(RolapConnection.QUERY_OBJ);
//    }
//
//    /*
//     * protected Result executeInternal(final Execution execution) { String
//     * cubeName = execution.getMondrianStatement().getQuery().getCube()
//     * .getName(); long queryId =
//     * execution.getMondrianStatement().getQuery().getId(); String schemaName =
//     * execution.getMondrianStatement().getQuery()
//     * .getCube().getSchema().getName(); queryStart(schemaName, cubeName,
//     * queryId);
//     * 
//     * try { return super.executeInternal(execution); } finally {
//     * queryEnd(schemaName, cubeName, queryId); } }
//     */
//
//    /**
//     * Executes a statement.
//     * 
//     * @param execution
//     *            Execution context (includes statement, query)
//     * 
//     * @throws ResourceLimitExceededException
//     *             if some resource limit specified in the property file was
//     *             exceeded
//     * @throws QueryCanceledException
//     *             if query was canceled during execution
//     * @throws QueryTimeoutException
//     *             if query exceeded timeout specified in the property file
//     */
//    public Result execute(final Execution execution)
//    {
//        String cubeName = execution.getMondrianStatement().getQuery().getCube().getName();
//        long queryId = execution.getMondrianStatement().getQuery().getId();
//        String schemaName = execution.getMondrianStatement().getQuery().getCube().getSchema().getName();
//        queryStart(schemaName, cubeName, queryId);
//        try
//        {
//            return RolapResultShepherd.shepherdExecution(execution,
//                    getcallableExecution(execution, schemaName, cubeName, queryId));
//        }
//        finally
//        {
//            queryEnd(schemaName, cubeName, queryId);
//        }
//    }
//
//    /**
//     * Any of these parameters are not required over here but in case of
//     * InMemory we need to set some more information, This method has been
//     * extended
//     * 
//     * @param execution
//     * @param cubeName
//     * @param queryId
//     * @return
//     * 
//     */
//    protected Callable<Result> getcallableExecution(final Execution execution, String schemaName, String cubeName,
//            long queryId)
//    {
//        return new Callable<Result>()
//        {
//            public Result call() throws Exception
//            {
//
//                return executeInternal(execution);
//            }
//        };
//    }
//    
//    protected void validateRolapCredentials(StringBuilder buf) 
//    {
//        //Nothin to do as we always bluff about molapconnection
//    }
}
