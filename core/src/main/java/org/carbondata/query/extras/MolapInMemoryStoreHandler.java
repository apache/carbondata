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

package org.carbondata.query.extras;

/**
 * A <code>MolapInMemoryStoreHandler</code> is a handler to InMemory store.
 * <p>
 * Typically, you use its startInitInMemoryAction method at startup of BI Server
 * by executing a action of Action Sequence defined at sessionStartupActions.xml
 * </p>
 *
 * @author Sojer z00218041
 * @since 27 August, 2012
 */
public class MolapInMemoryStoreHandler {
    //
    //    /**
    //     * LOGGER.
    //     */
    //    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapInMemoryStoreHandler.class.getName());
    //
    //    /**
    //     *
    //     */
    //    private static MolapInMemoryStoreHandler instance = new MolapInMemoryStoreHandler();
    //
    //    /**
    //     * @return
    //     */
    //    public static MolapInMemoryStoreHandler getInstance()
    //    {
    //        return instance;
    //    }
    //
    //    /**
    //     * The main method that action sequence will invoke
    //     */
    //    public void startInitInMemoryAction()
    //    {
    //        InitInMemoryThread initInMemoryThread = new InitInMemoryThread();
    //        initInMemoryThread.start();
    //    }
    //
    //    /**
    //     * A <code>InitInMemoryThread</code> is a internal Thread to load cubes to
    //     * InMemory store.
    //     *
    //     * @author Sojer z00218041
    //     * @since 27 August, 2012
    //     */
    //    private class InitInMemoryThread extends Thread
    //    {
    //        /**
    //         * start initialize task
    //         *
    //         * @see java.lang.Thread#run()
    //         */
    //        public void run()
    //        {
    //            IPentahoSession systemSession = PentahoSystem.get(IPentahoSession.class, "systemStartupSession", null);
    //            MondrianCatalogHelper mondrianCatalogHelper = (MondrianCatalogHelper)PentahoSystem.get(
    //                    IMondrianCatalogService.class, "IMondrianCatalogService", null);
    //            List<MondrianCatalog> catalogs = mondrianCatalogHelper.listCatalogs(systemSession, true);
    //
    //            // iterate the schema catalogs list
    //            if(catalogs.size() > 0)
    //            {
    //                IDatasourceService datasourceService = null;
    //                try
    //                {
    //                    datasourceService = PentahoSystem.getObjectFactory().get(IDatasourceService.class, null);
    //                }
    //                catch(ObjectFactoryException e)
    //                {
    //                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
    //                }
    //
    //                String dataSourceInfo;
    //                String[] datasourceStrTemp;
    //                // log info buffer
    //                StringBuffer logStr = new StringBuffer("Cubes in schemas [ ");
    //
    //                try
    //                {
    //                    for(MondrianCatalog catalog : catalogs)
    //                    {
    //                        dataSourceInfo = catalog.getDataSourceInfo();
    //                        if(dataSourceInfo != null)
    //                        {
    //                            // typically dataSourceInfo likes :
    //                            // Provider=mondrian;DataSource=InMemory
    //                            datasourceStrTemp = dataSourceInfo.split("DataSource=");
    //                            generateConProperties(datasourceService, datasourceStrTemp, logStr, catalog);
    //                        }
    //                    }
    //                }
    //                catch(DatasourceServiceException e)
    //                {
    //                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
    //                }
    //
    //                logStr.append("] have been added to InMemory!");
    //                LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, logStr.toString());
    //            }
    //        }
    //
    //        /**
    //         * @param datasourceService
    //         * @param datasourceStrTemp
    //         * @param logStr
    //         * @param catalog
    //         * @throws DatasourceServiceException
    //         */
    //        private void generateConProperties(IDatasourceService datasourceService, String[] datasourceStrTemp,
    //                StringBuffer logStr, MondrianCatalog catalog) throws DatasourceServiceException
    //        {
    //            DataSource dataSourceImpl;
    //            String dataSourceName;
    //            Connection conn;
    //            long ct = 0;
    //            if(null != datasourceService && datasourceStrTemp.length == 2)
    //            {
    //                dataSourceName = datasourceStrTemp[1];
    //                dataSourceImpl = datasourceService.getDataSource(dataSourceName);
    //                if(dataSourceImpl != null && "InMemoryDataSource".equals(dataSourceImpl.getClass().getSimpleName()))
    //                {
    //                    Util.PropertyList connectProperties = Util.parseConnectString(catalog.getDataSourceInfo());
    //                    connectProperties.put("Catalog", catalog.getDefinition());
    //                    connectProperties.put("Provider", "mondrian");
    //                    connectProperties.put("PoolNeeded", "false");
    //                    connectProperties.put(RolapConnectionProperties.Locale.name(), LocaleHelper.getLocale().toString());
    //                    connectProperties.remove(RolapConnectionProperties.DataSource.name());
    //                    // create MolapConnection, and actually
    //                    // add cubes to InMemory store here
    //
    //                    ct = System.currentTimeMillis();
    //                    LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Start loading @@@@@@@@@@@@@@@@@@@@@@@@@@:"
    //                            + ct);
    //
    //                    conn = DriverManager.getConnection(connectProperties, null, dataSourceImpl);
    //
    //                    LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, catalog.getName()
    //                            + " cost Mill secends to load ############: " + (System.currentTimeMillis() - ct));
    //
    //                    // add to log info
    //                    if(conn != null)
    //                    {
    //                        logStr.append(catalog.getName()).append(" ");
    //                    }
    //                }
    //            }
    //        }
    //    }

    // Changed by shiva

}
