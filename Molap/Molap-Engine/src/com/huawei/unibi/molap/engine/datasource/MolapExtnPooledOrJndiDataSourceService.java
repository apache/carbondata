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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBmyLV0O/ANGbITXbOAqcEJrzjjP3pXfiDc5+P7uaNgW1D3cQOTq3Vi8/3pqOEI5QwW08
AqJM5rsdaBH8FHUfyU5gXtGApA3my1Kwn7ix0DmzPOzgOgCGwUBXFO5mhF1Xog==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.datasource;


/**
 * Project Name NSE V3R7C00
 * 
 * Module Name : Molap Engine
 * 
 * Author K00900841
 * 
 * Created Date :13-May-2013 3:35:33 PM
 * 
 * FileName : MolapExtnPooledOrJndiDataSourceService.java
 * 
 * Class Description : MolapExtnPooledOrJndiDataSourceService class will be used
 * for getting the molap data source
 * 
 * Version 1.0
 */
public class MolapExtnPooledOrJndiDataSourceService //extends PooledOrJndiDatasourceService
{

//    /**
//     * Data source cache it will be used if data source is already present then
//     * it will return from this cache
//     */
//    private static Map<String, DataSource> pool = new HashMap<String, DataSource>();
//    
//    
//    /**
//     * Attribute for Molap LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapExtnPooledOrJndiDataSourceService.class.getName());
//    
//    /**
//     * 
//     * Overriden method for getting the molap data source it will check whether
//     * its a molap data source or not, if it molap data source then it will
//     * return molap data source otherwise it will delegate to super class to get
//     * the data source
//     * 
//     * @param datasource 
//     *          data source name
//     * @return data source
//     * @throws DatasourceServiceException
//     *          will throw data source service exception
//     * 
//     */
//    public DataSource getDataSource(String datasource) throws DatasourceServiceException
//    {
//        try
//        {
//            IDatasourceMgmtService datasourceMgmtSvc = (IDatasourceMgmtService)PentahoSystem.getObjectFactory().get(
//                    IDatasourceMgmtService.class, null);
//            IDatasource dataSource = datasourceMgmtSvc.getDatasource(datasource);
//
//            // If it is MOLAP data source, handle it otherwise delegate to super
//            // class
//            if(isMolapDataSource(dataSource))
//            {
//                return getMolapDataSource(dataSource);
//            }
//        }
//        catch(Exception objface)
//        {
//            LOGGER.error(
//                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,objface,objface.getMessage());
//        }
//
//        return super.getDataSource(datasource);
//    }
//
//
//    /**
//     * This method return the molap data source, if data source is already
//     * present in cache then it will return it from cache other wise it will add
//     * to cache and return
//     * 
//     * @param dataSource
//     *            IDatasource
//     * @return molap datasource
//     * 
//     * 
//     */
//    private DataSource getMolapDataSource(IDatasource dataSource)
//    {
//        
//        Util.PropertyList connectInfo = new Util.PropertyList();
//        // create the connection info 
//        connectInfo.put(RolapConnectionProperties.JdbcDrivers.name(), dataSource.getDriverClass());
//        connectInfo.put(RolapConnectionProperties.JdbcUser.name(), dataSource.getUserName());
//        connectInfo.put(RolapConnectionProperties.JdbcPassword.name(), dataSource.getPassword());
//
//        connectInfo.put(RolapConnectionProperties.Jdbc.name(), dataSource.getUrl());
//
//        // TODO temporary pooling . Find better way to solve data source pooling
//        // issue.
//        // Problem: If the data source object is new, schema connection will be
//        // treated as new for every request. So, cache will not be reused.
//        StringBuilder keyString = new StringBuilder(50);
//        keyString.append(dataSource.getUrl());
//        keyString.append(dataSource.getUserName());
//        keyString.append(dataSource.getPassword());
//        keyString.append(dataSource.getDriverClass());
//
//        String key = keyString.toString();
//        // check whether data source is present in cache or not 
//        DataSource ds = pool.get(key);
//        if(ds == null)
//        {
//            ds = MolapDataSourceFactory.getMolapDataSource(connectInfo);
//            pool.put(key, ds);
//        }
//
//        return ds;
//    }
//
//    /**
//     * This method checked whether datasource is molap data source or not 
//     * 
//     * @param dataSource
//     *          data source
//     * @return
//     *      return true if it is a molap data source
//     *
//     */
//    private boolean isMolapDataSource(IDatasource dataSource)
//    {
//        if(dataSource != null)
//        {
//            // get the driver class name
//            String driverClass = dataSource.getDriverClass();
//            
//            // comapre data source 
//            if(OlapFactory.INMEMORY_DATA_SOURCE.equals(driverClass))
//            {
//                return true;
//            }
//            if(OlapFactory.HBASE_DATA_SOURCE.equals(driverClass))
//            {
//                return true;
//            }
//            if(OlapFactory.SPARK_DATA_SOURCE.equals(driverClass))
//            {
//                return true;
//            }
//            
//        }
//        return false;
//    }
}
