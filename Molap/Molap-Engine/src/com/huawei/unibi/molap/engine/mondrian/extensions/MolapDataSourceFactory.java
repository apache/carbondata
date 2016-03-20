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


public class MolapDataSourceFactory
{
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapDataSourceFactory.class.getName());
//
//    /**
//     * @param connectInfo
//     * @return
//     */
//    public static DataSource getMolapDataSource(Util.PropertyList connectInfo)
//    {
//
//        DataSource dataSource = null;
//        String jdbcDrivers = connectInfo.get(RolapConnectionProperties.JdbcDrivers.name());
//        try
//        {
//            /* if (OlapFactory.HBASE_DATA_SOURCE.equals(jdbcDrivers))
//             {
//             //dataSource = new HbaseDataSourceImpl(connectInfo);
//            
//             }*/
//            if(OlapFactory.INMEMORY_DATA_SOURCE.equals(jdbcDrivers) || OlapFactory.SPARK_DATA_SOURCE.equals(jdbcDrivers))
//            {
//                dataSource = new MolapDataSourceImpl(connectInfo);
//            }
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//        return dataSource;
//    }

}
