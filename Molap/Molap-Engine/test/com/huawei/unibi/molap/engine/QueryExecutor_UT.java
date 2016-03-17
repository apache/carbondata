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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine;

import junit.framework.TestCase;

/**
 * 
 * Project Name NSE V3R7C10 
 * Module Name : Molap Engine 
 * Author A00903119
 * Created Date :25-Jul-2013 3:42:29 PM FileName : QueryExecutor_UT Class
 * Description : UT class for QueryExecutor Version 1.0
 */
public class QueryExecutor_UT extends TestCase
{/*
    Connection connection;
    
    @BeforeClass
    public void setUp()
    {
        connection = DriverManager.getConnection(makeConnectString(), null,
                new MolapDataSourceFactory().getMolapDataSource(makeConnectString()));
    }
    
    public  Util.PropertyList makeConnectString() 
    {
        String connectString = null;

        Util.PropertyList connectProperties;
        if (connectString == null || connectString.equals("")) 
        {
            // create new and add provider
            connectProperties = new Util.PropertyList();
            connectProperties.put(
                RolapConnectionProperties.Provider.name(),
                "mondrian");
        } else {
            // load with existing connect string
            connectProperties = Util.parseConnectString(connectString);
        }

        // override jdbc urlmondrian.jdbcURL
        String jdbcURL = MolapProperties.getInstance().getProperty("mondrian.jdbcURL","molap://sourceDB=rdbmscon;mode=file");//"jdbc:hsqldb:hsql://localhost:9001/sampledata";


        if (jdbcURL != null) {
            // add jdbc url to connect string
            connectProperties.put(
                RolapConnectionProperties.Jdbc.name(),
                jdbcURL);
        }

        // override jdbc drivers
        String jdbcDrivers = MolapProperties.getInstance().getProperty("mondrian.jdbcDrivers","com.huawei.unibi.molap.engine.datasource.MolapDataSourceImpl");//"org.hsqldb.jdbcDriver";

        if (jdbcDrivers != null) {
            // add jdbc drivers to connect string
            connectProperties.put(
                RolapConnectionProperties.JdbcDrivers.name(),
                jdbcDrivers);
        }

        // override catalog url
        String catalogURL = MolapProperties.getInstance().getProperty("mondrian.catalogURL",null);//"D:/Cloud/Pentaho/biserver-ce-3.9.0-stable/biserver-ce/pentaho-solutions/steel-wheels/analysis/steelwheels.mondrian.xml";

        if (catalogURL != null) {
            // add catalog url to connect string
            connectProperties.put(
                RolapConnectionProperties.Catalog.name(),
                catalogURL);
        }

        // override JDBC user
        String jdbcUser = "pentaho_user";


        if (jdbcUser != null) {
            // add user to connect string
            connectProperties.put(
                RolapConnectionProperties.JdbcUser.name(),
                jdbcUser);
        }

        // override JDBC password
        String jdbcPassword = "password";

        if (jdbcPassword != null) {
            // add password to connect string
            connectProperties.put(
                RolapConnectionProperties.JdbcPassword.name(),
                jdbcPassword);
        }
        return connectProperties;

    }

    @AfterClass
    public void tearDown()
    {
        connection.close();
    }
    
    @Test
    public void test_MOLAP_Query_Check_Query_Working()
    {
        Result result = formatAndExecuteQuery("");
        
        File file = new File(OUTPUT_LOCATION+File.separator+"Vishal5SecondsTest"+File.separator+"VishalPerfCube");
        
        if(file.exists())
        {
            assertTrue(true);
        }
        else
        {
            assertTrue(false);
        }
    }
    
    public void formatAndExecuteQuery(String query)
    {
        mondrian.olap.Query query = connection.parseQuery(query);
        return connection.execute(query);
    }
    
*/}
