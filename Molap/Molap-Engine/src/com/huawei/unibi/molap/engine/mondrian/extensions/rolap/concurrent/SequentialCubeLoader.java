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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9LjRGTxZ4y8Wv3AC4kEbx7TN9PW5eYW/mevY229AXvGNzya25Yz6N9tMq25NedwkyZf2
sgGKThgCN6WfO5vG+MEO0XePZCJXv214VG7rkTZs3lsJD9afnVb36PnWY4+huA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent;

//import java.util.ArrayList;
import java.util.LinkedList;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;

//import com.huawei.iweb.platform.logging.LogService;
//import com.huawei.iweb.platform.logging.LogServiceFactory;
//import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;


/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :02-Aug-2013
 * FileName : SequentialCubeLoader.java
 * Class Description : 
 * Version 1.0
 */
public class SequentialCubeLoader implements ICubeLoader
{

    @Override
    public void submit(CubeLoader cubeLoader)
    {
        // TODO Auto-generated method stub
        
    }
    
    //TODO SIMIAN
    @Override
    public void maxThresholdReached()
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void start()
    {
        // TODO Auto-generated method stub
        
    }

    //TODO SIMIAN
    @Override
    public void minThresholdReached()
    {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean isDone()
    {
        // TODO Auto-generated method stub
        return false;
    }

   
    @Override
    public LinkedList<CubeLoader> getFailedCubes()
    {
        // TODO Auto-generated method stub
        return null;
    }

//    /**
//     * future tasks
//     */
//    private Future<CubeLoader>[] results ;
//    
//    /**
//     * memory sensor
//     */
//    protected MemorySensor sensor;
//
//    /**
//     * cubes to load
//     */
//    private LinkedList<CubeLoader> cubesQueue = new LinkedList<CubeLoader>();
//    
//    /**
//     * failed to load
//     */
//    private LinkedList<CubeLoader> cubesFailedFirst = new LinkedList<CubeLoader>();
//    
//    /**
//     * task is completed or not
//     */
//    private Boolean complete = false;
//    
//    /**
//     * syncObject
//     */
//    private final Object syncObject = new Object();
//    
//    /**
//     * CubeLoadercreate the thread poll
//     */
//    protected ExecutorService exec;
//    
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(SequentialCubeLoader.class.getName());
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#isDone()
//     */
//    @Override
//    public boolean isDone()
//    {
//        synchronized(syncObject)
//        {
//            if(null == results)
//            {
//              //failure condition as results might be cleared in case of 
//              // memory threshold reached i.e complete cos it failed
//                complete=true;
//                return complete;
//            }
//            
//            if( results.length > 0)
//            {
//                //System.out.println(" Check from the concurrent cubes");
//                complete = true;
//                for (Future<CubeLoader> tas : results) 
//                {
//                    if(!tas.isDone())
//                    {
//                       complete = false;
//                    }
//                }
//            }else
//            {
//                complete = true;
//            }
//        }
//        if( complete )
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG," Stoping the Thread Executor ");
//            exec.shutdown();
//            sensor.shutdown();
//        }
//        return complete;
//    }
//    
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#minThresholdReached()
//     */
//    @Override
//    public void minThresholdReached()
//    {
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG," minThresholdReached ");
//        exec.shutdownNow();
//        sensor.shutdown();
//        complete = true;
//        int length = cubesQueue.size();
//        for(int i=0;i<length;i++)
//        {
//            CubeLoader cube = cubesQueue.poll();
//            if(!cube.isCompleted() )
//            {
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,cube +" still not completed");
//                cubesFailedFirst.add(cube);
//            } 
//        }
//        results = null;
//    }
//
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#maxThresholdReached()
//     */
//    @Override
//    public void maxThresholdReached()
//    {
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"maxThresholdReached");
//        exec.shutdownNow();
//        sensor.shutdown();
//        for(CubeLoader eachTask : cubesQueue)
//        {
//            if( null != eachTask)
//            {
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"maxThresholdReached so "+ eachTask +" stoped loading");
//            }
//        }
//        complete = true;
//    }
//
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#start()
//     */
//    @Override
//    public void start()
//    {
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"starting loading cubes");
//        ArrayList<Future<CubeLoader>> arrayList = new ArrayList<Future<CubeLoader>>(cubesQueue.size());
//        results = arrayList.toArray(new Future[cubesQueue.size()]);
//        int i=0;
//        for(CubeLoader cube : cubesQueue)
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"starting cube "+cube);
//            results[i++]=exec.submit(cube);
//        }
//        sensor.start();
//    }
//
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#submit(mondrian.rolap.CubeLoader)
//     */
//    @Override
//    public void submit(CubeLoader cubeLoader)
//    {
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,cubeLoader+" is added");
//        cubesQueue.add(cubeLoader);
//    }
//
//    /* (non-Javadoc)
//     * @see com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent.ICubeLoader#getFailedCubes()
//     */
//    @Override
//    public LinkedList<CubeLoader> getFailedCubes()
//    {
//        return cubesFailedFirst;
//    }
//
//    /**
//     * 
//     */
//    public SequentialCubeLoader()
//    {
//        exec= Executors.newFixedThreadPool(1);
//        sensor = new MemorySensor(this);
//    }
//    
//    @Override
//    public String toString()
//    {
//        return "SequentialCubeLoader";
//    }
}
