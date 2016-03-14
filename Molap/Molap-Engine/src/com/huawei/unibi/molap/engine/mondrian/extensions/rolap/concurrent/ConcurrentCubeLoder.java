/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3YT5i5Qb5zi7l2Le7WMDZun/u10Sn+D7fwIsVWOQQU3ePl7Yu889z2vrbd6HPQmRwmpr
9VLaXCVUpQM3ZBtU12XCmZJE/ZWKvjZxq78KrIdiTRcWXbQwlclAaFLgOvMQ+Q==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent;

//import java.util.ArrayList;
import java.util.LinkedList;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;

//import com.huawei.iweb.platform.logging.LogService;
//import com.huawei.iweb.platform.logging.LogServiceFactory;
//import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
//import com.huawei.unibi.molap.util.MolapProperties;



/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :29-Jul-2013
 * FileName : CuncurrentCubeLoder.java
 * Class Description : 
 * Version 1.0
 */
public class ConcurrentCubeLoder implements ICubeLoader
{

    @Override
    public void submit(CubeLoader cubeLoader)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void start()
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
    public void minThresholdReached()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void maxThresholdReached()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public LinkedList<CubeLoader> getFailedCubes()
    {
        // TODO Auto-generated method stub
        return null;
    }
//    /**
//     * 
//     */
//    private int DEFAULT_NUM_CUBES = 5;
//    
//    /**
//     * 
//     */
//    private int MIN_NUM_CUBES = 1;
//    
//    /**
//     * 
//     */
//    private int MAX_NUM_CUBES = 5;
//    
//    /**
//     * 
//     */
//    private int noOfCubesToLoadConcurrent = DEFAULT_NUM_CUBES;
//    
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(ConcurrentCubeLoder.class.getName());
//    public ConcurrentCubeLoder()
//    {
//        getNumOfCubes();
//        exec = Executors.newFixedThreadPool(noOfCubesToLoadConcurrent);
//        sensor = new MemorySensor(this);
//    }
//
//
//    private void getNumOfCubes()
//    {
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG," getNumOfCubes start");
//        String numberofCubes = MolapProperties.getInstance().getProperty("molap.numberOfCubesToLoadConcurrent", Integer.toString(DEFAULT_NUM_CUBES));
//        try
//        {
//            noOfCubesToLoadConcurrent = Integer.parseInt(numberofCubes);
//        }
//        catch(NumberFormatException e)
//        {
//            noOfCubesToLoadConcurrent = DEFAULT_NUM_CUBES;
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"The value set of the molap.numberOfCubesToLoadConcurrent is not correct so taking default value: "+noOfCubesToLoadConcurrent);
//        }
//        
//        if( noOfCubesToLoadConcurrent < MIN_NUM_CUBES || noOfCubesToLoadConcurrent > MAX_NUM_CUBES)
//        {
//            noOfCubesToLoadConcurrent = DEFAULT_NUM_CUBES;
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"The value set of the molap.numberOfCubesToLoadConcurrent is out of range so taking default value: "+noOfCubesToLoadConcurrent);
//        }
//        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG," getNumOfCubes end "+noOfCubesToLoadConcurrent);
//    }
//
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
//    
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
//            if( results.length > 0)
//            {
//                // System.out.println(" Check from the concurrent cubes");
//                complete = true;
//                try
//                {
//                    for(Future<CubeLoader> tas : results)
//                    {
//                        if(!tas.isDone())
//                        {
//                            CubeLoader res = tas.get();
//                            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " task completed:" + res.toString());
//                        }
//                    }
//                }
//                catch(InterruptedException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
//                    complete = false;
//                }
//                catch(ExecutionException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
//                    complete = false;
//                }
//            }
//            else
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
//    @SuppressWarnings("unchecked")
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
//            results[i++] = exec.submit(cube);
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
//    @Override
//    public String toString()
//    {
//        return "ConcurrentCubeLoder";
//    }
}
