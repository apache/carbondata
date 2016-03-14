/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3TZoKjH1tGRqiDdI9Tp0OEgUVQA3SqLBVCDb/KHye8kwpgyVhs8icbJGoVvQEIvYdE1/
1Xmzg4jMxIUY0Yz/WxNVZr5QUk3ukmGpLYmx0WQBLLU2QDh2dmI19RauiE6j2g==*/
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

import java.util.LinkedList;



/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :01-Aug-2013
 * FileName : ICubeLoader.java
 * Class Description : 
 * Version 1.0
 */
public interface ICubeLoader
{
    /**
     * Submit cubes which need to start by loader
     * @param cubeLoader
     */
    void submit(CubeLoader cubeLoader);
    /**
     * Start loading
     */
    void start();
    /**
     * @return whether the given cubes are completed or not
     */
    boolean isDone();
    /**
     * Handle min Threshold reached
     */
    void minThresholdReached();
    /**
     * Handle max threshold reached
     */
    void maxThresholdReached();
    /**
     * @return gives the failed cubes which are not loaded
     */
    LinkedList<CubeLoader> getFailedCubes();
}
