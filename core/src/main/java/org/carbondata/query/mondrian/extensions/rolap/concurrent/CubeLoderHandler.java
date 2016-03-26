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

package org.carbondata.query.mondrian.extensions.rolap.concurrent;

import java.util.LinkedList;

import org.apache.commons.collections.CollectionUtils;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.util.MolapEngineLogEvent;

public class CubeLoderHandler {
    /**
     *
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CubeLoderHandler.class.getName());
    /**
     * which are the cube to load
     */
    private LinkedList<CubeLoader> cubesQueue = new LinkedList<CubeLoader>();

    /**
     * Start the loading task
     */
    public void startLoading() {
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Starting loading the cubes");
        ConcurrentCubeLoder cubeLoader = new ConcurrentCubeLoder();
        int size = cubesQueue.size();
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                CubeLoader cube = cubesQueue.poll();
                cubeLoader.submit(cube);
            }
        }
        cubeLoader.start();
        //Wait till the concurrent load is done
        while (true) {
            if (cubeLoader.isDone()) {
                break;
            }
            //            try
            //            {
            //                Thread.sleep(5);
            //            }
            //            catch(InterruptedException e)
            //            {
            //                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Error for concurrent cube loading pause ",e);
            //            }
            //            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"waiting");
        }
        //if the concurrent is completed, then if any left load in sequential fashion
        if (cubeLoader.isDone()) {
            //CHECKSTYLE:OFF    Approval No:Approval-
            //            System.gc();
            //            System.gc();
            //CHECKSTYLE:on    Approval No:Approval-
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            }
            LinkedList<CubeLoader> failedConcurrent = cubeLoader.getFailedCubes();
            startSequencially(failedConcurrent);
        }
    }

    /**
     * @param loader
     * @param failedConcurrent
     */
    private void startSequencially(LinkedList<CubeLoader> failedConcurrent) {
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Starting loading the cubes startSequencially");
        SequentialCubeLoader loader = new SequentialCubeLoader();
        int size = failedConcurrent.size();
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                CubeLoader cube = failedConcurrent.poll();
                loader.submit(cube);
            }

            loader.start();
            while (true) {
                if (loader.isDone()) {
                    break;
                }
                //LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Wait");
            }
            LinkedList<CubeLoader> stillFailed = loader.getFailedCubes();
            if (CollectionUtils.isNotEmpty(stillFailed)) {
                for (CubeLoader stillCu : stillFailed) {
                    if (null != stillCu) {
                        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                                stillCu + " failed to load Sequencially");
                    }
                }
            }
        }
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "completed loading the cubes startSequencially");
    }

    /**
     * add cube to load
     *
     * @param cubeLoader
     */
    public void submit(CubeLoader cubeLoader) {
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                cubeLoader + " is submitted to load");
        cubesQueue.add(cubeLoader);
    }
}
