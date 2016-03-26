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

package org.carbondata.processing.threadbasedmerger.container;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;

public class Container {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(Container.class.getName());
    /**
     * record array
     */
    private Object[][] sortHolderArray;

    /**
     * is array filled
     */
    private boolean isFilled;

    /**
     * is done
     */
    private boolean isDone;

    /**
     * container counter
     */
    private int containerCounter;

    /**
     * Below method will be used to fill the container
     *
     * @param sortHolder
     */
    public void fillContainer(Object[][] sortHolder) {
        sortHolderArray = sortHolder;
    }

    /**
     * below method will be used to get the container data
     *
     * @return
     */
    public Object[][] getContainerData() {
        //CHECKSTYLE:OFF
        while (!isFilled && !isDone) {
            try//CHECKSTYLE:ON
            {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
            }
        }

        Object[][] temp = sortHolderArray;
        sortHolderArray = null;
        return temp;
    }

    /**
     * @return the isFilled
     */
    public boolean isFilled() {
        return isFilled;
    }

    /**
     * @param isFilled the isFilled to set
     */
    public void setFilled(boolean isFilled) {
        this.isFilled = isFilled;
    }

    /**
     * @return the isDone
     */
    public boolean isDone() {
        return isDone;
    }

    /**
     * @param isDone the isDone to set
     */
    public void setDone(boolean isDone) {
        this.isDone = isDone;
    }

    /**
     * @return the containerCounter
     */
    public int getContainerCounter() {
        return containerCounter;
    }

    /**
     * @param containerCounter the containerCounter to set
     */
    public void setContainerCounter(int containerCounter) {
        this.containerCounter = containerCounter;
    }
}
