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

package com.huawei.unibi.molap.engine.mondrian.extensions.rolap.concurrent;

import java.util.LinkedList;

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
