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

package com.huawei.unibi.molap.util;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;



/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :r70299
 * Created Date :Sep 4, 2013
 * FileName : LoadRestructureLock.java
 * Class Description : 
 * Version 1.0
 */
public class LoadRestructureLock
{

    private Queue<Thread> taskQueue = new LinkedBlockingQueue<Thread>();
    private int rsLockCtr;
    private int loadLockCtr;
    
    /**
     * restructureLock
     * @throws Exception void
     */
    public void restructureLock() throws Exception
    {
        if(rsLockCtr > 0)
        {
            throw new Exception();
        }
        rsLockCtr++;
        taskQueue.add(Thread.currentThread());
    }
    
    /**
     * loadLock void
     */
    public void loadLock()
    {
        loadLockCtr++;        
        taskQueue.add(Thread.currentThread());
    }
    
    /*private void start()
    {
        Thread t = taskQueue.peek();
      
    }*/
} 
