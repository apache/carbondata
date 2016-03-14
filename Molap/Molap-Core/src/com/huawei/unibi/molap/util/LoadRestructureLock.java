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
