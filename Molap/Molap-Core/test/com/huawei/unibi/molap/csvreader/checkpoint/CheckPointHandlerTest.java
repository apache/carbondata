package com.huawei.unibi.molap.csvreader.checkpoint;

import com.huawei.unibi.molap.csvreader.checkpoint.exception.CheckPointException;

import junit.framework.TestCase;

public class CheckPointHandlerTest extends TestCase
{
    public void testGetCheckPointCache()
    {
        CheckPointInterface  a= new CSVCheckPointHandler("a", "b");
        try
        {
            a.getCheckPointCache();
        }
        catch(CheckPointException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
}
