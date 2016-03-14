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
package com.huawei.unibi.molap.util;

import java.io.File;
import java.util.Comparator;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Core
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : MolapFileComparator.java
 * Class Description :  Molap File Comparator
 * Version 1.0
 */
public class MolapFileComparator implements Comparator<File>
{

    /**
     * File extension
     */
    private String fileExt;
    
    public MolapFileComparator(String fileExt)
    {
        this.fileExt=fileExt;
    }
    /**
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     * 
     */
    @Override
    public int compare(File file1, File file2)
    {
        String firstFileName = file1.getName().split(fileExt)[0];
        String secondFileName = file2.getName().split(fileExt)[0];
        int lastIndexOfO1 = firstFileName.lastIndexOf('_');
        int lastIndexOfO2 = secondFileName.lastIndexOf('_');
        int f1= 0;
        int f2= 0;
        
        try
        {
             f1 = Integer.parseInt(firstFileName.substring(lastIndexOfO1+1));
             f2 = Integer.parseInt(secondFileName.substring(lastIndexOfO2+1));
        }
        catch(NumberFormatException e)
        {
            return -1;
        }
        return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
    }
}
