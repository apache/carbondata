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
package com.huawei.unibi.molap.util;

import java.io.File;
import java.util.Comparator;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Core
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : MolapFolderComparator.java
 * Class Description :  Molap Folder Comparator
 * Version 1.0
 */
public class MolapFolderComparator implements Comparator<File>
{

    /**
     * Below method will be used to compare two file 
     * 
     * @param o1
     *          first file
     *
     * @param o2
     *          Second file
     * @return compare result
     */
    @Override
    public int compare(File o1, File o2)
    {
        String firstFileName=o1.getName();
        String secondFileName=o2.getName();
        int lastIndexOffile1 = firstFileName.lastIndexOf('_'); 
        int lastIndexOffile2 = secondFileName.lastIndexOf('_'); 
        int f1= 0;
        int f2= 0;
        
        try
        {
             f1 = Integer.parseInt(firstFileName.substring(lastIndexOffile1+1));
             f2 = Integer.parseInt(secondFileName.substring(lastIndexOffile2+1));
        }
        catch(NumberFormatException e)
        {
            return -1;
        }
        return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
    }
}
