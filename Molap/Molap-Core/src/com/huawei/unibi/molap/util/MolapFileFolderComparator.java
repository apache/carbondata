package com.huawei.unibi.molap.util;

import java.util.Comparator;

import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;

public class MolapFileFolderComparator implements Comparator<MolapFile>
{

    //TODO SIMIAN
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
    public int compare(MolapFile o1, MolapFile o2)
    {
        String firstFileName=o1.getName();
        String secondFileName=o2.getName();
        int lastIndexOfO1 = firstFileName.lastIndexOf('_');
        int lastIndexOfO2 = secondFileName.lastIndexOf('_');
        int file1= 0;   
        int file2= 0;
            
        try
        {
             file1 = Integer.parseInt(firstFileName.substring(lastIndexOfO1+1));
             file2 = Integer.parseInt(secondFileName.substring(lastIndexOfO2+1));
        }
        catch(NumberFormatException e)
        {
            return -1;
        }
        return (file1 < file2) ? -1 : (file1 == file2 ? 0 : 1);
    }
}
