package com.huawei.unibi.molap.util;

import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

public class MolapSliceAndFiles
{
    /**
     * slice path
     */
    private String path;

    /**
     * slice fact files 
     */
    private MolapFile[] sliceFactFilesList;
    
    private KeyGenerator keyGen;
    
    /**
     * This method will be used get the slice fact files 
     * 
     * @return slice fact files 
     *
     */
    public MolapFile[] getSliceFactFilesList()
    {
        return sliceFactFilesList;
    }

    /**
     * This method  will be used to set the slice fact files 
     * 
     * @param sliceFactFilesList
     *
     */
    public void setSliceFactFilesList(MolapFile[] sliceFactFilesList)
    {
        this.sliceFactFilesList = sliceFactFilesList;
    }
    
    /**
     * This method will return the slice path
     * 
     * @return slice path
     *
     */
    public String getPath()
    {
        return path;
    }

    /**
     * This method will be used to set the slice path 
     * 
     * @param path
     *
     */
    public void setPath(String path)
    {
        this.path = path;
    }

    /**
     * @return the keyGen
     */
    public KeyGenerator getKeyGen()
    {
        return keyGen;
    }

    /**
     * @param keyGen the keyGen to set
     */
    public void setKeyGen(KeyGenerator keyGen)
    {
        this.keyGen = keyGen;
    }

   
}
