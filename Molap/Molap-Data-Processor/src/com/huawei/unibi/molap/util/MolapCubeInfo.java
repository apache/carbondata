/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbQU1kq6nMTEda3dx7xjs4WdrWd21771ZcC5B/CNWrvcCoktVmNXlG7DQVhlQM0TFPACL
7ZDuIaIW/2pIbMYA/OCS5EKJ8rx7pRx8k8x0AtpUQeSsH0IIkUsZk+y0imbAKA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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

import java.util.List;

import com.huawei.unibi.molap.olap.MolapDef.Cube;

/**
 * 
 * Project Name NSE V3R7C00 Module Name : Molap Data Processor Author K00900841
 * Created Date :21-May-2013 6:42:29 PM FileName : MolapCubeInfo.java Class
 * Description : This class is responsible for holding cube level metadata
 * Version 1.0
 */
public class MolapCubeInfo
{

    /**
     * cubeName.
     */
    private String cubeName;

    private String schemaName;

    private List<String> factAndAggtables;

    private Cube cube;
    
    private String schemaPath;
    
    

    /**
     * getCube
     * @return Cube
     */
    public Cube getCube()
    {
        return cube;
    }

    /**
     * setCube
     * @param cube void
     */
    public void setCube(Cube cube)
    {
        this.cube = cube;
    }

    /**
     * getCubeName
     * @return String
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * setCubeName
     * @param cubeName void
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * getSchemaName
     * @return String
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * setSchemaName
     * @param schemaName void
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * getTableNames
     * @return List<String>
     */
    public List<String> getTableNames()
    {
        return factAndAggtables;
    }

    /**
     * setTableNames
     * @param tableNames void
     */
    public void setTableNames(List<String> tableNames)
    {
        this.factAndAggtables = tableNames;
    }

    /**
     * setCubeName
     * @param cube void
     */
    public void setCubeName(Cube cube)
    {
        this.cube = cube;

    }

    /**
     * @return the schemaPath
     */
    public String getSchemaPath()
    {
        return schemaPath;
    }

    /**
     * @param schemaPath the schemaPath to set
     */
    public void setSchemaPath(String schemaPath)
    {
        this.schemaPath = schemaPath;
    }
}
