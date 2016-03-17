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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/mod31uIuJbfX9GrosI/5laQ5xpJRA87ngvUO1OgM/KGYF7hvdSU7kZ3YMvI1j4N1scA
gxKNfgG0OPJxZp0jQbwyaKG1AD1XLWyg3V33MjhP+O3XJ7NamCODIufOjv3beg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.surrogatekeysgenerator.csvbased;

import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;


/**
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :24-Jun-2013 FileName : MolapSeqGenData.java Class Description : Version 1.0
 */
public class MolapCSVBasedSeqGenData extends BaseStepData implements StepDataInterface {

    /**
     * outputRowMeta
     */
    private RowMetaInterface outputRowMeta;
    
    /**
     * surrogateKeyGen
     */
    private MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen;
    
  
    /**
     * keyGenerators
     */
    private Map<String,KeyGenerator> keyGenerators = new HashMap<String,KeyGenerator>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * columnIndex
     */
    private Map<String,int[]> columnIndex = new HashMap<String,int[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * precomputed default objects
     */
    private Object[] defaultObjects;
    
    /**
     * generator
     */
    private KeyGenerator generator;
    

    /**
     * the size of the input rows
     */
    private int inputSize;

    /**
     * where the key field indexes are
     */
    private int[] keyFieldIndex;

    /**
     * meta info for a string conversion
     */
    private ValueMetaInterface[] conversionMeta;
   

    /**
     * 
     * @return Returns the surrogateKeyGen.
     * 
     */
    public MolapCSVBasedDimSurrogateKeyGen getSurrogateKeyGen()
    {
        return surrogateKeyGen;
    }

    /**
     * 
     * @return Returns the defaultObjects.
     * 
     */
    public Object[] getDefaultObjects()
    {
        return defaultObjects;
    }

    /**
     * 
     * @param defaultObjects The defaultObjects to set.
     * 
     */
    public void setDefaultObjects(Object[] defaultObjects)
    {
        this.defaultObjects = defaultObjects;
    }

    /**
     * 
     * @return Returns the inputSize.
     * 
     */
    public int getInputSize()
    {
        return inputSize;
    }
    
    /**
     * 
     * @return Returns the columnIndex.
     * 
     */
    public Map<String, int[]> getColumnIndex()
    {
        return columnIndex;
    }

    /**
     * 
     * @param columnIndex The columnIndex to set.
     * 
     */
    public void setColumnIndex(Map<String, int[]> columnIndex)
    {
        this.columnIndex = columnIndex;
    }


    /**
     * 
     * @param inputSize The inputSize to set.
     * 
     */
    public void setInputSize(int inputSize)
    {
        this.inputSize = inputSize;
    }

    /**
     * 
     * @return Returns the keyFieldIndex.
     * 
     */
    public int[] getKeyFieldIndex()
    {
        return keyFieldIndex;
    }

    /**
     * 
     * @param keyFieldIndex The keyFieldIndex to set.
     * 
     */
    public void setKeyFieldIndex(int[] keyFieldIndex)
    {
        this.keyFieldIndex = keyFieldIndex;
    }

    /**
     * 
     * @return Returns the conversionMeta.
     * 
     */
    public ValueMetaInterface[] getConversionMeta()
    {
        return conversionMeta;
    }

    /**
     * 
     * @param conversionMeta The conversionMeta to set.
     * 
     */
    public void setConversionMeta(ValueMetaInterface[] conversionMeta)
    {
        this.conversionMeta = conversionMeta;
    }
    

    /**
     * 
     * @param surrogateKeyGen The surrogateKeyGen to set.
     * 
     */
    public void setSurrogateKeyGen(MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen)
    {
        this.surrogateKeyGen = surrogateKeyGen;
    }

    /**
     * 
     * @return Returns the generator.
     * 
     */
    public KeyGenerator getGenerator()
    {
        return generator;
    }

    /**
     * 
     * @param generator The generator to set.
     * 
     */
    public void setGenerator(KeyGenerator generator)
    {
        this.generator = generator;
    }

    /**
     * 
     * @return Returns the keyGenerators.
     * 
     */
    public Map<String, KeyGenerator> getKeyGenerators()
    {
        return keyGenerators;
    }

    /**
     * 
     * @param keyGenerators The keyGenerators to set.
     * 
     */
    public void setKeyGenerators(Map<String, KeyGenerator> keyGenerators)
    {
        this.keyGenerators = keyGenerators;
    }
    
    /**
     * 
     * @return Returns the outputRowMeta.
     * 
     */
    public RowMetaInterface getOutputRowMeta()
    {
        return outputRowMeta;
    }

    /**
     * 
     * @param outputRowMeta The outputRowMeta to set.
     * 
     */
    public void setOutputRowMeta(RowMetaInterface outputRowMeta)
    {
        this.outputRowMeta = outputRowMeta;
    }

    public MolapCSVBasedSeqGenData()
    {
        super();
    }
    
    public void clean() 
    {
        outputRowMeta = null;

        surrogateKeyGen = null;

        generator = null;
        keyGenerators = null;

        columnIndex = null;

        defaultObjects = null;

        keyFieldIndex = null;

        conversionMeta = null;
    }
}
    
