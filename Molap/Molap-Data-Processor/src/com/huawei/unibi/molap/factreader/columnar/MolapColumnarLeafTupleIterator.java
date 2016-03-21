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

package com.huawei.unibi.molap.factreader.columnar;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.factreader.FactReaderInfo;
import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

public class MolapColumnarLeafTupleIterator  implements
                    MolapIterator<MolapSurrogateTupleHolder>
{

    /**
     * unique value if slice
     */
    private double[] uniqueValue;

    /**
     * hash next
     */
    private boolean hasNext;

    /**
     * leaf node iterator
     */
    private MolapIterator<AbstractColumnarScanResult> leafNodeIterator;
    
    /**
     * measureCount
     */
    private int measureCount;
    
    /**
     * aggType
     */
    private char [] aggType;
    
    /**
     * keyValue
     */
    private AbstractColumnarScanResult keyValue;
    
    private boolean isMeasureUpdateResuired;
    
    /**
     * rowCounter
     */

    /**
     * MolapSliceTupleIterator constructor to initialise
     * 
     * @param mdkeyLength
     *            mdkey length
     */
    public MolapColumnarLeafTupleIterator(String sliceLocation, MolapFile[] factFiles,FactReaderInfo factItreatorInfo, int mdkeyLength)
    {
        this.measureCount=factItreatorInfo.getMeasureCount();
        ValueCompressionModel compressionModel = getCompressionModel(sliceLocation,
                factItreatorInfo.getTableName(), measureCount);
        this.uniqueValue=compressionModel.getUniqueValue();
        this.leafNodeIterator = new MolapColumnarLeafNodeIterator(factFiles,mdkeyLength,compressionModel,factItreatorInfo);
        this.aggType=compressionModel.getType();
        initialise();
        this.isMeasureUpdateResuired=factItreatorInfo.isUpdateMeasureRequired();
    }
    
    public MolapColumnarLeafTupleIterator(String loadPath,
			MolapFile[] factFiles, FactReaderInfo factReaderInfo,
			int mdKeySize, LeafNodeInfoColumnar leafNodeInfoColumnar) {
        this.measureCount=factReaderInfo.getMeasureCount();
        ValueCompressionModel compressionModel = getCompressionModel(loadPath,
        		factReaderInfo.getTableName(), measureCount);
        this.uniqueValue=compressionModel.getUniqueValue();
        this.leafNodeIterator = new MolapColumnarLeafNodeIterator(factFiles,mdKeySize,compressionModel,factReaderInfo,leafNodeInfoColumnar);
        this.aggType=compressionModel.getType();
        initialise();
        this.isMeasureUpdateResuired=factReaderInfo.isUpdateMeasureRequired();
		
	}

	/**
     * below method will be used to initialise
     */
    private void initialise()
    {
        if(this.leafNodeIterator.hasNext())
        {
            keyValue=leafNodeIterator.next();
            this.hasNext=true;
        }
    }

    /**
     * This method will be used to get the compression model for slice
     * 
     * @param measureCount
     *          measure count
     * @return compression model
     *
     */
    private ValueCompressionModel getCompressionModel(String sliceLocation,String tableName, int measureCount)
    {
        ValueCompressionModel compressionModel = ValueCompressionUtil
                .getValueCompressionModel(sliceLocation
                        + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                        + tableName
                        + MolapCommonConstants.MEASUREMETADATA_FILE_EXT,
                        measureCount);
        return compressionModel;
    }

    /**
     * below method will be used to get the measure value from measure data
     * wrapper
     * 
     * @return
     */
    private Object[] getMeasure()
    {
        Object[] measures = new Object[measureCount];
        double values=0;
        for(int i = 0;i < measures.length;i++)
        {
            if(aggType[i]=='n')
            {
                values = keyValue.getNormalMeasureValue(i);
				if (isMeasureUpdateResuired && values != uniqueValue[i])
                {
                    measures[i] = values;
                }
            }
            else
            {
                measures[i] = keyValue.getCustomMeasureValue(i);
            }
        }
        return measures;
    }

    /**
     * below method will be used to check whether any data is present in the
     * slice
     */
    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    /**
     * below method will be used to get the slice tuple
     */
    @Override
    public MolapSurrogateTupleHolder next()
    {
        MolapSurrogateTupleHolder tuple = new MolapSurrogateTupleHolder();
        tuple.setSurrogateKey(keyValue.getKeyArray());
        tuple.setMeasures(getMeasure());
        if(keyValue.hasNext())
        {
            return tuple;
        }
        else if(!leafNodeIterator.hasNext())
        {
            hasNext = false;
        }
        else
        {
            initialise();
        }
        return tuple;
    }
}
