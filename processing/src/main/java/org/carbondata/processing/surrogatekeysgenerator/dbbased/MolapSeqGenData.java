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

package org.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.util.HashMap;
import java.util.Map;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class MolapSeqGenData extends BaseStepData implements StepDataInterface {

    /**
     * outputRowMeta
     */
    private RowMetaInterface outputRowMeta;

    /**
     * surrogateKeyGen
     */
    private MolapDimSurrogateKeyGen surrogateKeyGen;

    /**
     * generator
     */
    private KeyGenerator generator;

    /**
     * keyGenerators
     */
    private Map<String, KeyGenerator> keyGenerators =
            new HashMap<String, KeyGenerator>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * columnIndex
     */
    private Map<String, int[]> columnIndex =
            new HashMap<String, int[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * precomputed default objects
     */
    private Object[] defaultObjects;

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

    public MolapSeqGenData() {
        super();
    }

    /**
     * @return Returns the outputRowMeta.
     */
    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    /**
     * @param outputRowMeta The outputRowMeta to set.
     */
    public void setOutputRowMeta(RowMetaInterface outputRowMeta) {
        this.outputRowMeta = outputRowMeta;
    }

    /**
     * @return Returns the surrogateKeyGen.
     */
    public MolapDimSurrogateKeyGen getSurrogateKeyGen() {
        return surrogateKeyGen;
    }

    /**
     * @param surrogateKeyGen The surrogateKeyGen to set.
     */
    public void setSurrogateKeyGen(MolapDimSurrogateKeyGen surrogateKeyGen) {
        this.surrogateKeyGen = surrogateKeyGen;
    }

    /**
     * @return Returns the generator.
     */
    public KeyGenerator getGenerator() {
        return generator;
    }

    /**
     * @param generator The generator to set.
     */
    public void setGenerator(KeyGenerator generator) {
        this.generator = generator;
    }

    /**
     * @return Returns the keyGenerators.
     */
    public Map<String, KeyGenerator> getKeyGenerators() {
        return keyGenerators;
    }

    /**
     * @param keyGenerators The keyGenerators to set.
     */
    public void setKeyGenerators(Map<String, KeyGenerator> keyGenerators) {
        this.keyGenerators = keyGenerators;
    }

    /**
     * @return Returns the columnIndex.
     */
    public Map<String, int[]> getColumnIndex() {
        return columnIndex;
    }

    /**
     * @param columnIndex The columnIndex to set.
     */
    public void setColumnIndex(Map<String, int[]> columnIndex) {
        this.columnIndex = columnIndex;
    }

    /**
     * @return Returns the defaultObjects.
     */
    public Object[] getDefaultObjects() {
        return defaultObjects;
    }

    /**
     * @param defaultObjects The defaultObjects to set.
     */
    public void setDefaultObjects(Object[] defaultObjects) {
        this.defaultObjects = defaultObjects;
    }

    /**
     * @return Returns the inputSize.
     */
    public int getInputSize() {
        return inputSize;
    }

    /**
     * @param inputSize The inputSize to set.
     */
    public void setInputSize(int inputSize) {
        this.inputSize = inputSize;
    }

    /**
     * @return Returns the keyFieldIndex.
     */
    public int[] getKeyFieldIndex() {
        return keyFieldIndex;
    }

    /**
     * @param keyFieldIndex The keyFieldIndex to set.
     */
    public void setKeyFieldIndex(int[] keyFieldIndex) {
        this.keyFieldIndex = keyFieldIndex;
    }

    /**
     * @return Returns the conversionMeta.
     */
    public ValueMetaInterface[] getConversionMeta() {
        return conversionMeta;
    }

    /**
     * @param conversionMeta The conversionMeta to set.
     */
    public void setConversionMeta(ValueMetaInterface[] conversionMeta) {
        this.conversionMeta = conversionMeta;
    }
}

