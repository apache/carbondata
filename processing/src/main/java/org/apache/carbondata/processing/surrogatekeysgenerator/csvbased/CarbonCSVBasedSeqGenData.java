/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.KeyGenerator;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class CarbonCSVBasedSeqGenData extends BaseStepData implements StepDataInterface {

  /**
   * outputRowMeta
   */
  private RowMetaInterface outputRowMeta;

  /**
   * surrogateKeyGen
   */
  private CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen;

  /**
   * keyGenerators
   */
  private Map<String, KeyGenerator> keyGenerators =
      new HashMap<String, KeyGenerator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * columnIndex
   */
  private Map<String, int[]> columnIndex =
      new HashMap<String, int[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

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


  public CarbonCSVBasedSeqGenData() {
    super();
  }

  /**
   * @return Returns the surrogateKeyGen.
   */
  public CarbonCSVBasedDimSurrogateKeyGen getSurrogateKeyGen() {
    return surrogateKeyGen;
  }

  /**
   * @param surrogateKeyGen The surrogateKeyGen to set.
   */
  public void setSurrogateKeyGen(CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen) {
    this.surrogateKeyGen = surrogateKeyGen;
  }

  /**
   * @param inputSize The inputSize to set.
   */
  public void setInputSize(int inputSize) {
    this.inputSize = inputSize;
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

  public void clean() {
    outputRowMeta = null;

    surrogateKeyGen = null;

    generator = null;
    keyGenerators = null;

    columnIndex = null;

    defaultObjects = null;

  }
}
