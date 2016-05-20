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

package org.carbondata.core.metadata;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import org.carbondata.core.keygenerator.KeyGenerator;

public class SliceMetaData implements Serializable {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 3046237866264840878L;

  /**
   * Array of dimensions declared.
   */
  private String[] dimensions;

  private String complexTypeString;
  /**
   * actualDimensions
   */
  private String[] actualDimensions;
  /**
   * Array of measures declared.
   */
  private String[] measures;
  /**
   * measuresAggregator
   */
  private String[] measuresAggregator;
  /**
   * Array of newDimensions declared.
   */
  private String[] newDimensions;
  /**
   * actualNewDimensions
   */
  private String[] newActualDimensions;
  /**
   * tableNamesToLoadMandatory
   */
  private HashSet<String> tableNamesToLoadMandatory;
  /**
   * Array of newMeasures declared.
   */
  private String[] newMeasures;
  /**
   * Array of newMsrDfts declared.
   */
  private double[] newMsrDfts;
  /**
   * newMeasuresAggregator
   */
  private String[] newMeasuresAggregator;
  /**
   * heirAnKeySize
   */
  private String heirAnKeySize;
  /**
   * KeyGenerator declared.
   */
  private KeyGenerator keyGenerator;
  /**
   * dimLens
   */
  private int[] dimLens;
  /**
   * actualDimLens
   */
  private int[] actualDimLens;
  /**
   * newDimLens
   */
  private int[] newDimLens;
  /**
   * newActualDimLens
   */
  private int[] newActualDimLens;
  /**
   * oldDimsNewCardinality
   */
  private int[] oldDimsNewCardinality;
  /**
   * isDimCarinalityChanged
   */
  private boolean isDimCarinalityChanged;
  private String[] newDimsDefVals;
  private int[] newDimsSurrogateKeys;
  /**
   * new keygenerator
   */
  private KeyGenerator newKeyGenerator;

  public String getComplexTypeString() {
    return complexTypeString;
  }

  public void setComplexTypeString(String complexTypeString) {
    this.complexTypeString = complexTypeString;
  }

  public int[] getNewDimLens() {
    return newDimLens;
  }

  public void setNewDimLens(int[] newDimLens) {
    this.newDimLens = newDimLens;
  }

  public int[] getDimLens() {
    return dimLens;
  }

  public void setDimLens(int[] dimLens) {
    this.dimLens = dimLens;
  }

  public KeyGenerator getNewKeyGenerator() {
    return newKeyGenerator;
  }

  public void setNewKeyGenerator(KeyGenerator newKeyGenerator) {
    this.newKeyGenerator = newKeyGenerator;
  }

  public String[] getDimensions() {
    return dimensions;
  }

  public void setDimensions(String[] dimensions) {
    this.dimensions = dimensions;
  }

  public String[] getMeasures() {
    return measures;
  }

  public void setMeasures(String[] measures) {
    this.measures = measures;
  }

  public String[] getNewDimensions() {
    return newDimensions;
  }

  public void setNewDimensions(String[] newDimensions) {
    this.newDimensions = newDimensions;
  }

  public String[] getNewMeasures() {
    return newMeasures;
  }

  public void setNewMeasures(String[] newMeasures) {
    this.newMeasures = newMeasures;
  }

  public double[] getNewMsrDfts() {
    return newMsrDfts;
  }

  public void setNewMsrDfts(double[] newMsrDfts) {
    this.newMsrDfts = newMsrDfts;
  }

  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  public void setKeyGenerator(KeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(dimensions);
    result = prime * result + ((keyGenerator == null) ? 0 : keyGenerator.hashCode());
    result = prime * result + Arrays.hashCode(measures);
    result = prime * result + Arrays.hashCode(newDimensions);
    result = prime * result + Arrays.hashCode(newMeasures);
    result = prime * result + Arrays.hashCode(newMsrDfts);
    result = prime * result + Arrays.hashCode(dimLens);
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof SliceMetaData) {
      SliceMetaData other = (SliceMetaData) obj;
      if (Arrays.equals(dimensions, other.dimensions) && Arrays
          .equals(measuresAggregator, other.measuresAggregator) && Arrays
          .equals(dimLens, other.dimLens)) {
        return true;
      }
    }
    return false;
  }

  public boolean isSameAs(SliceMetaData other) {
    return (Arrays.equals(newDimensions, other.newDimensions) && Arrays
        .equals(newMeasures, other.newMeasures));
  }

  public String[] getMeasuresAggregator() {
    return measuresAggregator;
  }

  public void setMeasuresAggregator(String[] measuresAggregator) {
    this.measuresAggregator = measuresAggregator;
  }

  public String[] getNewMeasuresAggregator() {
    return newMeasuresAggregator;
  }

  public void setNewMeasuresAggregator(String[] newMeasuresAggregator) {
    this.newMeasuresAggregator = newMeasuresAggregator;
  }

  public String getHeirAnKeySize() {
    return heirAnKeySize;
  }

  public void setHeirAnKeySize(String heirAnKeySize) {
    this.heirAnKeySize = heirAnKeySize;
  }

  public boolean isDimCarinalityChanged() {
    return isDimCarinalityChanged;
  }

  public void setDimCarinalityChanged(boolean isDimCarinalityChanged) {
    this.isDimCarinalityChanged = isDimCarinalityChanged;
  }

  public int[] getOldDimsNewCardinality() {
    return oldDimsNewCardinality;
  }

  public void setOldDimsNewCardinality(int[] oldDimsNewCardinality) {
    this.oldDimsNewCardinality = oldDimsNewCardinality;
  }

  public String[] getNewActualDimensions() {
    return newActualDimensions;
  }

  public void setNewActualDimensions(String[] newActualDimensions) {
    this.newActualDimensions = newActualDimensions;
  }

  public int[] getNewActualDimLens() {
    return newActualDimLens;
  }

  public void setNewActualDimLens(int[] newActualDimLens) {
    this.newActualDimLens = newActualDimLens;
  }

  public String[] getActualDimensions() {
    return actualDimensions;
  }

  public void setActualDimensions(String[] actualDimensions) {
    this.actualDimensions = actualDimensions;
  }

  public int[] getActualDimLens() {
    return actualDimLens;
  }

  public void setActualDimLens(int[] actualDimLens) {
    this.actualDimLens = actualDimLens;
  }

  public HashSet<String> getTableNamesToLoadMandatory() {
    return tableNamesToLoadMandatory;
  }

  public void setTableNamesToLoadMandatory(HashSet<String> tableNamesToLoadMandatory) {
    this.tableNamesToLoadMandatory = tableNamesToLoadMandatory;
  }

  /**
   * return the new dimensions default values
   */
  public String[] getNewDimsDefVals() {
    return newDimsDefVals;
  }

  /**
   * set the default values of new dimensions added
   */
  public void setNewDimsDefVals(String[] newDimsDefVals) {
    this.newDimsDefVals = newDimsDefVals;
  }

  /**
   * return the surrogate keys of new dimension values
   */
  public int[] getNewDimsSurrogateKeys() {
    return newDimsSurrogateKeys;
  }

  /**
   * set the surrogate keys for the new dimension values
   */
  public void setNewDimsSurrogateKeys(int[] newDimsSurrogateKeys) {
    this.newDimsSurrogateKeys = newDimsSurrogateKeys;
  }
}
