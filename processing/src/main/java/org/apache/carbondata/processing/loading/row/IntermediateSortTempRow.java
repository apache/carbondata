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
package org.apache.carbondata.processing.loading.row;

/**
 * During sort procedure, each row will be written to sort temp file in this logic format.
 * an intermediate sort temp row consists 3 parts:
 * dictSort, noDictSort, noSortDimsAndMeasures(dictNoSort, noDictNoSort, measure)
 */
public class IntermediateSortTempRow {
  private int[] dictSortDims;
  private Object[] noDictSortDims;
  /**
   * this will be used for intermediate merger when
   * no sort field and measure field will not be
   * used for sorting
   */
  private byte[] noSortDimsAndMeasures;
  /**
   * for final merger keep the measures
   */
  private Object[] measures;

  public IntermediateSortTempRow(int[] dictSortDims, Object[] noDictSortDims,
      byte[] noSortDimsAndMeasures) {
    this.dictSortDims = dictSortDims;
    this.noDictSortDims = noDictSortDims;
    this.noSortDimsAndMeasures = noSortDimsAndMeasures;
  }

  public IntermediateSortTempRow(int[] dictSortDims, Object[] noDictSortDims,
      Object[] measures) {
    this.dictSortDims = dictSortDims;
    this.noDictSortDims = noDictSortDims;
    this.measures = measures;
  }

  public int[] getDictSortDims() {
    return dictSortDims;
  }

  public Object[] getMeasures() {
    return measures;
  }

  public Object[] getNoDictSortDims() {
    return noDictSortDims;
  }

  public byte[] getNoSortDimsAndMeasures() {
    return noSortDimsAndMeasures;
  }
}
