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

package org.carbondata.processing.groupby.step;

import java.util.Arrays;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.processing.groupby.CarbonGroupBy;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CarbonGroupByStep extends BaseStep implements StepInterface {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonGroupByStep.class.getName());
  /**
   * carbon data writer step data class
   */
  private CarbonGroupByStepData data;

  /**
   * carbon data writer step meta
   */
  private CarbonGroupByStepMeta meta;

  /**
   * carbonGroupBy
   */
  private CarbonGroupBy carbonGroupBy;

  /**
   * CarbonSliceMergerStep Constructor
   *
   * @param stepMeta          stepMeta
   * @param stepDataInterface stepDataInterface
   * @param copyNr            copyNr
   * @param transMeta         transMeta
   * @param trans             trans
   */
  public CarbonGroupByStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  /**
   * Perform the equivalent of processing one row. Typically this means
   * reading a row from input (getRow()) and passing a row to output
   * (putRow)).
   *
   * @param smi The steps metadata to work with
   * @param sdi The steps temporary working data to work with (database
   *            connections, result sets, caches, temporary variables, etc.)
   * @return false if no more rows can be processed or an error occurred.
   * @throws KettleException
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    // carbon data writer step meta
    meta = (CarbonGroupByStepMeta) smi;

    // carbon data writer step data
    data = (CarbonGroupByStepData) sdi;
    // get row from previous step, blocks when needed!
    Object[] row = getRow();

    //outRow

    Object[] outRow = null;
    // if row is null then there is no more incoming data
    if (null == row) {
      if (null != this.carbonGroupBy) {
        outRow = this.carbonGroupBy.getLastRow();
        if (null != outRow) {
          putRow(data.getOutputRowMeta(), outRow);
        }
      }
      setOutputDone();
      return false;
    } else if (checkAllValuesAreNull(row)) {
      int outSize = Integer.parseInt(meta.getOutputRowSize());
      outRow = new Object[outSize];
      this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
      this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);
      setStepOutputInterface(outSize);
      putRow(data.getOutputRowMeta(), outRow);
      setOutputDone();
      return false;
    }
    if (first) {
      LOGGER.info("Start group by");
      first = false;
      this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
      this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);
      this.carbonGroupBy = new CarbonGroupBy(meta.getAggTypeString(), meta.getColumnName(),
          meta.getActualColumnName(), row);
      setStepOutputInterface(Integer.parseInt(meta.getOutputRowSize()));
      return true;
    }
    outRow = this.carbonGroupBy.add(row);
    if (null != outRow) {
      putRow(data.getOutputRowMeta(), outRow);
    }
    return true;
  }

  private boolean checkAllValuesAreNull(Object[] row) {
    for (int i = 0; i < row.length; i++) {
      if (null != row[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method will be used for setting the output interface.
   * Output interface is how this step will process the row to next step
   */
  private void setStepOutputInterface(int outRowSize) {
    ValueMetaInterface[] out = new ValueMetaInterface[outRowSize];
    int l = 0;
    for (int i = 0; i < outRowSize - 1; i++) {

      out[l] = new ValueMeta(i + "", ValueMetaInterface.TYPE_NUMBER,
          ValueMetaInterface.STORAGE_TYPE_NORMAL);
      out[l].setStorageMetadata(new ValueMeta(i + "", ValueMetaInterface.TYPE_NUMBER,
          ValueMetaInterface.STORAGE_TYPE_NORMAL));
      l++;
    }
    out[out.length - 1] = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
        ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
    out[out.length - 1].setStorageMetadata(new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
        ValueMetaInterface.STORAGE_TYPE_NORMAL));
    out[out.length - 1].setLength(256);
    out[out.length - 1].setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);
    out[out.length - 1].getStorageMetadata().setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);
    data.getOutputRowMeta().setValueMetaList(Arrays.asList(out));
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonGroupByStepMeta) smi;
    data = (CarbonGroupByStepData) sdi;
    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonGroupByStepMeta) smi;
    data = (CarbonGroupByStepData) sdi;
    super.dispose(smi, sdi);
    meta = null;
    data = null;
    this.carbonGroupBy = null;
  }

}
