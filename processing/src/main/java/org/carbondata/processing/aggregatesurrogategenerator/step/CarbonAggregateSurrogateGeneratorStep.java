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

package org.carbondata.processing.aggregatesurrogategenerator.step;

import java.util.Arrays;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.processing.aggregatesurrogategenerator.AggregateSurrogateGenerator;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class CarbonAggregateSurrogateGeneratorStep extends BaseStep implements StepInterface {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonAggregateSurrogateGeneratorStep.class.getName());

    /**
     * BYTE ENCODING
     */
    private static final String BYTE_ENCODING = "ISO-8859-1";

    /**
     * meta
     */
    private CarbonAggregateSurrogateGeneratorMeta meta;

    /**
     * data
     */
    private CarbonAggregateSurrogateGeneratorData data;

    /**
     * rowCounter
     */
    private long readCounter;

    /**
     * writeCounter
     */
    private long writeCounter;

    /**
     * logCounter
     */
    private int logCounter;

    /**
     * aggregateRecordIterator
     */
    private AggregateSurrogateGenerator aggregateRecordIterator;

    /**
     * MolapAggregateSurrogateGeneratorStep Constructor to initialize the step
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public CarbonAggregateSurrogateGeneratorStep(StepMeta stepMeta,
            StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
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
        try {
            Object[] factTuple = getRow();
            if (first) {
                meta = (CarbonAggregateSurrogateGeneratorMeta) smi;
                data = (CarbonAggregateSurrogateGeneratorData) sdi;
                meta.initialize();
                first = false;
                if (null != getInputRowMeta()) {
                    this.data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
                    this.meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
                    setStepOutputInterface(meta.isMdkeyInOutRowRequired());
                }

                String[] aggLevels = meta.getAggregateLevels();
                String[] factLevels = meta.getFactLevels();
                int[] cardinality = meta.getFactDimLens();

                int[] aggCardinality = new int[aggLevels.length];
                Arrays.fill(aggCardinality, -1);

                for (int i = 0; i < aggLevels.length; i++) {
                    for (int j = 0; j < factLevels.length; j++) {
                        if (aggLevels[i]
                                .equals(factLevels[j])) {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_001
                            aggCardinality[i] = cardinality[j];
                            break;
                        }// CHECKSTYLE:ON
                    }
                }
                this.aggregateRecordIterator =
                        new AggregateSurrogateGenerator(factLevels, aggLevels,
                                meta.getFactMeasure(), meta.getAggregateMeasures(),
                                meta.isMdkeyInOutRowRequired(), aggCardinality);

                this.logCounter = Integer.parseInt(
                        CarbonCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
            }
            if (null == factTuple) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + meta.getTableName());
                String logMessage =
                        "Summary: Molap Auto Aggregate Generator Step: Read: " + readCounter
                                + ": Write: " + writeCounter;
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
                setOutputDone();
                return false;
            }
            readCounter++;
            putRow(data.outputRowMeta, this.aggregateRecordIterator.generateSurrogate(factTuple));
            writeCounter++;
            if (readCounter % logCounter == 0) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + meta.getTableName());
                String logMessage =
                        "Molap Auto Aggregate Generator Step: Read: " + readCounter + ": Write: "
                                + writeCounter;
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            }
        } catch (Exception ex) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
            throw new RuntimeException(ex);
        }
        return true;
    }

    /**
     * This method will be used for setting the output interface. Output
     * interface is how this step will process the row to next step
     */
    private void setStepOutputInterface(boolean isMdkeyRequiredInOutRow) {
        String[] aggregateMeasures = meta.getAggregateMeasures();
        int size = aggregateMeasures.length + 1;

        if (isMdkeyRequiredInOutRow) {
            size += 1;
        }
        ValueMetaInterface[] out = new ValueMetaInterface[size];
        int l = 0;
        ValueMetaInterface valueMetaInterface = null;
        ValueMetaInterface storageMetaInterface = null;
        for (int i = 0; i < aggregateMeasures.length; i++) {
            valueMetaInterface = new ValueMeta(aggregateMeasures[i], ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            storageMetaInterface =
                    new ValueMeta(aggregateMeasures[i], ValueMetaInterface.TYPE_NUMBER,
                            ValueMetaInterface.STORAGE_TYPE_NORMAL);
            valueMetaInterface.setStorageMetadata(storageMetaInterface);
            out[l++] = valueMetaInterface;

        }
        valueMetaInterface = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        valueMetaInterface.setStorageMetadata((new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.STORAGE_TYPE_NORMAL)));
        valueMetaInterface.getStorageMetadata().setStringEncoding(BYTE_ENCODING);
        valueMetaInterface.setStringEncoding(BYTE_ENCODING);
        out[l++] = valueMetaInterface;
        if (isMdkeyRequiredInOutRow) {
            valueMetaInterface = new ValueMeta("factMdkey", ValueMetaInterface.TYPE_BINARY,
                    ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
            valueMetaInterface.setStorageMetadata(
                    (new ValueMeta("factMdkey", ValueMetaInterface.TYPE_STRING,
                            ValueMetaInterface.STORAGE_TYPE_NORMAL)));
            valueMetaInterface.setStringEncoding(BYTE_ENCODING);
            valueMetaInterface.setStringEncoding(BYTE_ENCODING);
            valueMetaInterface.getStorageMetadata().setStringEncoding(BYTE_ENCODING);
            out[l] = valueMetaInterface;
        }

        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonAggregateSurrogateGeneratorMeta) smi;
        data = (CarbonAggregateSurrogateGeneratorData) sdi;
        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonAggregateSurrogateGeneratorMeta) smi;
        data = (CarbonAggregateSurrogateGeneratorData) sdi;
        super.dispose(smi, sdi);
    }
}
