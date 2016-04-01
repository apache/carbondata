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

package org.carbondata.processing.autoaggregategraphgenerator.step;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.graphgenerator.*;
import org.carbondata.processing.schema.metadata.AggregateTable;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class CarbonAutoAGGGraphGeneratorStep extends BaseStep implements StepInterface {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonAutoAGGGraphGeneratorStep.class.getName());

    /**
     * meta
     */
    private CarbonAutoAGGGraphGeneratorMeta meta;

    /**
     * data
     */
    private CarbonAutoAGGGraphGeneratorData data;

    /**
     * writeCounter
     */
    private long writeCounter;

    /**
     * generator
     */
    private GraphGenerator generator;

    /**
     * aggTableQueue
     */
    private Deque<AggTableInfo> aggTableQueue;

    /**
     * CarbonAutoAGGGraphGeneratorStep Constructor to initialise the step
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public CarbonAutoAGGGraphGeneratorStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
            int copyNr, TransMeta transMeta, Trans trans) {
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
            Object[] row = getRow();
            if (first) {
                meta = (CarbonAutoAGGGraphGeneratorMeta) smi;
                data = (CarbonAutoAGGGraphGeneratorData) sdi;
                if (null != getInputRowMeta()) {
                    this.data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
                    this.meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
                }
                first = false;
                setStepConfiguration();
            }
            if (null == row) {
                String generateGraph = null;
                AggTableInfo poll = aggTableQueue.poll();
                generateGraph = generator
                        .generateGraph(meta.getFactTableName(), meta.getFactStoreLocation(),
                                generator.getAllAggTables(), true, meta.getAggTables());

                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Loaded From Table: " + poll.factTableName + " : " + meta.getAggTables());
                executeAggregateGeneration(generateGraph);
                //				}

                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Record Procerssed For Auto Aggregate Table: ");
                String logMessage =
                        "Summary: Carbon Fact Reader Step: Read: " + 1 + ": Write: " + writeCounter;
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
                putRow(data.outputRowMeta, new Object[0]);
                setOutputDone();
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        putRow(data.outputRowMeta, new Object[0]);
        return true;
    }

    private void getAllAggregateTableDetails(AggregateTable[] aggTables, Cube cube) {
        List<AggregateTable> copyOfaggregateTable =
                new ArrayList<AggregateTable>(Arrays.asList(aggTables));
        Collections.sort(copyOfaggregateTable, new AggregateTableComparator());
        AggregateTableSelecter tableSelecter = new AutoAggregateTableSelecter(copyOfaggregateTable);
        tableSelecter.selectTableForAggTableAggregationProcess(aggTables, cube);

        AggregateTableDerivative aggregateTableDerivativeMetadata =
                tableSelecter.getAggregateTableDerivativeInstanceForAggEval();
        List<AggregateTableDerivative> listOfChildAggregateTableDerivativeMetadata =
                new ArrayList<AggregateTableDerivative>(10);
        listOfChildAggregateTableDerivativeMetadata.add(aggregateTableDerivativeMetadata);
        aggTableQueue = new ArrayDeque<AggTableInfo>();

        evaluateAggregateTableDerivativeAndGenerateGraphs(
                listOfChildAggregateTableDerivativeMetadata,
                CarbonSchemaParser.getFactTableName(cube), aggTableQueue);

    }

    private void evaluateAggregateTableDerivativeAndGenerateGraphs(
            List<AggregateTableDerivative> listOfaggregateTableDerivativeMetadata,
            String factTableName, Deque<AggTableInfo> aggInfoQueue) {
        List<String> tableNamesForEvaluation = null;
        List<AggregateTable> tableInstancesForEvaluation = null;
        List<AggregateTableDerivative> listOfChildAggregateTableDerivativeMetadata = null;
        Iterator<AggregateTableDerivative> itrMetadata =
                listOfaggregateTableDerivativeMetadata.iterator();
        while (itrMetadata.hasNext()) {
            AggregateTableDerivative aggregateTableDerivativeMetadata = itrMetadata.next();
            if (!aggregateTableDerivativeMetadata.getChildrens().isEmpty()) {
                listOfChildAggregateTableDerivativeMetadata =
                        new ArrayList<AggregateTableDerivative>(10);
                if (aggregateTableDerivativeMetadata instanceof AggregateTableDerivativeComposite) {
                    AggregateTableDerivative aggTableDer = null;
                    Iterator<AggregateTableDerivative> itr =
                            aggregateTableDerivativeMetadata.getChildrens().iterator();
                    tableNamesForEvaluation = new ArrayList<String>(10);
                    tableInstancesForEvaluation = new ArrayList<AggregateTable>(10);
                    while (itr.hasNext()) {
                        aggTableDer = itr.next();
                        if (null != aggTableDer.getAggregateTable()) {
                            tableNamesForEvaluation
                                    .add(aggTableDer.getAggregateTable().getAggregateTableName());
                            tableInstancesForEvaluation.add(aggTableDer.getAggregateTable());
                        }
                        listOfChildAggregateTableDerivativeMetadata.add(aggTableDer);

                    }
                    if (null != aggregateTableDerivativeMetadata.getAggregateTable()) {

                        AggTableInfo aggTableInfo = new AggTableInfo();
                        aggTableInfo.factTableName =
                                aggregateTableDerivativeMetadata.getAggregateTable()
                                        .getAggregateTableName();
                        aggInfoQueue.offer(aggTableInfo);

                    } else {

                        AggTableInfo aggTableInfo = new AggTableInfo();
                        aggTableInfo.factTableName = factTableName;
                        aggInfoQueue.offer(aggTableInfo);
                    }
                    if (!listOfChildAggregateTableDerivativeMetadata.isEmpty()) {
                        evaluateAggregateTableDerivativeAndGenerateGraphs(
                                listOfChildAggregateTableDerivativeMetadata, factTableName,
                                aggInfoQueue);
                    }

                }
            }
        }

    }

    /**
     * Below method will be used to set the step configuration
     *
     * @throws Exception
     */
    private void setStepConfiguration() throws Exception {
        DataLoadModel model = new DataLoadModel();
        SchemaInfo info = new SchemaInfo();
        info.setSchemaName(meta.getSchemaName());
        info.setCubeName(meta.getCubeName());
        info.setSchemaPath(null);
        info.setAutoAggregateRequest(true);
        model.setSchemaInfo(info);
        model.setTableName(meta.getAggTables());
        model.setLoadNames(meta.getLoadNames());
        model.setModificationOrDeletionTime(meta.getModificationOrDeletionTime());
        generator = new GraphGenerator(model, meta.isHDFSMode(), meta.getPartitionId(),
                parseStringToSchema(meta.getSchema()), meta.getFactStoreLocation(),
                meta.getCurrentRestructNumber(), 1);
        getAllAggregateTableDetails(generator.getAllAggTables(), generator.getCube());
    }

    private Schema parseStringToSchema(String schema) throws Exception {
        Parser xmlParser = XOMUtil.createDefaultParser();
        ByteArrayInputStream baoi =
                new ByteArrayInputStream(schema.getBytes(Charset.defaultCharset()));
        return new CarbonDef.Schema(xmlParser.parse(baoi));
    }

    /**
     * Below method will be used to to run aggregate table graph
     *
     * @param graphFilePath
     * @throws KettleException
     */
    private void executeAggregateGeneration(String graphFilePath) throws KettleException {
        Trans trans = null;

        TransMeta transMeta = null;
        try {
            transMeta = new TransMeta(graphFilePath);
        } catch (KettleXMLException e) {
            throw new KettleException("Problem while creating the trans", e);
        }
        transMeta.setFilename(graphFilePath);
        trans = new Trans(transMeta);
        try {
            trans.execute(null);
        } catch (KettleException e) {
            throw new KettleException(
                    "Problem while running the auto aggregate graph For Auto aggregation", e);
        }
        trans.waitUntilFinished();
        if ((trans.getErrors() > 0) && (trans.isStopped())) {
            throw new KettleException(
                    "Problem while running the auto aggregate graph For Auto aggregation");
        }
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonAutoAGGGraphGeneratorMeta) smi;
        data = (CarbonAutoAGGGraphGeneratorData) sdi;
        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonAutoAGGGraphGeneratorMeta) smi;
        data = (CarbonAutoAGGGraphGeneratorData) sdi;
        super.dispose(smi, sdi);
    }

    private class AggTableInfo {
        private String factTableName;
    }

}
