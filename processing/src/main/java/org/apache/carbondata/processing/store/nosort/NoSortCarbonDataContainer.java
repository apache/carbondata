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
package org.apache.carbondata.processing.store.nosort;

import java.math.BigDecimal;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.NewFlowDataLoadUtil;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.parser.RowParser;
import org.apache.carbondata.processing.newflow.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class NoSortCarbonDataContainer {

  private static final LogService LOGGER = LogServiceFactory.getLogService(NoSortCarbonDataContainer
      .class.getName());

  private CarbonLoadModel loadModel;
  private String storeLocation;
  private CarbonDataLoadConfiguration configuration;
  private RowParser rowParser;
  private RowConverter converter;
  private CarbonFactHandler dataHandler;
  private CarbonFactDataHandlerModel dataHandlerModel;
  private int dimensionCount;
  private int dictDimensionCount;
  private int noDictionaryCount;
  boolean[] noDictionaryMapping;
  private int complexDimensionCount;
  private int measureCount;
  private SegmentProperties segmentProperties;
  private KeyGenerator keyGenerator;
  private int complexPosition;

  public NoSortCarbonDataContainer(CarbonLoadModel loadModel, String storeLocation) {
    this.loadModel = loadModel;
    this.storeLocation = storeLocation;
  }

  public void initialise() throws Exception {
    CarbonMetadata.getInstance().addCarbonTable(loadModel.getCarbonDataLoadSchema()
        .getCarbonTable());
    configuration = NewFlowDataLoadUtil.createConfiguration(loadModel, storeLocation);
    rowParser = new RowParserImpl(configuration.getDataFields(), configuration);
    BadRecordsLogger badRecordLogger = NewFlowDataLoadUtil.createBadRecordLogger(configuration);
    converter = new RowConverterImpl(configuration.getDataFields(), configuration, badRecordLogger);
    converter.initialize();

    dataHandlerModel =
        CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(configuration, storeLocation);
    dictDimensionCount = dataHandlerModel.getDimensionCount();
    noDictionaryCount = dataHandlerModel.getNoDictionaryCount();
    noDictionaryMapping = CarbonDataProcessorUtil.getNoDictionaryMapping(configuration
        .getDataFields());
    complexDimensionCount = configuration.getComplexDimensionCount();
    dimensionCount = configuration.getDimensionCount();
    complexPosition = dimensionCount - complexDimensionCount;
    measureCount = dataHandlerModel.getMeasureCount();
    segmentProperties = dataHandlerModel.getSegmentProperties();
    keyGenerator = segmentProperties.getDimensionKeyGenerator();

    dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(dataHandlerModel,
        CarbonFactHandlerFactory.FactHandlerType.NOSORT);
    dataHandler.initialise();

  }

  public void addRow(String[] data) {
    CarbonRow row = new CarbonRow(rowParser.parseRow(data));
    row.setOrigin(data);
    converter.convert(row);

    Object[] outputRow;
    // adding one for the high cardinality dims byte array.
    if (noDictionaryCount > 0 || complexDimensionCount > 0) {
      outputRow = new Object[measureCount + 1 + 1];
    } else {
      outputRow = new Object[measureCount + 1];
    }

    // convert row to 3-elements row
    // the input row is dimension(N), measure(N)
    // the outputRow: measure(N object), noDictAndComplexColumn(1 byte[][]), dict(1 byte[], mdk)
    // fill measure(N object)

    int columnIndex = 0;
    for (; columnIndex < measureCount; columnIndex++) {
      Object value = row.getObject(columnIndex + dimensionCount);
      if (value instanceof BigDecimal) {
        outputRow[columnIndex] = DataTypeUtil.bigDecimalToByte((BigDecimal) value);
      } else {
        outputRow[columnIndex] = value;
      }

    }
    // no dictionary + complex column data
    byte[][] noDictAndComplexData = new byte[noDictionaryCount + complexDimensionCount][];
    // dictionary data
    int[] dictData = new int[dictDimensionCount];
    outputRow[columnIndex++] = noDictAndComplexData;
    int noDictcount = 0;
    int dictCount = 0;
    for (int i = 0; i < noDictionaryMapping.length; i++) {
      if (noDictionaryMapping[i]) {
        noDictAndComplexData[noDictcount++] = row.getBinary(i);
      } else {
        dictData[dictCount++] = row.getInt(i);
      }
    }
    for (int i = 0; i < complexDimensionCount; i++) {
      noDictAndComplexData[noDictcount++] = row.getBinary(complexPosition + i);
    }
    // generate MDK
    try {
      outputRow[outputRow.length - 1] = keyGenerator.generateKey(dictData);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("unable to generate the mdkey", e);
    }
    dataHandler.addDataToStore(outputRow);
  }

  public void finish() {
    if (converter != null) {
      converter.finish();
    }
    if (dataHandler != null) {
      try {
        dataHandler.finish();
      } catch (Exception e) {
        LOGGER.error(e, "Failed for table: " + loadModel.getTableName()
            + " in  finishing data handler");
      } finally {
        dataHandler.closeHandler();
      }
    }

  }


}
