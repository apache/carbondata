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
package org.carbondata.processing.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.csvload.DataGraphExecuter;
import org.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.carbondata.processing.graphgenerator.GraphGenerator;
import org.carbondata.processing.graphgenerator.GraphGeneratorException;
import org.carbondata.processing.suggest.autoagg.util.CommonUtil;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * This class will create store file based on provided schema
 * @author Administrator
 */
public class StoreCreator {

  /**
   * Create store without any restructure
   */
  public static void createCarbonStore() {

    try {
      String metadataPath = "src/test/resources/schemas/default/carbon/metadata";
      String tableName = "carbon";
      String factFilePath = "src/test/resources/input/100.csv";
      String storeLocation = "src/test/resources/store";
      File storeDir = new File(storeLocation);
      CarbonUtil.deleteFoldersAndFiles(storeDir);

      String kettleHomePath = "carbonplugins";
      int currentRestructureNumber = 0;
      List<Schema> schemas = CommonUtil.readMetaData(metadataPath);
      LoadModel loadModel = new LoadModel();
      Schema schema = schemas.get(0);
      String partitionId = "0";
      schema.name = schema.name + "_" + partitionId;
      schema.cubes[0].name = schema.cubes[0].name + "_0";

      loadModel.setSchema(schemas.get(0));
      loadModel.setSchemaName(schema.name);
      loadModel.setCubeName(schema.cubes[0].name);
      loadModel.setTableName(tableName);
      loadModel.setFactFilePath(factFilePath);
      loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());

      executeGraph(loadModel, storeLocation, kettleHomePath, currentRestructureNumber);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Create store with resturcture
   */
  public static void createRestructureStore() {
    // TO-DO
    // add for restructure scenario, its not clear yet.
  }

  /**
   * Execute graph which will further load data
   * @param loadModel
   * @param storeLocation
   * @param kettleHomePath
   * @param currentRestructNumber
   * @throws Exception
   */
  public static void executeGraph(LoadModel loadModel, String storeLocation, String kettleHomePath,
      int currentRestructNumber) throws Exception {
    System.setProperty("KETTLE_HOME", kettleHomePath);
    new File(storeLocation).mkdirs();
    String outPutLoc = storeLocation + "/etl";
    String schemaName = loadModel.getSchemaName();
    String cubeName = loadModel.getCubeName();
    String tempLocationKey = schemaName + '_' + cubeName;
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");

    String tableName = loadModel.getTableName();
    String fileNamePrefix = "";
    String graphPath =
        outPutLoc + '/' + loadModel.getSchemaName() + '/' + loadModel.getCubeName() + '/'
            + tableName.replaceAll(",", "") + fileNamePrefix + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    DataProcessTaskStatus schmaModel = new DataProcessTaskStatus(schemaName, cubeName, tableName);
    schmaModel.setCsvFilePath(loadModel.getFactFilePath());
    SchemaInfo info = new SchemaInfo();

    info.setSchemaName(schemaName);
    info.setCubeName(cubeName);
   
    generateGraph(schmaModel, info, loadModel.getTableName(), "0", loadModel.getSchema(), null,
      currentRestructNumber, loadModel.getLoadMetadataDetails());

    DataGraphExecuter graphExecuter = new DataGraphExecuter(schmaModel);
    graphExecuter.executeGraph(graphPath, new ArrayList<String>(
        CarbonCommonConstants.CONSTANT_SIZE_TEN), info, "0", loadModel.getSchema());
  }

  /**
   * generate graph
   * @param schmaModel
   * @param info
   * @param tableName
   * @param partitionID
   * @param schema
   * @param factStoreLocation
   * @param currentRestructNumber
   * @param loadMetadataDetails
   * @throws GraphGeneratorException
   */
  private static void generateGraph(IDataProcessStatus schmaModel, SchemaInfo info,
      String tableName, String partitionID, Schema schema, String factStoreLocation,
      int currentRestructNumber, List<LoadMetadataDetails> loadMetadataDetails)
      throws GraphGeneratorException {
    DataLoadModel model = new DataLoadModel();
    model.setCsvLoad(null != schmaModel.getCsvFilePath() || null != schmaModel.getFilesToProcess());
    model.setSchemaInfo(info);
    model.setTableName(schmaModel.getTableName());
    if (null != loadMetadataDetails && !loadMetadataDetails.isEmpty()) {
      model.setLoadNames(CarbonDataProcessorUtil
          .getLoadNameFromLoadMetaDataDetails(loadMetadataDetails));
      model.setModificationOrDeletionTime(CarbonDataProcessorUtil
          .getModificationOrDeletionTimesFromLoadMetadataDetails(loadMetadataDetails));
    }
    boolean hdfsReadMode =
        schmaModel.getCsvFilePath() != null && schmaModel.getCsvFilePath().startsWith("hdfs:");
    int allocate = null != schmaModel.getCsvFilePath() ? 1 : schmaModel.getFilesToProcess().size();
    GraphGenerator generator =
        new GraphGenerator(model, hdfsReadMode, partitionID, schema, factStoreLocation,
            currentRestructNumber, allocate);
    generator.generateGraph();
  }

  /**
   * This is local model object used inside this class to store information related to data loading
   * @author Administrator
   *
   */
  static class LoadModel {
    
    private Schema schema;
    private String tableName;
    private String cubeName;
    private String schemaName;
    private List<LoadMetadataDetails> loadMetaDetail;
    private String factFilePath;

    public void setSchema(Schema schema) {
      this.schema = schema;
    }

    public List<LoadMetadataDetails> getLoadMetadataDetails() {
      return loadMetaDetail;
    }

    public Schema getSchema() {
      return schema;
    }

    public String getFactFilePath() {
      return factFilePath;
    }

    public String getTableName() {
      return tableName;
    }

    public String getCubeName() {
      return cubeName;
    }

    public String getSchemaName() {
      return schemaName;
    }

    public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetaDetail) {
      this.loadMetaDetail = loadMetaDetail;
    }

    public void setFactFilePath(String factFilePath) {
      this.factFilePath = factFilePath;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setCubeName(String cubeName) {
      this.cubeName = cubeName;
    }

    public void setSchemaName(String schemaName) {
      this.schemaName = schemaName;
    }

  }

}
