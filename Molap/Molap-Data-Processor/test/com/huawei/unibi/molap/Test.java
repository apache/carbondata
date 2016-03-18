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

package com.huawei.unibi.molap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Schema;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvload.DataGraphExecuter;
import com.huawei.unibi.molap.graphgenerator.GraphGenerator;
import com.huawei.unibi.molap.graphgenerator.GraphGeneratorException;
import com.huawei.unibi.molap.graphgenerator.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.dataprocessor.IDataProcessStatus;
import com.huawei.unibi.molap.api.dataloader.DataLoadModel;
import com.huawei.unibi.molap.api.dataloader.SchemaInfo;
import com.huawei.unibi.molap.dataprocessor.DataProcessTaskStatus;

public class Test
{
    private static void generateGraph(IDataProcessStatus schmaModel,
            SchemaInfo info, String tableName, String partitionID, Schema schema)
            throws GraphGeneratorException
    {
        DataLoadModel model = new DataLoadModel();
        model.setCsvLoad(null != schmaModel.getCsvFilePath());
        model.setSchemaInfo(info);
        model.setTableName(schmaModel.getTableName());
        boolean hdfsReadMode = schmaModel.getCsvFilePath() != null
                && schmaModel.getCsvFilePath().startsWith("hdfs:");
        GraphGenerator generator = new GraphGenerator(model, hdfsReadMode,
                partitionID, schema);
        generator.generateGraph();
    }

    public static void main123(String[] args) throws IOException
    {
//        int x = -8 >>> 1;
//        System.out.println(x);
//        long sum = 0;
//        for(int i = 2;i < 32 - 1;i++)
//        {
//            sum += Math.pow(2, i);
//        }
//        
//        System.out.println(sum);
//        sum=0;
//        for(int i = 0;i < 32;i++)
//        {
//            sum += Math.pow(2, i);
//        }
//        
//        //
//        System.out.println(sum);
        
        BufferedReader reader = new BufferedReader(new FileReader("I:\\vmall\\columnnames.txt"));
        BufferedReader reader1 = new BufferedReader(new FileReader("I:\\vmall\\cardinality.txt"));
        
        List<String> columnsList = new ArrayList<String>();
        List<String> cardinalityList = new ArrayList<String>();
        String s = "";
        while((s=reader.readLine())!=null)
        {
            columnsList.add(s);
        }
        
        while((s=reader1.readLine())!=null)
        {
            cardinalityList.add(s.split("\\(")[1].replace(")", ""));
        }
        
        System.out.println(cardinalityList);
        String schemaPath = "F:\\vmall\\vishalschema.xml";
        Schema loadXML = MolapSchemaParser.loadXML(schemaPath);
        
        Cube mondrianCube = MolapSchemaParser.getMondrianCube(loadXML, "VmallCube");
//        
        CubeDimension[] dimensions = mondrianCube.dimensions;
//        
//        for(int i = 0;i < columnsList.size();i++)
//        {
//            dimensions[0].name=columnsList.get(i);
//             Hierarchy hierarchy = MolapSchemaParser.extractHierarchies(loadXML, dimensions[0])[0];
//             hierarchy.name=columnsList.get(i);
//             hierarchy.caption=null;
//             hierarchy.levels[0].name=columnsList.get(i);
//             hierarchy.levels[0].caption=null;
//             hierarchy.levels[0].column=columnsList.get(i);
//             hierarchy.levels[0].caption=null;
//             hierarchy.levels[0].type="String";
//             hierarchy.levels[0].levelCardinality=Integer.parseInt(cardinalityList.get(i));
//             System.out.println(dimensions[0].toXML());
//        }
//      
       System.out.println(MolapSchemaParser.getKeyGeneratorForFact(dimensions, loadXML).getKeySizeInBytes());

    }

    public static void main(String[] args) throws Exception
    {

        // String schemaPath="/opt/vishalcolumnar/Network_Audit_Gn.xml";
        // String kettleHome="/opt/vishalcolumnar/molapplugins/molapplugins";
        // String storeLocation="/opt/vishalcolumnar/backupcolumnar/";
        // String csvFilePath="/home/rawcsv/fact_gn_info_31day/32";
        // String
        // csvFilePath="hdfs://10.19.92.151:54310/opt/naresh/itr2/partitiondata/";

        // String schemaPath="F:/SmartCare_en.xml";
        // String
        // kettleHome="F:\\TRP\\Molap\\Molap-Data-Processor_vishal/molapplugins/molapplugins";
        // String storeLocation="D:/molap";
        // String
        // csvFilePath="F:/SDR_PCC_USER_ALL_INFO_1DAY__31-01-2015_20150430162243.csv";
        // String dimFiles="F:\\testing\\dimensions";

        // String schemaPath="D:/Network_Audit_Gn.xml";
        // String
        // kettleHome="F:\\TRP\\Molap\\Molap-Data-Processor_vishal/molapplugins/molapplugins";
        // String storeLocation="D:/molap";
        // String csvFilePath="I:\\file";
        // String
        // csvFilePath="hdfs://10.19.92.151:54310/opt/vishalcsv/sample.csv";
        // String dimFiles="F:\\testing\\dimensions";

//        String schemaPath = "F:\\testing\\SmartCare_ec.xml";
//        String kettleHome = "F:\\TRP\\Molap\\Molap-Data-Processor_vishal/molapplugins/molapplugins";
//        String storeLocation = "D:/molap";
//        String csvFilePath = "F:\\testing\\sample1.csv";
//        String dimFiles = "F:\\testing\\dimensions";
        
        String schemaPath = "F:\\vmall\\vishalschema.xml";
        String kettleHome = "F:\\TRP\\Molap\\Molap-Data-Processor_vishal/molapplugins/molapplugins";
        String storeLocation = "D:/molap";
        String csvFilePath = "F:\\vmall\\data.csv";

        
//        String schemaPath = "F:\\ramana\\SmartPCC_1day_agg_en.xml";
//      String kettleHome = "F:\\TRP\\Molap\\Molap-Data-Processor_vishal/molapplugins/molapplugins";
//      String storeLocation = "D:/molap";
//      String csvFilePath = "F:\\ramana\\SDR_PCC_USER_ALL_INFO_1DAY__31-01-2015_0_1000rows.csv";
//      String dimFiles = "F:\\ramana\\dimensions";
        
        Schema loadXML = MolapSchemaParser.loadXML(schemaPath);

        System.out.println(MolapSchemaParser.getKeyGeneratorForFact(
                MolapSchemaParser.getMondrianCubes(loadXML)[0].dimensions,
                loadXML).getKeySizeInBytes());
        long currentTimeMillis = System.currentTimeMillis();
        System.setProperty("KETTLE_HOME", kettleHome);
        MolapLoadModel loadModel = new MolapLoadModel();
        loadModel.setSchemaPath(schemaPath);
        loadModel.setSchema(loadXML);
        loadModel.setSchemaName(loadXML.name);
        loadModel
                .setCubeName(MolapSchemaParser.getMondrianCubes(loadXML)[0].name);
        loadModel
                .setTableName(MolapSchemaParser
                        .getFactTableName(MolapSchemaParser
                                .getMondrianCubes(loadXML)[0]));
        // loadModel.setTableName("agg_1_FACT_IU_DATA_INFO");
        loadModel.setFactFilePath(csvFilePath);
//        loadModel.setDimFolderPath(dimFiles);
        executeGraph(loadModel, storeLocation, "", kettleHome);

        System.out
                .println("***************************************Time take for data loading: "
                        + (System.currentTimeMillis() - currentTimeMillis));

    }

    public static void executeGraph(MolapLoadModel loadModel,
            String storeLocation, String hdfsStoreLocation,
            String kettleHomePath) throws Exception
    {
        MolapProperties.getInstance().addProperty("store_output_location",
                storeLocation);
        System.setProperty("KETTLE_HOME", kettleHomePath);
        String output_loc = storeLocation + "/etl";
        MolapProperties.getInstance().addProperty(
                MolapCommonConstants.STORE_LOCATION, storeLocation);
        MolapProperties.getInstance().addProperty(
                MolapCommonConstants.STORE_LOCATION_HDFS, hdfsStoreLocation);
        MolapProperties.getInstance().addProperty("store_output_location",
                output_loc);
        MolapProperties.getInstance().addProperty("send.signal.load", "false");
        MolapProperties.getInstance().addProperty(
                "molap.dimension.split.value.in.columnar", "1");
        MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits",
                "true");
        MolapProperties.getInstance().addProperty("is.int.based.indexer",
                "true");
        MolapProperties.getInstance().addProperty(
                "aggregate.columnar.keyblock", "true");
        MolapProperties.getInstance().addProperty("high.cardinality.value",
                "100000");
        MolapProperties.getInstance().addProperty("molap.is.columnar.storage",
                "true");
         MolapProperties.getInstance().addProperty("molap.leaf.node.size",
         "299");
         MolapProperties.getInstance().addProperty("is.compressed.keyblock",
         "false");

        String graphPath = output_loc + "/" + loadModel.getSchemaName() + "/"
                + loadModel.getCubeName() + "/" + loadModel.getTableName()
                + ".ktr";
        File path = new File(graphPath);
        if(path.exists())
        {
            path.delete();
        }
        String schemaName = loadModel.getSchemaName();
        String cubeName = loadModel.getCubeName();
        DataProcessTaskStatus schmaModel = new DataProcessTaskStatus(
                schemaName, cubeName, loadModel.getTableName());
        schmaModel.setCsvFilePath(loadModel.getFactFilePath());
        schmaModel.setDimCSVDirLoc(loadModel.getDimFolderPath());
        
        SchemaInfo info = getSchemaInfo(loadModel.getSchemaName(),
                loadModel.getCubeName(), loadModel.getSchemaPath());
        info.setCubeName(cubeName);
        info.setSchemaPath(loadModel.getSchemaPath());
        
        generateGraph(schmaModel, info, loadModel.getTableName(),
                loadModel.getPartitionId(), loadModel.getSchema());
        DataGraphExecuter graphExecuter = new DataGraphExecuter(schmaModel);

        graphExecuter.executeGraph(graphPath, new ArrayList<String>(), info,
                loadModel.getPartitionId(), loadModel.getSchema());
    }

    private static SchemaInfo getSchemaInfo(String schemaName, String cubeName,
            String schemaPath)
    {
        SchemaInfo info = new SchemaInfo();
        info.setCubeName(cubeName);
        info.setSchemaName(schemaName);
        info.setSchemaPath(schemaPath);
        info.setSrcConUrl("jdbc:mysql://10.19.92.37:3999/" + "vishal");
        info.setSrcUserName("vishal");
        info.setSrcPwd("vishal");
        info.setSrcDriverName("com.mysql.jdbc.Driver");
        // info.setAutoAggregateRequest(true);
        return info;
    }

    public static void main1(String[] args) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader("D:\\a.csv"));

        String s = "";
        int counter = 0;
        while((s = reader.readLine()) != null)
        {
            counter++;
        }
        System.out.println(counter);
    }
}
