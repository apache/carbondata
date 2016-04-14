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

package org.carbondata.processing.suggest.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TBase;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.format.TableInfo;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.query.querystats.Preference;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

public class TestUtil {

    public static CarbonDataLoadSchema readMetaData(String metaDataPath,String schemaName,String tableName) {
    	try{
    		FileType fileType=FileFactory.getFileType(metaDataPath);
        	CarbonProperties.getInstance().addProperty("carbon.storelocation", metaDataPath);
        	if (FileFactory.isFileExist(metaDataPath, fileType)) {
        	
        		ThriftReader.TBaseCreator createTBase=	new ThriftReader.TBaseCreator() {
                    public TBase create(){
                        return new TableInfo();
                    }
                 };
                 ThriftReader thriftReader= new ThriftReader(metaDataPath,  createTBase);
                 thriftReader.open();
                 TableInfo tableInfo=(TableInfo)thriftReader.read();
                 thriftReader.close();
                 SchemaConverter converter=new ThriftWrapperSchemaConverterImpl();
                 org.carbondata.core.carbon.metadata.schema.table.TableInfo  wrappedTableInfo=converter.fromExternalToWrapperTableInfo(tableInfo, schemaName, tableName);
                 org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance().loadTableMetadata(wrappedTableInfo);
                 CarbonDataLoadSchema carbonDataLoadSchema=new CarbonDataLoadSchema(org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance().getCarbonTable(tableName));
                 
                 return carbonDataLoadSchema;
        	}
    	}catch(Exception e){
    		
    	}
    	
    	return null;
    }

    public static Schema parseStringToSchema(String schema) throws XOMException {

        Parser xmlParser = XOMUtil.createDefaultParser();
        ByteArrayInputStream baoi = new ByteArrayInputStream(schema.getBytes());
        DOMWrapper defin = xmlParser.parse(baoi);
        return new Schema(defin);
    }

    public static String getDistinctDataPath(LoadModel loadModel) {
        StringBuffer dataStatsPath = new StringBuffer(loadModel.getMetaDataPath());
        dataStatsPath.append(File.separator).append(Preference.AGGREGATE_STORE_DIR)
                .append(File.separator).append(Preference.DATASTATS_DISTINCT_FILE_NAME);
        return dataStatsPath.toString();
    }

    public static String getDataStatsAggCombination(String schemaName, String cubeName,
            String tableName) {
        StringBuffer dataStatsPath =
                new StringBuffer(CarbonUtil.getCarbonStorePath(schemaName, cubeName)/*CarbonProperties
                .getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)*/);
        dataStatsPath.append(File.separator).append(Preference.AGGREGATE_STORE_DIR)
                .append(File.separator).append(schemaName).append(File.separator).append(cubeName)
                .append(File.separator).append(tableName).append(File.separator)
                .append(Preference.DATA_STATS_FILE_NAME);
        return dataStatsPath.toString();
    }

    public static LoadModel createLoadModel(String schemaName, String cubeName, Schema schema,
            Cube cube, String dataPath, String metaPath) {
        LoadModel loadModel = new LoadModel();
        loadModel.setSchema(schema);
        loadModel.setCube(cube);
        loadModel.setSchemaName(schemaName);
        loadModel.setCubeName(cubeName);
        loadModel.setTableName(cube.fact.getAlias());
        loadModel.setDataPath(dataPath);
        loadModel.setMetaDataPath(metaPath);
        loadModel.setPartitionId("0");
        loadModel.setMetaCube(null);
        loadModel.getMetaCube();
        List<String> validLoad = new ArrayList<String>();
        validLoad.add("Load_0");
        loadModel.setValidSlices(validLoad);
        loadModel.setCubeCreationtime(System.currentTimeMillis());
        return loadModel;
    }

}