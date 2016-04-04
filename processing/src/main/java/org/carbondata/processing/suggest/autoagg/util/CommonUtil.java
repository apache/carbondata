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

package org.carbondata.processing.suggest.autoagg.util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

/**
 * Util class used by other bundle
 *
 * @author A00902717
 */
public final class CommonUtil {
    private static final String LOAD_NAME = "Load_";
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CommonUtil.class.getName());

    private CommonUtil() {
        // TODO Auto-generated constructor stub
    }

    public static void setListOfValidSlices(String metaPath, LoadModel loadModel) {
        String loadMetaPath = metaPath + File.separator + CarbonCommonConstants.LOADMETADATA_FILENAME
                + CarbonCommonConstants.CARBON_METADATA_EXTENSION;
        List<String> listOfValidSlices = new ArrayList<String>(10);
        DataInputStream dataInputStream = null;
        Gson gsonObjectToRead = new Gson();

        AtomicFileOperations fileOperation =
                new AtomicFileOperationsImpl(loadMetaPath, FileFactory.getFileType(loadMetaPath));

        try {
            if (FileFactory.isFileExist(loadMetaPath, FileFactory.getFileType(loadMetaPath))) {

                dataInputStream = fileOperation.openForRead();

				/*dataInputStream = FileFactory.getDataInputStream(
                        loadMetaPath,
						FileFactory.getFileType(loadMetaPath));
*/
                BufferedReader buffReader =
                        new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

                LoadMetadataDetails[] loadFolderDetailsArray =
                        gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
                List<String> listOfValidUpdatedSlices = new ArrayList<String>(10);
                // just directly iterate Array
                List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

                //this will have all load except failed load
                List<String> allLoads = new ArrayList<String>(10);

                for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
                    if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
                            || CarbonCommonConstants.MARKED_FOR_UPDATE
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
                            || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
                            .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                        if (CarbonCommonConstants.MARKED_FOR_UPDATE
                                .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {

                            listOfValidUpdatedSlices.add(loadMetadataDetails.getLoadName());
                        }
                        listOfValidSlices.add(LOAD_NAME + loadMetadataDetails.getLoadName());

                    }

                    if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                            .equals(loadMetadataDetails.getLoadStatus())) {
                        allLoads.add(LOAD_NAME + loadMetadataDetails.getLoadName());
                    }
                }
                loadModel.setValidUpdateSlices(listOfValidUpdatedSlices);
                loadModel.setAllLoads(allLoads);
                loadModel.setLoadMetadataDetails(loadFolderDetailsArray);
            }

        } catch (IOException e) {
            LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "IO Exception @: " + e.getMessage());
        } finally {
            CarbonUtil.closeStreams(dataInputStream);
        }
        loadModel.setValidSlices(listOfValidSlices);

    }

    /**
     * read metadata
     *
     * @param metaDataPath
     * @return
     */
    public static List<Schema> readMetaData(String metaDataPath) {
        if (null == metaDataPath) {
            return null;
        }
        FileType fileType = FileFactory.getFileType(metaDataPath);
        CarbonFile metaDataFile = FileFactory.getCarbonFile(metaDataPath, fileType);
        if (!metaDataFile.exists()) {
            return null;
        }

        List<Schema> list = new ArrayList<Schema>();
        DataInputStream in = null;
        try {

            in = FileFactory.getDataInputStream(metaDataPath, fileType);
            int len = in.readInt();
            while (len > 0) {
                byte[] schemaNameBytes = new byte[len];
                in.readFully(schemaNameBytes);

                //String schemaName = new String(schemaNameBytes, "UTF8");
                int cubeNameLen = in.readInt();
                byte[] cubeNameBytes = new byte[cubeNameLen];
                in.readFully(cubeNameBytes);
                //String cubeName = new String(cubeNameBytes, "UTF8");
                int dataPathLen = in.readInt();
                byte[] dataPathBytes = new byte[dataPathLen];
                in.readFully(dataPathBytes);
                //String dataPath = new String(dataPathBytes, "UTF8");

                int versionLength = in.readInt();
                byte[] versionBytes = new byte[versionLength];
                in.readFully(versionBytes);
                //String version = new String(versionBytes,"UTF8");

                int schemaLen = in.readInt();
                byte[] schemaBytes = new byte[schemaLen];
                in.readFully(schemaBytes);

                String schema = new String(schemaBytes, "UTF8");
                int partitionLength = in.readInt();
                byte[] partitionBytes = new byte[partitionLength];
                in.readFully(partitionBytes);

                Schema mondSchema = parseStringToSchema(schema);

                list.add(mondSchema);
                try {
                    in.readLong();
                    len = in.readInt();
                } catch (EOFException eof) {
                    len = 0;
                }

            }

            return list;
        } catch (IOException e) {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        } catch (XOMException e) {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
        } finally {
            CarbonUtil.closeStreams(in);
        }

        return null;
    }

    public static Schema parseStringToSchema(String schema) throws XOMException {

        Parser xmlParser = XOMUtil.createDefaultParser();
        ByteArrayInputStream baoi = new ByteArrayInputStream(schema.getBytes());
        DOMWrapper defin = xmlParser.parse(baoi);
        return new CarbonDef.Schema(defin);
    }

    public static void fillSchemaAndCubeDetail(LoadModel loadModel) {
        String dataStatsPath = loadModel.getMetaDataPath();
        List<Schema> schemas = readMetaData(dataStatsPath + File.separator + "metadata");
        if (null != schemas) {
            Schema schema = schemas.get(0);
            loadModel.setSchema(schema);
            CarbonDef.Cube[] cubes = schema.cubes;
            for (CarbonDef.Cube cube : cubes) {
                if (loadModel.getCubeName().equalsIgnoreCase(cube.name)) {
                    loadModel.setCube(cube);
                }
            }

        }

    }

    public static boolean isLoadDeleted(List<String> validSlices, List<String> calculatedLoads) {
        for (String calculatedLoad : calculatedLoads) {
            if (!validSlices.contains(calculatedLoad)) {
                return true;
            }
        }
        return false;
    }

}
