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

package org.carbondata.processing.suggest.datastats.util;

import java.io.*;
import java.util.List;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionFactory;
import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionService;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.datastats.model.DriverDistinctData;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.executer.impl.QueryExecutorImpl;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.scope.QueryScopeObject;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Utility
 *
 * @author A00902717
 */
public final class DataStatsUtil {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DataStatsUtil.class.getName());

    private DataStatsUtil() {

    }

    public static CarbonFile[] getCarbonFactFile(CarbonFile file, final String table) {
        CarbonFile[] files = file.listFiles(new CarbonFileFilter() {
            public boolean accept(CarbonFile pathname) {
                return (!pathname.isDirectory()) && pathname.getName().startsWith(table) && pathname
                        .getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
            }

        });
        return files;
    }

    public static DataType getDataType(Type type) {
        switch (type) {
        case INT:
            return DataType.IntegerType;
        case DOUBLE:
            return DataType.DoubleType;
        case LONG:
            return DataType.LongType;
        case STRING:
            return DataType.StringType;
        case BOOLEAN:
            return DataType.BooleanType;
        default:
            return DataType.IntegerType;
        }

    }

    /**
     * Get sample size
     *
     * @param dimension
     * @return
     */
    public static int getNumberOfRows(Dimension dimension) {
        int recCount = 10;
        String conRecCount = CarbonProperties.getInstance().getProperty(Preference.AGG_REC_COUNT);
        if (null != conRecCount && Integer.parseInt(conRecCount) < recCount) {
            recCount = Integer.parseInt(conRecCount);
        }
        return recCount;
    }

    public static boolean createDirectory(String path) {
        FileFactory.FileType fileType = FileFactory.getFileType(path);
        try {
            if (!FileFactory.isFileExist(path, fileType, false)) {
                if (!FileFactory.mkdirs(path, fileType)) {
                    return false;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static void serializeObject(Object object, String path, String fileName) {
        if (createDirectory(path)) {
            OutputStream out = null;
            ObjectOutputStream os = null;
            try {
                FileType fileType = FileFactory.getFileType(path);
                out = FileFactory.getDataOutputStream(path + File.separator + fileName, fileType);
                os = new ObjectOutputStream(out);
                os.writeObject(object);
            } catch (Exception e) {
                LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "Error in serializing file:" + path + '/' + fileName);
            } finally {
                CarbonUtil.closeStreams(out, os);
            }
        }
    }

    public static Object readSerializedFile(String path) {
        Object object = null;
        if (isFileExist(path)) {
            ObjectInputStream is = null;
            InputStream in = null;

            try {
                FileType fileType = FileFactory.getFileType(path);
                in = FileFactory.getDataInputStream(path, fileType);
                is = new ObjectInputStream(in);

                object = is.readObject();

            } catch (Exception e) {
                LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "Error in deserializing file:" + path);
            } finally {
                CarbonUtil.closeStreams(in, is);
            }
        }
        return object;
    }

    private static boolean isFileExist(String path) {
        FileFactory.FileType fileType = FileFactory.getFileType(path);

        try {
            return FileFactory.isFileExist(path, fileType, false);
        } catch (IOException e) {
            return false;
        }

    }

    public static QueryExecutor getQueryExecuter(Cube cube, String factTable,
            QueryScopeObject queryScopeObject) {
        QueryExecutor executer =
                new QueryExecutorImpl(cube.getDimensions(factTable), cube.getSchemaName(),
                        cube.getOnlyCubeName(), queryScopeObject);
        return executer;

    }

    public static QueryScopeObject createDataSource(CarbonDef.Schema schema, Cube cube,
            String partitionID, List<String> sliceLoadPaths, String factTableName, String dataPath,
            int restructureNo, long cubeCreationTime, LoadMetadataDetails[] loadMetadataDetails) {
        QueryScopeObject queryScopeObject = InMemoryTableStore.getInstance()
                .loadCube(schema, cube, partitionID, sliceLoadPaths, factTableName, dataPath,
                        restructureNo, cubeCreationTime, loadMetadataDetails);
        return queryScopeObject;

    }

    public static Level[] getDistinctDataFromDataStats(LoadModel loadModel)
            throws AggSuggestException {
        StringBuffer dataStatsPath = new StringBuffer(loadModel.getMetaDataPath());
        dataStatsPath.append(File.separator).append(Preference.AGGREGATE_STORE_DIR);
        // checking for distinct data if its already calculated
        String distinctDataPath =
                dataStatsPath.toString() + File.separator + Preference.DATASTATS_DISTINCT_FILE_NAME;
        DriverDistinctData driverDistinctData =
                (DriverDistinctData) DataStatsUtil.readSerializedFile(distinctDataPath);
        if (null == driverDistinctData) {
            AutoAggSuggestionService dataStatsService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
            dataStatsService.getAggregateDimensions(loadModel);
            driverDistinctData =
                    (DriverDistinctData) DataStatsUtil.readSerializedFile(distinctDataPath);
        }

        return driverDistinctData != null ? driverDistinctData.getLevels() : null;
    }

    /**
     * @param path
     * @param folderStsWith
     * @return
     */
    public static CarbonFile[] getRSFolderListList(LoadModel loadModel) {
        String basePath = loadModel.getDataPath();
        basePath = basePath + File.separator + loadModel.getSchemaName() + '_' + loadModel
                .getPartitionId() + File.separator + loadModel.getCubeName() + '_' + loadModel
                .getPartitionId();
        CarbonFile file = FileFactory.getCarbonFile(basePath, FileFactory.getFileType(basePath));
        CarbonFile[] files = null;
        if (file.isDirectory()) {
            files = file.listFiles(new CarbonFileFilter() {

                @Override
                public boolean accept(CarbonFile pathname) {
                    String name = pathname.getName();
                    return (pathname.isDirectory()) && name.startsWith("RS_");
                }
            });

        }
        return files;
    }

    /**
     * @param queryModel
     * @param listLoadFolders
     * @param cubeUniqueName
     * @return
     * @throws Exception
     */
    public static List<String> validateAndLoadRequiredSlicesInMemory(List<String> listLoadFolders,
            String cubeUniqueName, Set<String> columns) throws AggSuggestException {
        try {
            List<String> levelCacheKeys = InMemoryTableStore.getInstance()
                    .loadRequiredLevels(cubeUniqueName, columns, listLoadFolders);
            return levelCacheKeys;
        } catch (RuntimeException e) {
            throw new AggSuggestException("Failed to load level file.", e);
        }
    }

}
