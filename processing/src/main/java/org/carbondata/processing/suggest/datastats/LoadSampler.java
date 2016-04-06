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

package org.carbondata.processing.suggest.datastats;

import java.io.File;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.datastats.load.FactDataHandler;
import org.carbondata.processing.suggest.datastats.load.FactDataReader;
import org.carbondata.processing.suggest.datastats.load.LoadHandler;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.datastats.util.DataStatsUtil;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.datastorage.MemberStore;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.scope.QueryScopeObject;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * This class have all loads for which data sampling needs to be done
 * also it delegates the task of loading configured loads to BPlus tree
 *
 * @author A00902717
 */
public class LoadSampler {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LoadSampler.class.getName());

    /**
     * folder name where carbon data writer will write
     */
    //private static final String RS_FOLDER_NAME = "RS_";

    private List<LoadHandler> loadHandlers;

    private String tableName;

    private String schemaName;

    private String cubeName;

    private Cube metaCube;

    private Schema schema;

    private String cubeUniqueName;

    private List<String> allLoads;
    /**
     * Instance of query scope object holding the segment cache and map of segment name as key and
     * modification time as value.
     */
    private QueryScopeObject queryScopeObject;

    /**
     * This will have dimension ordinal as key and dimension cardinality as value
     */
    private Map<Integer, Integer> dimCardinality = new HashMap<>();
    ;

    /**
     * Dimension present in cube.
     */
    private List<Dimension> visibleDimensions;

    /**
     * loading stores
     *
     * @param loadModel
     */
    public void loadCube(LoadModel loadModel) {
        this.schema = loadModel.getSchema();
        this.allLoads = loadModel.getAllLoads();
        CarbonDef.Cube cube = loadModel.getCube();

        String partitionId = loadModel.getPartitionId();
        if (null != partitionId) {
            schemaName = loadModel.getSchemaName() + '_' + partitionId;
            cubeName = loadModel.getCubeName() + '_' + partitionId;
        }
        this.tableName = loadModel.getTableName();

        this.cubeUniqueName = schemaName + '_' + cubeName;

        loadHandlers = new ArrayList<LoadHandler>();
        // Load data in memory
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Loading data to BPlus tree started");
        metaCube = InMemoryTableStore.getInstance()
                .loadCubeMetadataIfRequired(schema, cube, partitionId,
                        loadModel.getSchemaLastUpdatedTime());
        this.queryScopeObject = DataStatsUtil
                .createDataSource(schema, metaCube, partitionId, loadModel.getAllLoads(), tableName,
                        loadModel.getDataPath(), loadModel.getRestructureNo(),
                        loadModel.getCubeCreationtime(), loadModel.getLoadMetadataDetails());
        //set visible dimensions
        this.visibleDimensions = getVisibleDimensions(cube);
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Loading data to BPlus tree completed");

        //int restructureNo=Integer.parseInt(rsFolder.getName().substring(rsFolder.getName().indexOf('_')+1));
        CarbonFile[] rsFolders = DataStatsUtil.getRSFolderListList(loadModel);

        for (CarbonFile rsFolder : rsFolders) {
            CarbonFile tableFile = getTableFile(rsFolder, tableName);
            SliceMetaData sliceMetaData = (SliceMetaData) DataStatsUtil.readSerializedFile(
                    tableFile.getAbsolutePath() + File.separator + CarbonUtil
                            .getSliceMetaDataFileName(loadModel.getRestructureNo()));
            CarbonFile[] loads =
                    getLoadFolderList(tableFile.getAbsolutePath(), loadModel.getValidSlices());

            if (null == loads) {
                continue;
            }
            /*if((RS_FOLDER_NAME+loadModel.getRestructureNo()).equals(rsFolder.getName()))
			{
				this.dimCardinality=DataStatsUtil.getCardinalityForLastLoadFolder(loads[loads.length-1], tableName);
				//this.dimCardinality=sliceMetaData.getActualDimLens();
			}*/

            String confloadSize =
                    CarbonProperties.getInstance().getProperty(Preference.AGG_LOAD_COUNT);
            int loadSize = loads.length;
            if (null != confloadSize && Integer.parseInt(confloadSize) < loadSize) {
                loadSize = Integer.parseInt(confloadSize);
            }
            int consideredLoadCounter = 0;
            for (CarbonFile load : loads) {
                LoadHandler loadHandler = new LoadHandler(sliceMetaData, metaCube, load);
                if (loadHandler.isDataAvailable(load, tableName)) {
                    loadHandlers.add(loadHandler);
                    updateDimensionCardinality(sliceMetaData, tableName);
                    consideredLoadCounter++;
                }
                if (consideredLoadCounter == loadSize) {
                    break;
                }
            }

        }

    }

    /**
     * Update dimension cardinality with its ordinal and cardinality value
     *
     * @param sliceMetaData
     * @param tableName
     */
    private void updateDimensionCardinality(SliceMetaData sliceMetaData, String tableName) {
        String[] sliceDimensions = sliceMetaData.getDimensions();
        int[] sliceCardinalities = sliceMetaData.getActualDimLens();
        for (Dimension dimension : visibleDimensions) {
            String dimName = dimension.getColName();
            Integer dimensionCardinality = 1;
            for (int i = 0; i < sliceDimensions.length; i++) {
                String sliceColName = sliceDimensions[i].substring(
                        sliceDimensions[i].indexOf(tableName + "_") + tableName.length() + 1);
                if (dimName.equals(sliceColName)) {
                    dimensionCardinality = sliceCardinalities[i];
                    break;
                }

            }
            dimCardinality.put(dimension.getOrdinal(), dimensionCardinality);

        }
    }

    /**
     * This will extract visible dimension from cube
     *
     * @param cube
     * @return
     */
    private List<Dimension> getVisibleDimensions(CarbonDef.Cube cube) {
        List<Dimension> visibleDimensions = new ArrayList<Dimension>();
        CarbonDef.CubeDimension[] cubeDimensions = cube.dimensions;
        for (CarbonDef.CubeDimension cubeDimension : cubeDimensions) {
            if (cubeDimension.visible) {
                Dimension dim = metaCube.getDimension(cubeDimension.name, getTableName());
                visibleDimensions.add(dim);
            }
        }
        return visibleDimensions;
    }

    private CarbonFile getTableFile(CarbonFile rsFolder, final String tableName) {
        CarbonFile[] tableFiles = rsFolder.listFiles(new CarbonFileFilter() {
            public boolean accept(CarbonFile pathname) {
                return (pathname.isDirectory()) && tableName.equals(pathname.getName());
            }
        });
        return tableFiles[0];
    }

    private CarbonFile[] getLoadFolderList(String path, final List<String> validLoads) {
        CarbonFile file = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
        CarbonFile[] files = null;
        if (file.isDirectory()) {
            files = file.listFiles(new CarbonFileFilter() {

                @Override
                public boolean accept(CarbonFile pathname) {
                    String name = pathname.getName();
                    return validLoads.contains(name);
                }
            });

        }
        return files;
    }

    /**
     * Get sample data
     *
     * @param dimension
     * @param cubeUniqueName
     * @return
     * @throws AggSuggestException
     */
    public List<String> getSampleData(Dimension dimension, String cubeUniqueName)
            throws AggSuggestException {

        // Sample data
        HashSet<Integer> surrogates = new HashSet<Integer>(100);
        for (LoadHandler loadHandler : loadHandlers) {

            try {
                //check if dimension exist in this load
                if (!loadHandler.isDimensionExist(dimension, tableName)) {
                    continue;
                }
                FactDataHandler factDataHandler = loadHandler.handleFactData(getTableName());
                if (null != factDataHandler) {
                    FactDataReader reader = factDataHandler.getFactDataReader();
                    surrogates.addAll(reader.getSampleFactData(dimension.getOrdinal(),
                            DataStatsUtil.getNumberOfRows(dimension)));
                }
            } catch (AggSuggestException e) {
                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
            }

        }
        if (surrogates.size() == 0) {
            // in case of cube alteration, its possible that dimension will not have value in load. Hence
            // in that case, pass "null" value.
            List<String> nullData = new ArrayList<String>(1);
            nullData.add("null");
            return nullData;
        }

        String levelName = dimension.getTableName() + '_' + dimension.getColName() + '_' + dimension
                .getDimName() + '_' + dimension.getHierName();
        List<String> realDatas = new ArrayList<String>(surrogates.size());
        for (int surrogate : surrogates) {

            String data = getDimensionValueFromSurrogate(cubeUniqueName, levelName, surrogate);
            if (null == data) {
                LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "Member value of dimension," + dimension.getName() + ",for surrogate,"
                                + surrogate + ",is not found in level file");
                continue;
            }

            realDatas.add(data);

        }
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                dimension.getName() + " Load size:" + loadHandlers.size() + ":Sample size:"
                        + realDatas.size());
        return realDatas;
    }

    /**
     * Get actual dimension value from member cache by passing surrogate key
     *
     * @param cubeName
     * @param levelName
     * @param surrogate
     * @return
     */
    public String getDimensionValueFromSurrogate(String cubeName, String levelName, int surrogate) {
        List<InMemoryTable> inMemoryTables =
                InMemoryTableStore.getInstance().getActiveSlices(cubeName);
        for (InMemoryTable inMemoryTable : inMemoryTables) {
            MemberStore memberStore = inMemoryTable.getMemberCache(levelName);
            Member member = memberStore.getMemberByID(surrogate);
            if (null != member) {
                return member.toString();
            }

        }
        return null;

    }

    public List<Dimension> getDimensions() {
        return this.visibleDimensions;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<Integer, Integer> getLastLoadCardinality() {
        return this.dimCardinality;

    }

    public Cube getMetaCube() {
        return metaCube;
    }

    public String getCubeUniqueName() {
        // TODO Auto-generated method stub
        return cubeUniqueName;
    }

    public List<LoadHandler> getLoadHandlers() {
        return this.loadHandlers;
    }

    public List<String> getAllLoads() {
        return this.allLoads;
    }

    /**
     * returns the instance of query scope object holding the segment cache and map of segment name as key and
     * modification time as value.
     */
    public QueryScopeObject getQueryScopeObject() {
        return queryScopeObject;
    }

}
