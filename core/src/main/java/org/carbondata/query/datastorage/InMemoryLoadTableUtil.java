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
package org.carbondata.query.datastorage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;

/**
 * The class contains common methods related to the Segment loading
 */
public final class InMemoryLoadTableUtil {

    private InMemoryLoadTableUtil() {

    }

    /**
     * The method return the map of segment name and its load status as key value pair.
     */
    public static Map<String, String> createLoadNameAndStatusMapping(
            LoadMetadataDetails[] loadMetadataDetailses) {
        Map<String, String> loadNameAndStatusMapping =
                new HashMap<String, String>(loadMetadataDetailses.length);
        for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetailses) {
            loadNameAndStatusMapping
                    .put(CarbonCommonConstants.LOAD_FOLDER + loadMetadataDetail.getLoadName(),
                            loadMetadataDetail.getLoadStatus());
        }
        return loadNameAndStatusMapping;
    }

    /**
     * The Method returns the map of segment Name and its modification Time as key value pair
     */
    public static Map<String, Long> createLoadAndModificationTimeMappping(
            LoadMetadataDetails[] loadMetadataDetails) {
        Map<String, Long> mapOfLoadNameAndModificationTime =
                new HashMap<String, Long>(loadMetadataDetails.length);
        for (LoadMetadataDetails loadDetails : loadMetadataDetails) {
            String deletionTimestamp = loadDetails.getModificationOrdeletionTimesStamp();
            if (null == deletionTimestamp) {
                mapOfLoadNameAndModificationTime
                        .put(CarbonCommonConstants.LOAD_FOLDER + loadDetails.getLoadName(), 0L);
            } else {
                mapOfLoadNameAndModificationTime
                        .put(CarbonCommonConstants.LOAD_FOLDER + loadDetails.getLoadName(),
                                parseDeletionTime(deletionTimestamp));
            }
        }
        return mapOfLoadNameAndModificationTime;
    }

    /**
     * The method parses the date String to Date and returns the date in milliseconds.
     */
    public static Long parseDeletionTime(String deletionTimestamp) {
        SimpleDateFormat format =
                new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        if (null != deletionTimestamp) {
            try {
                Date deletionDate = format.parse(deletionTimestamp);
                return deletionDate.getTime();
            } catch (ParseException e) {
                return 0L;
            }

        }
        return 0L;
    }

    /**
     * The method return's the list of Segments to be loaded
     */
    public static List<String> getListOfFoldersToBeLoaded(List<String> listLoadFolders,
            Map<String, String> loadNameAndStatusMapping, List<RestructureStore> rsStoreList,
            String factTableName, LoadMetadataDetails[] loadMetadataDetails) {
        if (null == rsStoreList) {
            return listLoadFolders;
        }
        List<String> listOfLoadFolderToBeLoaded = new ArrayList<String>(loadMetadataDetails.length);
        listOfLoadFolderToBeLoaded.addAll(listLoadFolders);
        List<InMemoryTable> activeSlices = getActiveSlices(rsStoreList, factTableName);
        for (InMemoryTable memoryTable : activeSlices) {
            if (listLoadFolders.contains(memoryTable.getLoadName())) {
                //if the Segment folder is already loaded and not modified then remove from the
                //listOfFolderToBeUpdated
                String status = loadNameAndStatusMapping.get(memoryTable.getLoadName());
                if (CarbonCommonConstants.MARKED_FOR_UPDATE.equals(status)) {
                    if (!isSliceRequiredToBeLoaded(loadMetadataDetails, memoryTable)) {
                        listOfLoadFolderToBeLoaded.remove(memoryTable.getLoadName());
                    }
                } else {
                    listOfLoadFolderToBeLoaded.remove(memoryTable.getLoadName());
                }

            }
        }
        return listOfLoadFolderToBeLoaded;
    }

    /**
     * return true if the segment is being modified.
     */
    private static boolean isSliceRequiredToBeLoaded(LoadMetadataDetails[] loadMetadataDetails,
            InMemoryTable memoryTable) {

        for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
            String loadName = CarbonCommonConstants.LOAD_FOLDER + loadMetadataDetail.getLoadName();
            if (loadName.equals(memoryTable.getLoadName())) {
                long deletionTime =
                        parseDeletionTime(loadMetadataDetail.getModificationOrdeletionTimesStamp());
                if (deletionTime == memoryTable.getModificationTime()) {
                    return false;
                }
            }
        }
        return true;

    }

    /**
     * The method returns the List of segments identified by the tableName
     */
    private static List<InMemoryTable> getActiveSlices(List<RestructureStore> rsStoreList,
            String factTableName) {
        List<InMemoryTable> activeInMemTableList =
                new ArrayList<InMemoryTable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (RestructureStore rsStore : rsStoreList) {
            rsStore.getActiveSlicesByTableName(activeInMemTableList, factTableName);
        }
        return activeInMemTableList;
    }

    /**
     * This method will delete the segments if the segments modification time exceeds
     * the query time out
     */
    public static void checkAndDeleteStaleSlices(List<RestructureStore> slices, String tableName) {

        if (null != slices) {
            long currentTimeMillis = System.currentTimeMillis();
            long queryMaxTimeOut = getQueryMaxTimeOut();
            List<InMemoryTable> activeSlices = getActiveSlices(slices, tableName);
            List<InMemoryTable> listOfSliceToBeRemoved =
                    new ArrayList<InMemoryTable>(activeSlices.size());
            for (InMemoryTable inMemTable : activeSlices) {
                long modificationTime = inMemTable.getModificationTime();
                if (modificationTime > 0L) {
                    long diff = currentTimeMillis - modificationTime;
                    int minElapsed = (int) (diff / 1000 * 60);
                    if (minElapsed > queryMaxTimeOut) {
                        listOfSliceToBeRemoved.add(inMemTable);
                    }
                }
            }
            if (!listOfSliceToBeRemoved.isEmpty()) {
                for (RestructureStore rsStore : slices) {
                    rsStore.removeSlice(listOfSliceToBeRemoved, tableName);
                }
            }
        }
    }

    /**
     * returns max Query execution time
     */
    public static int getQueryMaxTimeOut() {
        int queryTimeOut = 0;
        String timeOutValue = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME);
        try {
            queryTimeOut = Integer.parseInt(timeOutValue);
        } catch (NumberFormatException ne) {
            queryTimeOut = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
        }

        return queryTimeOut;
    }

    /**
     * The method return list of valid segments.
     */
    public static List<InMemoryTable> getQuerySlices(List<InMemoryTable> activeSlices,
            Map<String, Long> loadNameAndModicationTimeMap) {
        List<InMemoryTable> validSliceList =
                new ArrayList<InMemoryTable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (InMemoryTable inMemoryTable : activeSlices) {
            Long modificationTimeStamp =
                    loadNameAndModicationTimeMap.get(inMemoryTable.getLoadName());
            if (inMemoryTable.getModificationTime() == modificationTimeStamp) {
                validSliceList.add(inMemoryTable);
            }
        }
        return validSliceList;
    }
}
