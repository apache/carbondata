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

package org.carbondata.processing.suggest.datastats.load;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.datastats.util.DataStatsUtil;
import org.carbondata.query.datastorage.streams.DataInputStream;
import org.carbondata.query.util.CarbonDataInputStreamFactory;

/**
 * Each load handler
 *
 * @author A00902717
 */
public class LoadHandler {

  private Cube metaCube;

  private CarbonFile loadFolder;

  private SliceMetaData sliceMetaData;

  private int msrCount;

  public LoadHandler(SliceMetaData sliceMetaData, Cube metaCube, CarbonFile loadFolder) {
    this.metaCube = metaCube;
    this.loadFolder = loadFolder;
    this.sliceMetaData = sliceMetaData;
    if (null != sliceMetaData.getMeasures()) {
      msrCount = sliceMetaData.getMeasures().length;
    }
  }

  public FactDataHandler handleFactData(String tableName) throws AggSuggestException {
    FactDataHandler factDataHandler = null;
    // Process fact and aggregate data cache
    for (String table : metaCube.getTablesList()) {
      if (!table.equals(tableName)) {
        continue;
      }
      factDataHandler = handleTableData(loadFolder, table, msrCount);

    }
    return factDataHandler;
  }

  /**
   * This checks if fact file present. In case of restructure, it will empty load
   *
   * @param loadFolder
   * @param table
   * @return
   */
  public boolean isDataAvailable(CarbonFile loadFolder, String table) {
    CarbonFile[] files = DataStatsUtil.getCarbonFactFile(loadFolder, table);
    if (null == files || files.length == 0) {
      return false;
    }
    return true;
  }

  private FactDataHandler handleTableData(CarbonFile loadFolder, String table, int msrCount)
      throws AggSuggestException {
    // get list of fact file
    CarbonFile[] files = DataStatsUtil.getCarbonFactFile(loadFolder, table);

    // Create LevelMetaInfo to get each dimension cardinality
    LevelMetaInfo levelMetaInfo = new LevelMetaInfo(loadFolder, table);
    int[] dimensionCardinality = levelMetaInfo.getDimCardinality();
    KeyGenerator keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimensionCardinality);
    int keySize = keyGenerator.getKeySizeInBytes();
    String factTableColumn = metaCube.getFactCountColMapping(table);
    boolean hasFactCount = factTableColumn != null && factTableColumn.length() > 0;
    // Create dataInputStream for each fact file
    List<DataInputStream> streams = new ArrayList<DataInputStream>(files.length);
    for (CarbonFile aFile : files) {

      streams.add(CarbonDataInputStreamFactory
          .getDataInputStream(aFile.getAbsolutePath(), keySize, msrCount, hasFactCount,
              loadFolder.getAbsolutePath(), table, FileFactory.getFileType()));
    }
    for (DataInputStream stream : streams) {
      // Initialize offset value and other detail for each stream
      stream.initInput();
    }

    return new FactDataHandler(metaCube, levelMetaInfo, table, keySize, streams);

  }

  public CarbonFile getLoadFolder() {
    return loadFolder;
  }

  /**
   * check if given dimension exist in current load in case of restructure.
   *
   * @param dimension
   * @param tableName
   * @return
   */
  public boolean isDimensionExist(Dimension dimension, String tableName) {
    String[] sliceDimensions = sliceMetaData.getDimensions();
    for (int i = 0; i < sliceDimensions.length; i++) {
      String colName = sliceDimensions[i]
          .substring(sliceDimensions[i].indexOf(tableName + "_") + tableName.length() + 1);
      if (dimension.getColName().equals(colName)) {
        return true;
      }
    }
    return false;
  }

}
