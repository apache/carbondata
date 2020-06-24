/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.indexstore.blockletindex;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import org.apache.hadoop.conf.Configuration;

/**
 * It is the model object to keep the information to build or initialize BlockletIndex.
 */
public class BlockletIndexModel extends IndexModel {

  private byte[] fileData;

  private Map<String, BlockMetaInfo> blockMetaInfoMap;

  private CarbonTable carbonTable;

  private String segmentId;

  private boolean addToUnsafe = true;
  /**
   * list of index thrift object present in index file
   */
  private List<DataFileFooter> indexInfos;

  private boolean serializeDmStore = false;

  public BlockletIndexModel(CarbonTable carbonTable, String filePath, byte[] fileData,
      Map<String, BlockMetaInfo> blockMetaInfoMap, String segmentId, Configuration configuration) {
    super(filePath, configuration);
    this.fileData = fileData;
    this.blockMetaInfoMap = blockMetaInfoMap;
    this.segmentId = segmentId;
    this.carbonTable = carbonTable;
  }

  public BlockletIndexModel(CarbonTable carbonTable, String filePath,
      byte[] fileData, Map<String, BlockMetaInfo> blockMetaInfoMap, String segmentId,
      boolean addToUnsafe, Configuration configuration, boolean serializeDmStore) {
    this(carbonTable, filePath, fileData, blockMetaInfoMap, segmentId, configuration);
    this.addToUnsafe = addToUnsafe;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    this.serializeDmStore = serializeDmStore;
  }

  public byte[] getFileData() {
    return fileData;
  }

  public Map<String, BlockMetaInfo> getBlockMetaInfoMap() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2071
    return blockMetaInfoMap;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public boolean isAddToUnsafe() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
    return addToUnsafe;
  }

  public CarbonTable getCarbonTable() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2701
    return carbonTable;
  }

  public void setIndexInfos(List<DataFileFooter> indexInfos) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
    this.indexInfos = indexInfos;
  }

  public List<DataFileFooter> getIndexInfos() {
    return indexInfos;
  }

  public boolean isSerializeDmStore() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
    return serializeDmStore;
  }
}
