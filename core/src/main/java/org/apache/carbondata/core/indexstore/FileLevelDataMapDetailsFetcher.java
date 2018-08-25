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

package org.apache.carbondata.core.indexstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapUtil;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;

public class FileLevelDataMapDetailsFetcher
    implements BlockletDetailsFetcher, SegmentPropertiesFetcher {

  @Override
  public List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment)
      throws IOException {
    List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>(blocklets.size());
    for (Blocklet blocklet : blocklets) {
      String composedShardPath = blocklet.getFilePath();
      String[] segment2FactFile =
          DataMapUtil.decomposeShardName4FileLevelDataMap(composedShardPath);
      assert segment.getSegmentNo().equals(segment2FactFile[0]);
      ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet(segment2FactFile[1], "0");
      extendedBlocklet.setSegmentId(segment.getSegmentNo());
      extendedBlocklets.add(extendedBlocklet);
    }
    return extendedBlocklets;
  }

  @Override
  public ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, Segment segment)
      throws IOException {
    String composedShardPath = blocklet.getFilePath();
    String[] segment2FactFile =
        DataMapUtil.decomposeShardName4FileLevelDataMap(composedShardPath);
    assert segment.getSegmentNo().equals(segment2FactFile[0]);
    ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet(segment2FactFile[1], "0");
    extendedBlocklet.setSegmentId(segment.getSegmentNo());
    return extendedBlocklet;
  }

  @Override
  public List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<Blocklet> blocklets = new ArrayList<>();
    for (String file : segment.getLoadMetadataDetails().getFactFilePath().split(",")) {
      ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet(file, "0");
      extendedBlocklet.setSegmentId(segment.getSegmentNo());
      blocklets.add(extendedBlocklet);
    }
    return blocklets;
  }

  @Override
  public void clear() {

  }

  @Override
  public SegmentProperties getSegmentProperties(Segment segment) throws IOException {
    return null;
  }
}
