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
package org.apache.carbondata.core.datamap.dev.fgdatamap;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;

/**
 * DataMap for Fine Grain level, see {@link org.apache.carbondata.core.datamap.DataMapLevel#FG}
 */
@InterfaceAudience.Developer("DataMap")
@InterfaceStability.Evolving
public abstract class FineGrainDataMap implements DataMap<FineGrainBlocklet> {

  @Override
  public List<FineGrainBlocklet> prune(Expression filter, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions, AbsoluteTableIdentifier identifier) throws IOException {
    throw new UnsupportedOperationException("Filter expression not supported");
  }

  @Override public int getNumberOfEntries() {
    // keep default, one record in one datamap
    return 1;
  }
}
