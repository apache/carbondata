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

package org.apache.carbondata.core.index.dev.cgindex;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 *  Factory for {@link CoarseGrainIndex}
 *  1. Any filter query which hits the table with index will call prune method of CGindex.
 *  2. The prune method of CGindex return list Blocklet , these blocklets contain the
 *     information of block and blocklet.
 *  3. Based on the splits scanrdd schedule the tasks.
 */
@InterfaceAudience.Developer("Index")
@InterfaceStability.Evolving
public abstract class CoarseGrainIndexFactory extends IndexFactory<CoarseGrainIndex> {

  public CoarseGrainIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema) {
    super(carbonTable, indexSchema);
  }

  @Override
  public IndexLevel getIndexLevel() {
    return IndexLevel.CG;
  }
}
